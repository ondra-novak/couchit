/*
 * batch.cpp
 *
 *  Created on: 10. 10. 2020
 *      Author: ondra
 */



#include "batch.h"

#include "shared/logOutput.h"
#include "couchDB.h"
#include "query.h"
#include "document.h"

using ondra_shared::logDebug;
using ondra_shared::logError;

namespace couchit {

BatchWrite::BatchWrite(CouchDB& db):db(db),run_state(RunState::not_started) {}

BatchWrite::~BatchWrite() {
	switch (run_state) {
	case RunState::running:
		queue.push(Msg());
		[[fallthrough]];
	case RunState::exited:
		thr.join();
		[[fallthrough]];
	case RunState::not_started:
		break;
	}
}

void BatchWrite::init_worker() {
	switch (run_state) {
		case RunState::exited:
			thr.join();
			[[fallthrough]];
		case RunState::not_started:
			thr=std::thread([&]{this->worker();});
			run_state = RunState::running;
			[[fallthrough]];
		case RunState::running:
			break;
	}
}

void BatchWrite::put(const Value& doc) {
	put(doc, nullptr);
}

void BatchWrite::put(const Value& doc, Callback&& cb) {
	if (doc["_id"].hasValue()) {
		std::lock_guard _(queue.getLock());
		queue.push(Msg(doc, std::move(cb), false));
		init_worker();
	} else {
		throw std::runtime_error("BatchWrite::put - document must have _id");
	}
}

BatchWrite::Msg::Msg(const Value& doc, Callback&& cb, bool replication)
	:doc(doc),cb(std::move(cb)),mode(replication?batch_replicate:batch_put) {

}
BatchWrite::Msg::Msg(const String& docId, ReadCallback&& cb)
	:doc(docId),cb(std::move(cb)),mode(batch_get) {

}


void BatchWrite::replicate(const Value& doc) {
	if (doc["_id"].hasValue()) {
		if (doc["_revisions"].type() == json::object) {
			std::lock_guard _(queue.getLock());
			queue.push(Msg(doc, nullptr, true));
			init_worker();
		} else {
			throw std::runtime_error("BatchWrite::replicate - document must have _revisions");
		}
	} else {
		throw std::runtime_error("BatchWrite::replicate - document must have _id");
	}
}

void BatchWrite::get(const json::String docId, ReadCallback &&cb) {
	std::lock_guard _(queue.getLock());
	queue.push(Msg(docId, std::move(cb)));
	init_worker();
}

void BatchWrite::onException() noexcept {
	try {
		throw;
	} catch (std::exception &e) {
		logError("Batch Write experienced an exception: $1", e.what());
	} catch (...) {
		logError("Batch Write experienced an undetermined exception");
	}
	std::this_thread::sleep_for(std::chrono::seconds(2));
}

void BatchWrite::worker() {
	std::vector<Callback> normal_cbs;
	std::vector<ReadCallback> read_cbs;
	Array normal_docs, replication_docs, gets;
	bool processed = false;
	bool exit = false;
	do {
		auto max_batch_size = db.getConfig().maxBulkSizeDocs;
		if (max_batch_size == 0) max_batch_size = 256;

		processed = queue.pump_for(std::chrono::seconds(5),[&](Msg &&msg){

			normal_docs.clear();
			replication_docs.clear();
			gets.clear();
			normal_cbs.clear();
			read_cbs.clear();
			bool exitLoop = false;

			while (msg.doc.hasValue()) {
				switch (msg.mode) {
					case batch_replicate:
						replication_docs.push_back(msg.doc);
						exitLoop = replication_docs.size() >= max_batch_size;
						break;
					case batch_put:
						normal_cbs.push_back(std::get<Callback>(std::move(msg.cb)));
						normal_docs.push_back(msg.doc);
						exitLoop = normal_docs.size() >= max_batch_size;
						break;
					case batch_get:
						read_cbs.push_back(std::get<ReadCallback>(std::move(msg.cb)));
						gets.push_back(msg.doc);
						exitLoop = gets.size() >= max_batch_size;
						break;
					case thread_exit:
						exit = true;
						exitLoop = true;
						break;
				}

				if (queue.empty() || exitLoop) break;
				msg = queue.pop();
			}
			if (!normal_docs.empty()) {
				try {
					logDebug("couchit: Batch upload $1 documents", normal_docs.size());
					Value resp = db.bulkUpload(normal_docs);
					auto iter1 = normal_docs.begin();
					auto iter2 = resp.begin();
					auto iter3 = normal_cbs.begin();
					while (iter1 != normal_docs.end() && iter2 != resp.end()) {
						Callback &cb = *iter3;
						if (cb) try {
							Value item = *iter2;
							Value rev (item["rev"]);
							Value err (item["error"]);
							String id (item["id"]);
							if (err.defined()) {
								cb(false,err);
							} else {
								cb(true, rev);
							}
						} catch (...) {

						}
						++iter1;
						++iter2;
						++iter3;
					}
				} catch (...) {
					onException();
					auto iter1 = normal_docs.begin();
					auto iter3 = normal_cbs.begin();
					while (iter1 != normal_docs.end()) {
						put(*iter1, std::move(*iter3));
						++iter1;++iter3;
					}
				}
			}
			if (!replication_docs.empty()) {
				logDebug("couchit: Batch replication $1 documents", replication_docs.size());
				try {
					db.bulkUpload(replication_docs, true);
				} catch (...) {
					onException();
					for (Value v: replication_docs) {
						replicate(v);
					}
				}
			}
			if (!gets.empty()) {
				logDebug("couchit: Batch get $1 documents", gets.size());
				try {
					Result res = db.createQuery(View::conflicts| View::includeDocs).keys(gets).exec();
					auto iter = read_cbs.begin();
					for (Row rw: res) {
						Document doc;
						if (rw.error.defined()) {
							doc.setID(rw.key);
							(*iter)(doc);
						} else {
							doc.setBaseObject(rw.doc);
							(*iter)(doc);
						}
						++iter;
					}
				} catch (...) {
					onException();
					auto iter = read_cbs.begin();
					for (Value v: gets) {
						get(v.toString(), std::move(*iter++));
					}
				}
			}
		});
		if (exit) {
			run_state = RunState::exited;
		}
		else if (!processed) {
			queue.modifyQueue([&](auto &q){
				if (q.empty()) {
					run_state = RunState::exited;
				}
			});
		}
	} while (run_state == RunState::running);
}


}
