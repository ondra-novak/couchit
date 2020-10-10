/*
 * batch.cpp
 *
 *  Created on: 10. 10. 2020
 *      Author: ondra
 */



#include "batch.h"

#include "shared/logOutput.h"
#include "couchDB.h"

using ondra_shared::logDebug;

namespace couchit {

BatchWrite::BatchWrite(CouchDB& db):db(db),run_state(RunState::not_started) {}

BatchWrite::~BatchWrite() {
	switch (run_state) {
	case RunState::running:
		queue.push(Msg(nullptr, nullptr, false));
		[[fallthrough]];
	case RunState::exited:
		thr.join();
		[[fallthrough]];
	case RunState::not_started:
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
	} else {
		throw std::runtime_error("BatchWrite::put - document must have _id");
	}
}

BatchWrite::Msg::Msg(const Value& doc, Callback&& cb, bool replication):doc(doc),cb(std::move(cb)),replication(replication) {

}


void BatchWrite::replicate(const Value& doc) {
	if (doc["_id"].hasValue()) {
		if (doc["_revisions"].type() == json::object) {
			queue.push(Msg(doc, nullptr, true));
		} else {
			throw std::runtime_error("BatchWrite::replicate - document must have _revisions");
		}
	} else {
		throw std::runtime_error("BatchWrite::replicate - document must have _id");
	}
}

void BatchWrite::onException() noexcept {
	std::this_thread::sleep_for(std::chrono::seconds(2));
}

void BatchWrite::worker() {
	std::vector<Callback> normal_cbs;
	Array normal_docs, replication_docs;
	bool processed = false;
	bool exit = false;
	do {
		processed = queue.pump_for(std::chrono::seconds(5),[&](Msg &&msg){

			normal_docs.clear();
			replication_docs.clear();

			auto max_batch_size = db.getConfig().maxBulkSizeDocs;
			while (msg.doc.hasValue()) {
				if (msg.replication) {
					replication_docs.push_back(msg.doc);
				} else {
					normal_cbs.push_back(std::move(msg.cb));
					normal_docs.push_back(msg.doc);
				}
				--max_batch_size;
				if (max_batch_size == 0 || queue.empty()) break;
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
			if (!msg.doc.hasValue()) {
				exit = true;
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
