/*
 * changeSet.cpp
 *
 *  Created on: 11. 3. 2016
 *      Author: ondra
 */

#include <ctime>
#include "changeset.h"

#include "document.h"



#include "localView.h"
#include "validator.h"
//#include "validator.h"
namespace couchit {

Changeset::Changeset(CouchDB &db):db(&db) {
}

Changeset::Changeset():db(nullptr) {
}

Changeset& Changeset::update(const Document &document) {
	Value doc = document;
	Value id = doc["_id"];
	if (!id.defined())
		throw DocumentHasNoID( document);

	scheduledDocs.push_back(document);

	return *this;
}



Changeset& Changeset::commit(CouchDB& db) {
	if (scheduledDocs.empty()) return *this;

	Value now = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());

	Array docsToCommit;

	const Validator *v  = db.getValidator();
	for(auto &&item: scheduledDocs) {
		Value doc = item;

		if (v) {
			Validator::Result r = v->validateDoc(doc);;
			if (!r) throw ValidationFailedException(r);
		}

		Object adj(doc);

		bool hasTm = doc[CouchDB::fldTimestamp].defined();
		if (hasTm) {
			adj.set(CouchDB::fldTimestamp,now);
		}

		docsToCommit.add(adj);
		Value conflicts = adj["_conflicts"];
		if (conflicts.defined()) {
			for (Value s: conflicts) {
				docsToCommit.push_back(Object("_id",doc["_id"])
									  ("_rev",doc["_rev"])
									  ("_deleted",true));

			}
		}

	}

	commitedDocs.clear();
	scheduledDocs.clear();


	Value out = db.bulkUpload(docsToCommit);

	auto siter = docsToCommit.begin();

	std::vector<UpdateException::ErrorItem> errors;

	for (Value item: out) {
		if (siter == docsToCommit.end()) break;

		Value rev (item["rev"]);
		Value err (item["error"]);
		String id (item["id"]);
		Value orgitem = *siter;


		if (id != orgitem["_id"].toString()) {
			continue;
		} else if (err.defined()) {
				UpdateException::ErrorItem e;

				e.errorDetails = item;
				e.document = *siter;
				e.errorType = String(err);
				e.reason = String(item["reason"]);
				errors.push_back(e);
		} else {
			commitedDocs.push_back(CommitedDoc(orgitem["_id"].getString(), String(rev), orgitem));
		}
		++siter;
	}


	if (!errors.empty()) throw UpdateException(errors);

	return *this;


}


void Changeset::revert(const StrViewA &docId) {
	auto iter = scheduledDocs.rbegin();
	while (iter != scheduledDocs.rend()) {
		if ((*iter)["_id"].getString() == docId) {
			scheduledDocs.erase(iter.base());
			break;
		} else {
			++iter;
		}
	}
}

void Changeset::checkDB() const  {
	if (db == nullptr) throw std::runtime_error("Operation with Changeset called without active database");
}
Changeset& Changeset::commit() {
	checkDB();
	return commit(*db);
}

Changeset& Changeset::erase(const String &docId, const String &revId) {


	Document doc;
	doc.setID(docId);
	doc.setRev(revId);
	doc.setDeleted();

	update(doc);

	return *this;
}

String Changeset::getCommitRev(const StrViewA& docId) const {

	for (auto &&x: commitedDocs) {
		if (x.id == docId) return x.newRev;
	}
	return String();
}

Changeset& Changeset::preview(LocalView& view) {
	for (auto &&item:scheduledDocs) {
		view.updateDoc(item);
	}
	return *this;


}

String Changeset::getCommitRev(const Document& doc) const {
	return getCommitRev(doc.getID());
}

std::size_t Changeset::mark() const {
	return scheduledDocs.size();
}

void Changeset::revertToMark(std::size_t mark) {
	if (mark < scheduledDocs.size()) {
		scheduledDocs.resize(mark);
	}
}

Changeset::CommitedDoc::operator Document() const {
	Document d (doc);
	d.setRev(newRev);

	return d;
}

Value Changeset::getUpdatedDoc(const StrViewA& docId) const {
	for (auto &&x: commitedDocs) {
		if (x.id == docId)
			return x.doc.replace("_rev", x.newRev);
	}
	return Value();
}

Value Changeset::getCommited() const {
	Object obj;
	for (auto &&x: commitedDocs) {
		obj.set(x.id, x.doc.replace("_rev",x.newRev));
	}
	return obj;
}

couchit::BatchWrite::BatchWrite(CouchDB& db):db(db),thr([this]{worker();}) {}

BatchWrite::~BatchWrite() {
	queue.push(Msg(nullptr, nullptr, false));
	thr.join();
}

void couchit::BatchWrite::put(const Value& doc) {
	put(doc, nullptr);
}

void couchit::BatchWrite::put(const Value& doc, Callback&& cb) {
	if (doc["_id"].hasValue()) {
		queue.push(Msg(doc, std::move(cb), false));
	} else {
		throw std::runtime_error("BatchWrite::put - document must have _id");
	}
}

couchit::BatchWrite::Msg::Msg(const Value& doc, Callback&& cb, bool replication):doc(doc),cb(std::move(cb)),replication(replication) {

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

void couchit::BatchWrite::worker() {
	std::vector<Callback> normal_cbs;
	Array normal_docs, replication_docs;
	for(;;) {
		Msg msg = queue.pop();
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
			try {
				db.bulkUpload(replication_docs, true);
			} catch (...) {
				onException();
				for (Value v: replication_docs) {
					replicate(v);
				}
			}
		}
		if (!msg.doc.hasValue()) break;
	}
}

} /* namespace assetex */

