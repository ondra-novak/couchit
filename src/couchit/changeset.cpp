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
			commitedDocs.push_back(CommitedDoc(id, String(rev), *siter));
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
} /* namespace assetex */
