/*
 * changeSet.cpp
 *
 *  Created on: 11. 3. 2016
 *      Author: ondra
 */

#include "changeset.h"

#include "document.h"

#include "lightspeed/base/containers/autoArray.tcc"

#include "localView.h"
#include "validator.h"
//#include "validator.h"
namespace LightCouch {

Changeset::Changeset(CouchDB &db):db(db) {


}

Changeset& Changeset::update(const Document &document) {
	if (!document.dirty()) return *this;
	Value doc = document;
	Value id = doc["_id"];
	if (!id.defined())
		throw DocumentHasNoID(THISLOCATION, document);

	Value ctodel = document.getConflictsToDelete();
	if (ctodel.defined()) {
		Array ccdocs;
		for (auto &&item: ctodel) {
			ccdocs.add(Object("_id",id)
					("_rev",item)
					("_deleted",true));
		}
		ctodel = ccdocs;
	}

	scheduledDocs[id.getString()] = std::make_pair(doc, ctodel);

	return *this;
}

Changeset& Changeset::update(const Value &document) {
	Value doc = document;
	Value id = doc["_id"];
	if (!id.defined())
		throw DocumentHasNoID(THISLOCATION, document);

	scheduledDocs[id.getString()] = std::make_pair(doc, Value());

	return *this;
}



Changeset& Changeset::commit(CouchDB& db,bool all_or_nothing) {
	if (scheduledDocs.empty()) return *this;

	///if validator is not present, do not run empty cycle.

	Value now;

	Array docsToCommit;

	const Validator *v  = db.getValidator();
	for(auto &&item: scheduledDocs) {
		Value doc = item.second.first;
		Value conflicts = item.second.second;

		if (v) {
			Validator::Result r = v->validateDoc(doc);;
			if (!r) throw ValidationFailedException(THISLOCATION,r);
		}

		bool hasTm = doc[CouchDB::fldTimestamp].defined();
		bool hasPrevRev =  doc[CouchDB::fldPrevRevision].defined();
		if (hasTm && !now.defined()) {
			now = Value(TimeStamp::now().asUnix());
		}
		if (hasTm || hasPrevRev) {
			Object adj(doc);
			if (hasTm) adj.set(CouchDB::fldTimestamp, now);
			if (hasPrevRev) adj.set(CouchDB::fldPrevRevision, doc["_rev"]);
			doc = adj;
		}

		docsToCommit.add(doc);

		for (auto &&c: item.second.second) {
			docsToCommit.add(c);
		}
	}


	commitedDocs.clear();
	scheduledDocs.clear();
	Value out = db.bulkUpload(docsToCommit, all_or_nothing);

	AutoArray<UpdateException::ErrorItem> errors;

	for (auto &&item : out) {
		Value rev = item["rev"];
		Value err = item["error"];
		String id = item["id"];
		if (err.defined()) {
			UpdateException::ErrorItem e;

			e.errorDetails = item;
			e.document = id;
			e.errorType = err;
			e.reason = item["reason"];
			errors.add(e);
		}
		if (rev.defined()) {
			commitedDocs.insert(std::make_pair(StrView(id),item));
		}
	}
	if (errors.length()) throw UpdateException(THISLOCATION,errors);

	return *this;


}

Changeset::Changeset(const Changeset& other):db(other.db) {
}


void Changeset::revert(const StrView &docId) {
	scheduledDocs.erase(docId);
}


Changeset& Changeset::commit(bool all_or_nothing) {
	return commit(db,all_or_nothing);
}

Changeset& Changeset::erase(const String &docId, const String &revId) {


	Document doc;
	doc.setID(docId);
	doc.setRev(revId);
	doc.setDeleted();

	update(doc);

	return *this;
}

String Changeset::getCommitRev(const StrView& docId) const {

	auto f = commitedDocs.find(docId);
	if (f == commitedDocs.end()) return String();
	else {
		return f->second["_rev"];
	}
}

Changeset& Changeset::preview(LocalView& view) {
	for (auto &&item:scheduledDocs) {
		view.updateDoc(item.second.first);
	}
	return *this;


}

String Changeset::getCommitRev(const Document& doc) const {
	return getCommitRev(doc.getID());
}

} /* namespace assetex */

