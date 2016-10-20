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
	Value id = document.getIDValue();
	if (!id.defined())
		throw DocumentHasNoID(THISLOCATION, document);
	docMap[id] = DocInfo(document);

/*
	DocInfo editedDoc(document);
	Value doc = document;
	if (!doc["_id"].defined())
	DocInfo nfo;
	nfo.doc = doc;
	nf
	docMap.insert(doc["_id"],)
	if (document["_id"] == null) {
		document.set("_id",db.genUIDValue());
	}
	docs.add(document);
	eraseConflicts(document["_id"], document.getConflictsToDelete());*/
	return *this;
}



Changeset& Changeset::commit(CouchDB& db,bool all_or_nothing) {
	if (docMap.empty()) return *this;

	///if validator is not present, do not run empty cycle.

	Value now;

	Array docsToCommit;

	const Validator *v;
	if ((v = db.getValidator())!=0) {
		for(auto &&item: docMap) {
			Validator::Result r = v->validateDoc(item.second.doc);;
			if (!r) throw ValidationFailedException(THISLOCATION,r);

			bool hasTm = item.second.doc[CouchDB::fldTimestamp].defined();
			bool hasPrevRev =  item.second.doc[CouchDB::fldPrevRevision].defined();
			if (hasTm && !now.defined()) {
				now = Value(TimeStamp::now().asUnix());
			}
			if (hasTm || hasPrevRev) {
				Object adj(item.second.doc);
				if (hasTm) adj.set(CouchDB::fldTimestamp, now);
				if (hasPrevRev) adj.set(CouchDB::fldPrevRevision, item.second.doc["_rev"]);
				item.second.doc = adj;
			}

			docsToCommit.add(item.second.doc);
			for (auto &&c: item.second.conflicts) {
				docsToCommit.add(c);
			}
		}
	}

	Object wholeRequest;
	wholeRequest.set("docs", docsToCommit);
	if (all_or_nothing)
		wholeRequest.set("all_or_nothing",true);

	Value out = db.requestPOST("_bulk_docs", wholeRequest);


	AutoArray<UpdateException::ErrorItem> errors;

	for (auto &&item : out) {
		Value rev = item["rev"];
		Value err = item["error"];
		String id = item["id"];
		DocMap::iterator itr = docMap.find(id);
		if (itr == docMap.end()) {
			UpdateException::ErrorItem e;
			e.errorDetails = item;
			e.errorType = "internal_error";
			e.reason = "Couchdb returned an unknown document";
			errors.add(e);
		} else {
			if (err.defined()) {
				UpdateException::ErrorItem e;

				e.errorDetails = item;
				e.document = itr->second.doc;
				e.errorType = err;
				e.reason = item["reason"];
				errors.add(e);
			}
			if (rev.defined()) {
				itr->second.newRevId = rev;
			}
		}
	}
	if (errors.length()) throw UpdateException(THISLOCATION,errors);

	return *this;


}

Changeset::Changeset(const Changeset& other):db(other.db) {
}


void Changeset::revert(const String &docId) {
	docMap.erase(docId);
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

Value Changeset::getDoc(const String& docId) const {

	auto iter = docMap.find(docId);
	if (iter == docMap.end()) return Value();
	Object o(iter->second.doc);
	o.set("_rev",iter->second.newRevId);
	return o;

}

String Changeset::getRev(const String& docId) const {
	auto iter = docMap.find(docId);
	if (iter == docMap.end()) return String();
	return iter->second.newRevId;

}

Changeset& Changeset::preview(LocalView& view) {
	for (auto &&item:docMap) {
		view.updateDoc(item.second.doc);
	}
	return *this;


}

Changeset::DocInfo::DocInfo(const Document& editedDoc) {

	this->doc = editedDoc;
	Array conflicts;
	for (auto &&item: editedDoc.getConflictsToDelete()) {
		Document doc;
		doc.setID(editedDoc.getIDValue());
		doc.setRev(item);
		doc.setDeleted();
		conflicts.add(doc);
	}
	this->conflicts = conflicts;

}

/*
void Changeset::eraseConflicts(ConstValue docId, ConstValue conflictList) {
	if (conflictList != null)
		conflictList->enumEntries(JSON::IEntryEnum::lambda([&](const ConstValue &v, ConstStrA, natural ){
		erase(docId, v);
		return false;
	}));
}*/


} /* namespace assetex */

