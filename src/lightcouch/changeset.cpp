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

Changeset::Changeset(CouchDB &db):json(db.json), db(db) {

	init();

}


Changeset& Changeset::update(Document &document) {
	if (!document.dirty()) return *this;
	if (document["_id"] == null) {
		document.set("_id",db.genUIDValue());
	}
	docs.add(document.getEditing());
	eraseConflicts(document["_id"], document.getConflictsToDelete());
	return *this;
}



Changeset& Changeset::commit(CouchDB& db,bool all_or_nothing) {
	if (docs->empty()) return *this;

	Value now = json(TimeStamp::now().asUnix());
	for (JSON::Iterator iter = docs->getFwIter(); iter.hasItems();) {
		JSON::KeyValue doc = iter.getNext();
		if (doc[CouchDB::fldTimestamp] != null) {
			doc.set(CouchDB::fldTimestamp, now);
		}
		if (doc[CouchDB::fldPrevRevision] != null) {
			Value rev = doc["_rev"];
			if (rev != null) doc.set(CouchDB::fldPrevRevision,rev);
		}
	}

	///if validator is not present, do not run empty cycle.

	const Validator *v;
	if ((v = db.getValidator())!=0) {
		docs->enumEntries(JSON::IEntryEnum::lambda([v](const JSON::INode *nd, ConstStrA, natural){
			Validator::Result r = v->validateDoc(nd);
			if (!r) throw ValidationFailedException(THISLOCATION,r);
			return false;
		}));
	}
	if (all_or_nothing)
		json.object(wholeRequest)("all_or_nothing",true);
	JSON::ConstValue out = db.requestPOST("_bulk_docs", wholeRequest);


	AutoArray<UpdateException::ErrorItem> errors;

	natural index = 0;
	for (JSON::ConstIterator iter = out->getFwIter(); iter.hasItems();) {
		const JSON::ConstKeyValue &kv = iter.getNext();

		JSON::ConstValue rev = kv["rev"];
		if (rev != null) json.object(docs[index])("_rev",rev);

		JSON::ConstValue err = kv["error"];
		if (err != null) {
			UpdateException::ErrorItem e;
			e.errorDetails = kv;
			e.document = docs[index];
			e.errorType = err->getStringUtf8();
			e.reason = kv["reason"]->getStringUtf8();
			errors.add(e);
		}

		index++;
	}

	//prepare for next request
	init();

	if (errors.length()) throw UpdateException(THISLOCATION,errors);

	return *this;


}

Changeset::Changeset(const Changeset& other):json(other.json),db(other.db) {
}

natural Changeset::mark() const {
	return docs->length();
}

void Changeset::revert(natural mark) {
	if (mark == 0) {
		docs.clear();
	} else {
		while (docs->length() != mark) {
			docs.erase(docs->length()-1);
		}
	}
}

void Changeset::revert(Value doc) {
	for (natural i = 0, cnt = docs->length(); i < cnt; i++) {
		if (docs[i] == doc) {
			docs.erase(i);
			break;
		}
	}
}

void Changeset::init() {
	docs = json.array();
	wholeRequest = json("docs",docs);
}


Changeset& Changeset::commit(bool all_or_nothing) {
	return commit(db,all_or_nothing);
}

Changeset& Changeset::erase(ConstValue docId, ConstValue revId) {
	docs.add(json("_id",static_cast<const Value &>(docId))
			("_rev",static_cast<const Value &>(revId))
			("_deleted",true)
			("+timestamp", TimeStamp::now().getFloat()));

	return *this;
}

Document Changeset::newDocument() {
	return db.newDocument();
}

Document Changeset::newDocument(ConstStrA suffix) {
	return db.newDocument(suffix);
}

Changeset& Changeset::preview(LocalView& view) {
	docs->enumEntries(JSON::IEntryEnum::lambda([&](const JSON::INode *value, ConstStrA , natural){
		view.updateDoc(value);
		return false;
	}));
	return *this;


}

void Changeset::eraseConflicts(ConstValue docId, ConstValue conflictList) {
	if (conflictList != null)
		conflictList->enumEntries(JSON::IEntryEnum::lambda([&](const ConstValue &v, ConstStrA, natural ){
		erase(docId, v);
		return false;
	}));
}


} /* namespace assetex */

