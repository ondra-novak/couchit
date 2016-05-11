/*
 * changeSet.cpp
 *
 *  Created on: 11. 3. 2016
 *      Author: ondra
 */

#include "changeset.h"

#include "document.h"

#include "lightspeed/base/containers/autoArray.tcc"
namespace LightCouch {

Changeset::Changeset(CouchDB &db):json(db.json), db(db) {

	init();

}


Changeset& Changeset::insert(JSON::Value document, ConstStrA *id) {

	JSON::Value v = document->getPtr("_id");
	if (v == nil) {
		v = json(ConstStrA(CouchDB::genUIDFast()));
		document->add("_id",v);
	}

	if (id) *id = v->getStringUtf8();
	docs->add(document);
	return *this;
}

Changeset& Changeset::update(JSON::Value document) {
	document["_id"];
	docs->add(document);
	return *this;
}

Changeset& Changeset::erase(JSON::Value document) {

	json.object(document)("_deleted", true);
	docs->add(document);
	return *this;
}


Changeset& Changeset::commit(CouchDB& db,bool all_or_nothing) {
	if (docs->empty()) return *this;
	if (all_or_nothing)
		json.object(wholeRequest)("all_or_nothing",true);
	JSON::Value out = db.jsonPOST("_bulk_docs", wholeRequest);


	AutoArray<ErrorItem> errors;

	natural index = 0;
	for (JSON::Iterator iter = out->getFwIter(); iter.hasItems();) {
		const JSON::KeyValue &kv = iter.getNext();

		JSON::Value rev = kv->getPtr("rev");
		if (rev != null) docs[index]->replace("_rev",rev);

		JSON::Value err = kv->getPtr("error");
		if (err != null) {
			ErrorItem e;
			e.errorDetails = kv;
			e.document = docs[index];
			e.errorType = err->getStringUtf8();
			e.reason = kv["reason"]->getStringUtf8();
			errors.add(e);
		}

		index++;
	}

	if (errors.length()) throw UpdateException(THISLOCATION,errors);

	return *this;


}

Changeset::Changeset(const Changeset& other):json(other.json),db(other.db) {
}

Changeset &Changeset::resolveConflict(const Conflicts& conflicts, JSON::Value mergedDocument, bool merge) {
	ConstStrA mergedRev = mergedDocument["_rev"]->getStringUtf8();
	for (Conflicts::Iterator iter = conflicts.getFwIter(); iter.hasItems(); ){
		const Document &doc = iter.getNext();
		if (doc.revision != mergedRev) erase(json("_id",doc.allData["_id"])
											     ("_rev",doc.allData["_rev"]));
	}
	if (merge) {
		update(mergedDocument);
	}
	return *this;

}

natural Changeset::mark() const {
	return docs->length();
}

void Changeset::revert(natural mark) {
	if (mark == 0) {
		docs->clear();
	} else {
		while (docs->length() != mark) {
			docs->erase(docs->length()-1);
		}
	}
}

void Changeset::revert(JValue doc) {
	for (natural i = 0, cnt = docs->length(); i < cnt; i++) {
		if (docs[i] == doc) {
			docs->erase(i);
			break;
		}
	}
}

void Changeset::init() {
	docs = json.array();
	wholeRequest = json("docs",docs);
}


Changeset::UpdateException::UpdateException(
		const ProgramLocation& loc, const StringCore<ErrorItem>& errors)

	:Exception(loc),errors(errors)
{
}

ConstStringT<Changeset::ErrorItem> Changeset::UpdateException::getErrors() const {
	return errors;
}

void Changeset::UpdateException::message(ExceptionMsg& msg) const {
	msg("Update exception - some items was not written: %1") << errors.length();
}

Changeset& Changeset::commit(bool all_or_nothing) {
	return commit(db,all_or_nothing);
}

} /* namespace assetex */

