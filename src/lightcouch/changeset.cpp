/*
 * changeSet.cpp
 *
 *  Created on: 11. 3. 2016
 *      Author: ondra
 */

#include "changeset.h"

#include "document.h"

#include "lightspeed/base/containers/autoArray.tcc"

//#include "validator.h"
namespace LightCouch {

Changeset::Changeset(CouchDB &db):json(db.json), db(db) {

	init();

}


Changeset& Changeset::insert(Document &document) {

	document.edit(json)("_id",db.genUIDFast());
	document.unset("_rev");
	docs.add(document);
	return *this;
}

Changeset& Changeset::update(Document &document) {
	if (!document.dirty()) return *this;
	docs.add(document);
	return *this;
}

Changeset& Changeset::erase(Document &document) {

	json.object(document)("_deleted", true);
	docs.add(document);
	return *this;
}


Changeset& Changeset::commit(CouchDB& db,bool all_or_nothing) {
	if (docs->empty()) return *this;
	///if validator is not present, do not run empty cycle.
	/*
	const Validator *v;
	if ((v = db.getValidator())!=0) {
		docs->enumEntries(JSON::IEntryEnum::lambda([v](const JSON::INode *nd, ConstStrA, natural){
			Validator::Result r = v->validateDoc(nd);
			if (!r) throw ValidationFailedException(THISLOCATION,r);
			return false;
		}));
	}*/
	if (all_or_nothing)
		json.object(wholeRequest)("all_or_nothing",true);
	JSON::ConstValue out = db.jsonPOST("_bulk_docs", wholeRequest);


	AutoArray<ErrorItem> errors;

	natural index = 0;
	for (JSON::ConstIterator iter = out->getFwIter(); iter.hasItems();) {
		const JSON::ConstKeyValue &kv = iter.getNext();

		JSON::ConstValue rev = kv["rev"];
		if (rev != null) json.object(docs[index])("_rev",rev);

		JSON::ConstValue err = kv["error"];
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

Changeset &Changeset::resolveConflict(const Conflicts& conflicts, Document &mergedDocument) {
	ConstStrA mergedRev = mergedDocument["_rev"].getStringA();
	for (Conflicts::Iterator iter = conflicts.getFwIter(); iter.hasItems(); ){
		Document doc = iter.getNext();
		if (doc.getRev() != mergedRev) {
			erase(doc);
		}
	}
	update(mergedDocument);
	return *this;

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

Changeset& Changeset::insert(ConstStrA id, Document& document) {
	document.edit(json)("_id",id);
	return update(document);
}


} /* namespace assetex */

