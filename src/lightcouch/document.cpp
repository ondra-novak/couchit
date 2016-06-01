/*
 * document.cpp
 *
 *  Created on: 4. 4. 2016
 *      Author: ondra
 */

#include "document.h"
#include "couchDB.h"

#include <lightspeed/base/containers/constStr.h>
#include "lightspeed/base/containers/autoArray.tcc"

namespace LightCouch {


Document::Document(const ConstValue& base):ConstValue(base),base(base) {
}

Document::Document(const Value& editableBase):ConstValue(editableBase),base(editableBase),editing(editableBase) {
}

ConstStrA Document::getID() const {
	ConstValue v = (*this)["_id"];
	if (v != null) return v.getStringA();
	else return ConstStrA();
}

ConstStrA Document::getRev() const {
	ConstValue v = (*this)["_rev"];
	if (v != null) return v.getStringA();
	else return ConstStrA();
}

Document &Document::set(ConstStrA key, const Value& value) {
	editing.set(key,value);
	return *this;
}

Document &Document::unset(ConstStrA key) {
	editing.unset(key);
	return *this;
}

void Document::revert() {
	editing = null;
	*this = base;
}

Json::Object Document::edit(const Json& json) {
	if (editing == null) {
		if (base == null) editing = json.object();
		else editing = base->copy(json.factory);
		*this = editing;
		cleanup();
	}
	return json.object(editing);
}

void Document::resolveConflicts() {
	conflictsToDelete = base["_conflicts"];
}

void LightCouch::Document::setRevision(const Json& json, const ConstValue& v) {
	editing = v->copy(json.factory);
	*this = editing;
	cleanup();
}


ConstValue Document::getConflictsToDelete() const {
	return conflictsToDelete;
}

void Document::cleanup() {
	//remove all couchDB reserved items
	for(auto iter = editing->getFwIter(); iter.hasItems();) {
		const JSON::KeyValue &kv = iter.getNext();
		ConstStrA name = kv.getStringKey();
		if (name.head(1) == ConstStrA("_")) editing->erase(name);
	}

	//resture _id and _rev
	ConstValue id = base["_id"];
	ConstValue rev = base["_rev"];
	if (id != null) editing.set("_id",static_cast<Value &>(id));
	if (rev != null) editing.set("_rev",static_cast<Value &>(rev));
}

void Document::setDeleted(const Json& json, ConstStringT<ConstStrA> fieldsToKept) {
	JSON::Value delDoc = json.object();
	cleanup();
	delDoc.set("_deleted",json(true));
	for (auto iter = fieldsToKept.getFwIter(); iter.hasItems();) {
		ConstStrA fld = iter.getNext();
		Value oldV = editing[fld];
		if (oldV != null) delDoc.set(fld,oldV);
	}
	setRevision(delDoc);
	enableTimestamp(json);
}

void Document::enableTimestamp(const Json& json) {
	edit(json)(CouchDB::fldPrevRevision,null);

}


} /* namespace LightCouch */

