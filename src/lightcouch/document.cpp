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
	ConstValue v = getIDValue();
	if (v != null) return v.getStringA();
	else return ConstStrA();
}

ConstStrA Document::getRev() const {
	ConstValue v = getRevValue();
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
		ConstValue::operator=(editing);
		cleanup();
	}
	return json.object(editing);
}

void Document::resolveConflicts() {
	conflictsToDelete = base["_conflicts"];
}

void Document::setRevision(const Json& json, const ConstValue& v) {
	editing = v->copy(json.factory);
	ConstValue::operator = (editing);
	cleanup();
}


ConstValue Document::getConflictsToDelete() const {
	return conflictsToDelete;
}

ConstValue Document::getIDValue() const {
	if (isNull()) return null;
	return (*this)["_id"];
}

ConstValue Document::getRevValue() const {
	if (isNull()) return null;
	return (*this)["_rev"];
}

void Document::setID(const ConstValue& id) {
	if (editing == null)
		throw DocumentNotEditedException(THISLOCATION, getIDValue());
	editing.set("_id",static_cast<const Value &>(id));

}

void Document::setRev(const ConstValue& rev) {
	if (editing == null)
		throw DocumentNotEditedException(THISLOCATION, getIDValue());
	editing.set("_rev",static_cast<const Value &>(rev));
}

void Document::cleanup() {
	//remove all couchDB reserved items
	for(auto iter = editing->getFwIter(); iter.hasItems();) {
		const JSON::KeyValue &kv = iter.getNext();
		ConstStrA name = kv.getStringKey();
		if (name.head(1) == ConstStrA("_")) editing->erase(name);
	}

	if (base != null) {
	//resture _id and _rev
		ConstValue id = base["_id"];
		ConstValue rev = base["_rev"];
		if (id != null) editing.set("_id",static_cast<Value &>(id));
		if (rev != null) editing.set("_rev",static_cast<Value &>(rev));
	}
}

void Document::setDeleted(const Json& json, ConstStringT<ConstStrA> fieldsToKept) {
	JSON::Value delDoc = json.object();
	for (auto iter = fieldsToKept.getFwIter(); iter.hasItems();) {
		ConstStrA fld = iter.getNext();
		Value oldV = editing[fld];
		if (oldV != null) delDoc.set(fld,oldV);
	}
	setRevision(delDoc);
	delDoc.set("_deleted",json(true));
	enableTimestamp();
}

void Document::enableTimestamp() {
	if (editing == null) throw DocumentNotEditedException(THISLOCATION, getIDValue());
	editing.set(CouchDB::fldTimestamp,JSON::getConstant(JSON::constNull));
}

void Document::enableRevTracking() {
	if (editing == null) throw DocumentNotEditedException(THISLOCATION, getIDValue());
	editing.set(CouchDB::fldPrevRevision,JSON::getConstant(JSON::constNull));
}




} /* namespace LightCouch */
