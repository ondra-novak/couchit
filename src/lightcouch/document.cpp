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


Document::Document(const Value& base):json::Object(base) {
}

StringRef Document::getID() const {
	return getIDValue().getString();
}

StringRef Document::getRev() const {
	return getRevValue().getString();
}


void Document::resolveConflicts() {
	conflictsToDelete = base["_conflicts"];
}

void Document::setContent(const Value& v) {
	Value id = getIDValue();
	Value rev = getRevValue();
	revert();
	setBaseObject(v);
	setID(id);
	setRev(rev);
}


Value Document::getConflictsToDelete() const {
	return conflictsToDelete;
}

Value Document::getIDValue() const {
	return (*this)["_id"];
}

Value Document::getRevValue() const {
	return (*this)["_rev"];
}

void Document::setID(const Value& id) {
	set("_id",id);
}

void Document::setRev(const Value& rev) {
	set("_rev",rev);
}

void Document::setDeleted(StringRefT<StringRef> fieldsToKept) {
	json::Object delDoc;
	for (auto &&fld : fieldsToKept) {
		delDoc(fld,(*this)[fld]);
	}
	delDoc.set("_deleted",true);
	setContent(delDoc);
	enableTimestamp();
}

void Document::enableTimestamp() {
	set(CouchDB::fldTimestamp,nullptr);
}

void Document::enableRevTracking() {
	set(CouchDB::fldPrevRevision,nullptr);
}


void Document::deleteAttachment(const StringRef &name) {
	object("_attachments").unset(name);
}

void Document::inlineAttachment(const StringRef &name, const AttachmentDataRef &data) {
	object("_attachments").set(name,data.toInline());
}

Value Document::getAttachment(const StringRef &name) const {
	return (*this)["_attachments"][name];
}


} /* namespace LightCouch */
