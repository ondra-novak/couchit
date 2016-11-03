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

StrView Document::getID() const {
	return getIDValue().getString();
}

StrView Document::getRev() const {
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

void Document::setDeleted(StringView<StrView> fieldsToKept, bool timestamp) {
	json::Object delDoc;
	for (auto &&fld : fieldsToKept) {
		delDoc(fld,(*this)[fld]);
	}
	delDoc.set("_deleted",true);
	setContent(delDoc);
	if (timestamp) enableTimestamp();
}

void Document::enableTimestamp() {
	set(CouchDB::fldTimestamp,nullptr);
}

void Document::enableRevTracking() {
	set(CouchDB::fldPrevRevision,nullptr);
}


void Document::deleteAttachment(const StrView &name) {
	object("_attachments").unset(name);
}

void Document::inlineAttachment(const StrView &name, const AttachmentDataRef &data) {
	object("_attachments").set(name,data.toInline());
}

Value Document::getAttachment(const StrView &name) const {
	return (*this)["_attachments"][name];
}

Document::Document(const StrView& id, const StrView& rev) {
	set("_id",id);
	if (!rev.empty()) set("_rev",rev);
}

Value Document::attachments() const {
	return (*this)["_attachments"];
}

Value Document::conflicts() const {
	return (*this)["_conflicts"];
}

Value Document::getTimestamp() const {
	return (*this)[CouchDB::fldTimestamp];
}

String Document::getPrevRevision() const {
	return (*this)[CouchDB::fldPrevRevision];
}

bool Document::isDeleted() const {
	return (*this)["_deleted"].getBool();
}


} /* namespace LightCouch */

