/*
 * document.cpp
 *
 *  Created on: 4. 4. 2016
 *      Author: ondra
 */

#include "document.h"
#include "couchDB.h"


namespace couchit {


Document::Document(const Value& base):json::Object(base) {
}

StrViewA Document::getID() const {
	return getIDValue().getString();
}

StrViewA Document::getRev() const {
	return getRevValue().getString();
}



void Document::setContent(const Value& v) {
	Value id = getIDValue();
	Value rev = getRevValue();
	revert();
	setBaseObject(v);
	setID(id);
	setRev(rev);
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

void Document::setDeleted(StringView<StrViewA> fieldsToKept, bool timestamp) {
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


void Document::deleteAttachment(const StrViewA &name) {
	object("_attachments").unset(name);
}

void Document::optimizeAttachments() {
	Value a = attachments();
	if (a.defined() && a.empty()) {
		unset("_attachments");
	}
}

void Document::inlineAttachment(const StrViewA &name, const AttachmentDataRef &data) {
	object("_attachments").set(name,data.toInline());
}

Value Document::getAttachment(const StrViewA &name) const {
	return (*this)["_attachments"][name];
}

Document::Document(const StrViewA& id, const StrViewA& rev) {
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
	return String((*this)[CouchDB::fldPrevRevision]);
}

bool Document::isDeleted() const {
	return (*this)["_deleted"].getBool();
}

void Document::clear() {
	setBaseObject(Value(json::object,{getIDValue(),getRevValue()}));
	revert();
}
bool Document::isSame(const Value &otherDoc) const {
	return getIDValue() == otherDoc["_id"];
}
bool Document::isNewer(const Value &otherDoc) const {
	return Revision(getRevValue()).getRevId() < Revision(otherDoc["_rev"]).getRevId();
}


} /* namespace couchit */

