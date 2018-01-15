/*
 * documentdb.cpp
 *
 *  Created on: Jan 9, 2018
 *      Author: ondra
 */

#include "documentdb.h"

#include "document.h"
#include "iidgen.h"

namespace couchit {

StrViewA DocumentDB::fldTimestamp("~timestamp");
StrViewA DocumentDB::fldPrevRevision("~prevRev");

DocumentDB::DocumentDB () {}

DocumentDB::DocumentDB (const DocumentDB &) {}

Value DocumentDB::genUIDValue() const {
	LockGuard _(lock);
	return StrViewA(lkGenUID());
}

Value DocumentDB::genUIDValue(StrViewA prefix)  const {
	LockGuard _(lock);
	return lkGenUID(prefix);
}


Document DocumentDB::newDocument() {
	return Document(lkGenUID(),StrViewA());
}

Document DocumentDB::newDocument(const StrViewA &prefix) {
	return Document(lkGenUID(StrViewA(prefix)),StrViewA());
}

StrViewA DocumentDB::lkGenUID() const {
	return getIDGenerator()(uidBuffer,StrViewA());
}

StrViewA DocumentDB::lkGenUID(StrViewA prefix) const {
	return getIDGenerator()(uidBuffer,prefix);
}

String DocumentDB::putAttachment(const Value &document, const StrViewA &attachmentName, const AttachmentDataRef &attachmentData) {
	Upload upld = putAttachment(document,attachmentName,attachmentData.contentType);
	upld.write(BinaryView(attachmentData));
	return upld.finish();

}

}
