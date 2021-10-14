/*
 * updateProc.cpp
 *
 *  Created on: 13. 2. 2017
 *      Author: ondra
 */

#include "couchDB.h"
#include "updateProc.h"

namespace couchit {

UpdateResult::UpdateResult() {}

UpdateResult::UpdateResult(const Value& result, const String& contentType, const String& rev)
	:result(result)
	,contentType(contentType)
	,rev(rev)
{
}

BinaryView UpdateResult::getContent() const {
	return result.getBinary(json::directEncoding);
}

Value UpdateResult::getJSONContent() const {
	return result;
}

String UpdateResult::getContentType() const {
	return contentType;
}

String UpdateResult::getRev() const {
	return rev;
}

bool UpdateResult::isJSON() {
	return contentType == "application/json";
}

UpdateProc::UpdateProc(const String path):path(path) {

}

UpdateResult UpdateProc::operator ()(CouchDB& db, std::string_view docId, Value arguments) {
	return db.execUpdateProc(path,docId,arguments);
}


}
