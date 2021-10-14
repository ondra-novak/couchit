/*
 * updateProc.cpp
 *
 *  Created on: 13. 2. 2017
 *      Author: ondra
 */

#include "couchDB.h"
#include "showProc.h"

namespace couchit {

ShowResult::ShowResult() {}

ShowResult::ShowResult(const Value& result, const String& contentType)
	:result(result)
	,contentType(contentType)
{
}

BinaryView ShowResult::getContent() const {
	return result.getBinary(json::directEncoding);
}

Value ShowResult::getJSONContent() const {
	return result;
}

String ShowResult::getContentType() const {
	return contentType;
}

bool ShowResult::isJSON() {
	return contentType == "application/json";
}

ShowProc::ShowProc(const String path,bool cacheable):path(path),cacheable(cacheable) {

}

ShowResult ShowProc::operator ()(CouchDB& db, std::string_view docId, Value arguments) {
	return db.execShowProc(path,docId,arguments,cacheable?0:CouchDB::flgDisableCache);
}


}
