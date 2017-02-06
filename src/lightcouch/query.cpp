#include "query.h"

#include <imtjson/abstractValue.h>
namespace LightCouch {

const StrViewA maxStrViewA("\xEF\xBF\xBF\xEF\xBF\xBF\xEF\xBF\xBF\xEF\xBF\xBF");


const Value Query::minKey(nullptr);
const Value Query::maxKey(Object(maxStrViewA,maxStrViewA));
const String Query::maxString(maxStrViewA);
const String Query::minString("");


Query::Query(const View& view, IQueryableObject& qao)
:request(view),qao(qao)
{
}

Query& Query::keys(const Value& v) {
	request.keys = v;
	request.mode = qmKeyList;
	return *this;
}


Query& Query::prefixKey(const Value& v) {
	request.mode = qmKeyPrefix;
	request.keys.clear();
	request.keys.add(v);
	return *this;
}

Query& Query::prefixString(const Value& v) {
	request.mode = qmStringPrefix;
	request.keys.clear();
	request.keys.add(v);
	return *this;
}

Query& Query::offset(std::size_t offset) {
	request.offset = offset;
	return *this;
}

Query& Query::limit(std::size_t limit) {
	request.limit = limit;
	return *this;
}

Query& Query::arg(const StrViewA& argname, const Value& value) {
	request.ppargs.set(argname,value);
	return *this;
}

Query& Query::reversedOrder() {
	request.reversedOrder = !request.reversedOrder;
	return *this;
}

Query& Query::noreduce() {
	request.reduceMode = rmNoReduce;
	return *this;
}

Query& Query::reduceAll() {
	request.reduceMode = rmReduce;
	return *this;
}

Query& Query::group() {
	request.reduceMode = rmGroup;
	return *this;
}

Query& Query::groupLevel(std::size_t level) {
	request.reduceMode = rmGroupLevel;
	request.groupLevel = level;
	return *this;
}

Query& Query::nosort() {
	request.nosort = true;
	return *this;
}

Query& Query::reset() {
	request.reset();
	return *this;
}

Query& Query::key(const Value& v) {
	request.mode = qmKeyList;
	request.keys.clear();
	request.keys.add(v);
	return *this;
}

Query& Query::range(const Value& from, const Value& to, std::size_t flags) {
	request.mode = qmKeyRange;
	request.keys.clear();
	request.keys.add(from);
	request.keys.add(to);
	request.docIdFromGetKey = (flags & docIdFromGetKey) != 0;
	request.exclude_end = (flags & excludeEnd) != 0;
	return *this;
}

Query& Query::range(const Value& from, const StrViewA& fromDoc,
		const Value& to, const StrViewA& toDoc, bool exclusiveEnd) {
	request.mode = qmKeyRange;
	request.keys.clear();
	if (fromDoc.empty() && toDoc.empty()) {
		request.keys.add(from);
		request.keys.add(to);
		request.docIdFromGetKey = false;
	} else {
		request.keys.add(from.setKey(fromDoc));
		request.keys.add(to.setKey(toDoc));
		request.docIdFromGetKey = true;
	}
	request.exclude_end = exclusiveEnd;
	return *this;
}

Value Query::exec() const {
	return qao.executeQuery(request);
}

Result::Result(const Value& result):pos(0),cnt(result.size()) {
	if (result.type() == json::object) {
		total = result["total_rows"].getUInt();
		offset = result["offset"].getUInt();
		Value::operator=(result["rows"]);
	} else {
		Value::operator=(result);
		total = offset = 0;
	}
	cnt = size();
}

Row::Row(const Value& jrow)
	:Value(jrow)
	,key(jrow["key"])
	,value(jrow["value"])
	,doc(jrow["doc"])
	,id(jrow["id"])
	,error(jrow["error"])
{
}

Query& Query::nocache() {
	request.nocache = true;
	return *this;
}


}

