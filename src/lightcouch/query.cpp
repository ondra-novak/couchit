/*
 * query.cpp
 *
 *  Created on: 12. 3. 2016
 *      Author: ondra
 */

#include <lightspeed/base/text/textOut.tcc>
#include "query.h"
#include "lightspeed/base/containers/autoArray.tcc"

#include "couchDB.h"

namespace LightCouch {

QueryBase::QueryBase(const Json &json, natural viewFlags)
:json(json),mode(mdKeys),viewFlags(viewFlags) {
	reset();
}


QueryBase& QueryBase::reset() {
	curKeySet.clear();
	startkey = endkey = keys = null;
	mode = mdKeys;
	staleMode = viewFlags & View::stale?smStale:(
			viewFlags & View::updateAfter?smUpdateAfter:smUpdate);

	groupLevel = (viewFlags & View::reduce)?((viewFlags & View::groupLevelMask) / View::groupLevel):naturalNull  ;

	offset = 0;
	maxlimit = naturalNull;
	descent = (viewFlags & View::reverseOrder) != 0;
	forceArray = false;
	args = null;

	return *this;
}


QueryBase& QueryBase::selectKey(ConstValue key) {
	if (keys== nil) {
		keys = json.array();
	}
	keys.add(key);
	return *this;

}

QueryBase& QueryBase::fromKey(ConstValue key) {
	keys = nil;
	startkey = key;
	return *this;

}

QueryBase& QueryBase::toKey(ConstValue key) {
	keys = nil;
	endkey = key;
	return *this;
}

QueryBase& QueryBase::operator ()(ConstStrA key) {
	curKeySet.add(json(key));
	return *this;
}

QueryBase& QueryBase::operator ()(natural key) {
	curKeySet.add(json(key));
	return *this;
}

QueryBase& QueryBase::operator ()(integer key) {
	curKeySet.add(json(key));
	return *this;
}

QueryBase& QueryBase::operator ()(int key) {
	curKeySet.add(json(integer(key)));
	return *this;
}

QueryBase& QueryBase::operator ()(double key) {
	curKeySet.add(json(key));
	return *this;
}

QueryBase& QueryBase::operator ()(ConstValue key) {
	curKeySet.add(key);
	return *this;
}

QueryBase& QueryBase::operator ()(bool key) {
	curKeySet.add(json(key));
	return *this;
}

QueryBase& QueryBase::operator ()(const char *key) {
	curKeySet.add(json(ConstStrA(key)));
	return *this;
}



QueryBase& QueryBase::operator()(MetaValue metaValue) {
	switch (metaValue) {
	case any: {
		forceArray=true;
		JSON::Container s,e;

		switch(mode) {
				case mdKeys:
					startkey = endkey = keys = null;
					if (curKeySet.empty()) return *this;
					e = buildRangeKey(curKeySet);
					s = buildRangeKey(curKeySet);
					s.add(json(null));
					e.add(json("\xEF\xBF\xBF",null));
					startkey = s;
					endkey = e;
					curKeySet.clear();
					return *this;
				case mdStart:
					curKeySet.add(json(null));
					return *this;
				case mdEnd:
					curKeySet.add(json("\xEF\xBF\xBF\xEF\xBF\xBF\xEF\xBF\xBF\xEF\xBF\xBF",null));
					return *this;
			}
		}
	case wildcard: {
			switch (mode) {
			case mdKeys:
				startkey = endkey = keys = null;
				if (curKeySet.empty()) return *this;
				startkey = buildKey(curKeySet);
				curKeySet(curKeySet.length()-1) = json(
						StringA(curKeySet(curKeySet.length()-1)->getStringUtf8()
							+ConstStrA("\xEF\xBF\xBF\xEF\xBF\xBF\xEF\xBF\xBF\xEF\xBF\xBF")));
				endkey = buildKey(curKeySet);
				curKeySet.clear();
				return *this;
			case mdStart:
				return *this;
			case mdEnd:
				if (curKeySet.empty()) return *this;
				curKeySet(curKeySet.length()-1) = json(
						StringA(curKeySet(curKeySet.length()-1)->getStringUtf8()
							+ConstStrA("\xEF\xBF\xBF\xEF\xBF\xBF\xEF\xBF\xBF\xEF\xBF\xBF")));
				return *this;
			}

		}
	case isArray:
		forceArray = true;
		return *this;
	}
}

void QueryBase::appendCustomArg(UrlFormatter &fmt, ConstStrA key, ConstStrA value ) {
	fmt("&%1=%2") << key << value;
}

void QueryBase::setJsonFactory(JSON::PFactory json) {
}

void QueryBase::appendCustomArg(UrlFormatter &fmt, ConstStrA key, const JSON::INode * value ) {
	if (value->getType() == JSON::ndString) {
		appendCustomArg(fmt,key,CouchDB::urlencode(value->getStringUtf8()));
	} else {
		ConstStrA str = json.factory->toString(*value);
		appendCustomArg(fmt,key,CouchDB::urlencode(str));
	}
}

ConstValue Query::exec(CouchDB &db) {
	finishCurrent();

	StringA hlp;

	urlline.clear();
	urlline.blockWrite(viewDefinition.viewPath,true);
	TextOut<AutoArrayStream<char> &, SmallAlloc<256> > urlformat(urlline);
	if (groupLevel==naturalNull)  urlformat("?reduce=false");
	else if (keys == null || keys->length() == 1){
		urlformat("?group_level=%1") << groupLevel;
	} else if (groupLevel > 0){
		urlformat("?group=true") ;
	}

	if (descent) {
		urlformat("&descending=true");
	}

	if (viewDefinition.flags & View::includeDocs) {
		urlformat("&include_docs=true");
	}

	if (offset) {
		urlformat("&skip=%1") << offset;
	}

	if (maxlimit!=naturalNull) {
		urlformat("&limit=%1") << maxlimit;
	}

	if (!offset_doc.empty()) {
		urlformat("&startkey_docid=%1") << (hlp=CouchDB::urlencode(offset_doc));
	}

	if (viewDefinition.flags & View::updateSeq) {
		urlformat("&update_seq=true");
	}

	if (viewDefinition.flags & View::exludeEnd) {
		urlformat("&inclusive_end=false");
	}

	if (args == null) {
		for (natural i = 0, cnt = viewDefinition.args.length(); i < cnt; i++) {
			appendCustomArg(urlformat,viewDefinition.args[i].key,viewDefinition.args[i].value);
		}
	} else {
		for (natural i = 0, cnt = viewDefinition.args.length(); i < cnt; i++) {
			JSON::INode *nd = args->getPtr(viewDefinition.args[i].key);
			if (nd==0)
				appendCustomArg(urlformat,viewDefinition.args[i].key,viewDefinition.args[i].value);
		}
		for (JSON::Iterator iter = args->getFwIter(); iter.hasItems();) {
			const JSON::KeyValue &kv = iter.getNext();
			appendCustomArg(urlformat,kv->getStringUtf8(), kv);
		}
	}

	switch (staleMode) {
	case smUpdate:break;
	case smUpdateAfter: urlformat("&stale=update_after");break;
	case smStale: urlformat("&stale=ok");break;
	}

	if (keys == nil) {

		if (startkey != nil) {
			urlformat(descent?"&endkey=%1":"&startkey=%1") << (hlp=CouchDB::urlencode(json.factory->toString(*startkey)));
		}
		if (endkey != nil) {
			urlformat(descent?"&startkey=%1":"&endkey=%1") << (hlp=CouchDB::urlencode(json.factory->toString(*endkey)));
		}

		return db.requestGET(urlline.getArray());
	} else if (keys->length() == 1) {
		urlformat("&key=%1") << (hlp=CouchDB::urlencode(json.factory->toString(*(keys[0]))));
		return db.requestGET(urlline.getArray());
	} else {
		if (viewDefinition.flags & View::forceGETMethod) {
			urlformat("&keys=%1") << (hlp=CouchDB::urlencode(json.factory->toString(*keys)));
			return db.requestGET(urlline.getArray());
		} else {
			JSON::Container req = json("keys",keys);
			return db.requestPOST(urlline.getArray(), req);
		}
	}

}

QueryBase& QueryBase::group(natural level) {
	groupLevel = level;
	return *this;
}

JSON::ConstValue QueryBase::buildKey(ConstStringT<ConstValue> values) {
	if (!forceArray && values.length() == 1) {
		return json(values[0]);
	}
	else return json(values);
}

JSON::Container QueryBase::buildRangeKey(ConstStringT<ConstValue> values) {
	return json(values);
}


QueryBase& QueryBase::reverseOrder() {
	descent = !descent;
	return *this;
}

QueryBase& QueryBase::limit(natural limit) {
	this->maxlimit = limit;
	this->offset = 0;
	this->offset_doc = StringA();
	return *this;
}

QueryBase& QueryBase::limit(natural offset, natural limit) {
	this->maxlimit = limit;
	this->offset = offset;
	this->offset_doc = StringA();
	return *this;
}

QueryBase& QueryBase::limit(ConstStrA docId, natural limit) {
	this->maxlimit = limit;
	this->offset_doc = docId;
	this->offset = 0;
	return *this;
}

QueryBase& QueryBase::updateAfter() {
	staleMode = smUpdateAfter;
	return *this;
}

QueryBase& QueryBase::stale() {
	staleMode = smStale;
	return *this;
}

QueryBase::QueryBase(const QueryBase& other)
	:json(other.json)
{
}


void QueryBase::finishCurrent()
{
	if (curKeySet.empty())
		return;
	JSON::ConstValue arr = buildKey(curKeySet);
	curKeySet.clear();
	switch (mode) {
		case mdKeys: {
			if (keys == nil) {
				startkey = endkey = nil;
				keys = json.array();
			}
			keys.add(arr);
			return;
		}
		case mdStart: {
			keys = nil;
			startkey = arr;
			return;
		}
		case mdEnd: {
			keys = nil;
			endkey = arr;
			return;
		}
	}
}


QueryBase::Result::Result(ConstValue jsonResult)
:rows(jsonResult["rows"])
,rowIter(jsonResult["rows"]->getFwIter())
{
	JSON::INode *jtotal = jsonResult->getPtr("total_rows");
	if (jtotal) total = jtotal->getUInt(); else total = rows->length();
	JSON::INode *joffset = jsonResult->getPtr("offset");
	if (joffset) offset = joffset->getUInt(); else offset = 0;
}

const ConstValue& QueryBase::Result::getNext() {
	out = rowIter.getNext();
	return out;
}

const ConstValue& QueryBase::Result::peek() const {
	out = rowIter.peek();
	return out;
}

bool QueryBase::Result::hasItems() const {
	return rowIter.hasItems();
}

natural QueryBase::Result::getTotal() const {
	return total;
}

natural QueryBase::Result::getOffset() const {
	return offset;
}

natural QueryBase::Result::length() const {
	return rows->length();
}

natural QueryBase::Result::getRemain() const {
	return rowIter.getRemain();
}

void QueryBase::Result::rewind() {
	rowIter = rows->getFwIter();
}

static ConstStrA getIDFromRow(const ConstValue & jrow) {
	const JSON::INode *nd = jrow->getPtr("id");
	if (nd) return nd->getStringUtf8();
	else return ConstStrA();
}

QueryBase::Row::Row(const ConstValue& jrow)
	:key(jrow->getPtr("key"))
	,value(jrow->getPtr("value"))
	,doc(jrow->getPtr("doc"))
	,id(getIDFromRow(jrow))
{}

JSON::Value QueryBase::initArgs() {
	if (args==null) args = json.object();
	return args;
}


ConstValue Query::exec() {
	return exec(db);
}
QueryBase::~QueryBase() {
}

Query::Query(CouchDB &db, const View &view):QueryBase(db.json,view.flags),db(db),viewDefinition(view) {
}

Query::Query(const Query& other):QueryBase(other),db(other.db),viewDefinition(other.viewDefinition) {
}

Query::~Query() {

}

}

