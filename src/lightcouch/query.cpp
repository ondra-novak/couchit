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

Query::Query(CouchDB &db,const View &view)
:json(db.json),db(db),viewDefinition(view),mode(mdKeys) {
	reset();
}


Query& Query::reset() {
	curKeySet.clear();
	startkey = endkey = keys = null;
	mode = mdKeys;
	staleMode = viewDefinition.flags & View::stale?smStale:(
			viewDefinition.flags & View::updateAfter?smUpdateAfter:smUpdate);

	groupLevel = (viewDefinition.flags & View::reduce)?((viewDefinition.flags & View::groupLevelMask) / View::groupLevel):naturalNull  ;

	offset = 0;
	maxlimit = naturalNull;
	descent = (viewDefinition.flags & View::reverseOrder) != 0;
	forceArray = false;
	args = null;

	return *this;
}


Query& Query::selectKey(JCValue key) {
	if (keys== nil) {
		keys = json.array();
	}
	keys.add(key);
	return *this;

}

Query& Query::fromKey(JCValue key) {
	keys = nil;
	startkey = key;
	return *this;

}

Query& Query::toKey(JCValue key) {
	keys = nil;
	endkey = key;
	return *this;
}

Query& Query::operator ()(ConstStrA key) {
	curKeySet.add(json(key));
	return *this;
}

Query& Query::operator ()(natural key) {
	curKeySet.add(json(key));
	return *this;
}

Query& Query::operator ()(integer key) {
	curKeySet.add(json(key));
	return *this;
}

Query& Query::operator ()(int key) {
	curKeySet.add(json(integer(key)));
	return *this;
}

Query& Query::operator ()(double key) {
	curKeySet.add(json(key));
	return *this;
}

Query& Query::operator ()(JCValue key) {
	curKeySet.add(key);
	return *this;
}

Query& Query::operator ()(bool key) {
	curKeySet.add(json(key));
	return *this;
}

Query& Query::operator ()(const char *key) {
	curKeySet.add(json(ConstStrA(key)));
	return *this;
}



Query& Query::operator()(MetaValue metaValue) {
	switch (metaValue) {
	case any: {
		forceArray=true;
			switch(mode) {
				case mdKeys:
					startkey = endkey = keys = null;
					if (curKeySet.empty()) return *this;
					endkey = buildKey(curKeySet);
					startkey = buildKey(curKeySet);
					startkey->add(json(null));
					endkey->add(json("\xEF\xBF\xBF",null));
					curKeySet.clear();
					return *this;
				case mdStart:
					curKeySet.add(json(null));
					return *this;
				case mdEnd:
					curKeySet.add(json("\xEF\xBF\xBF",null));
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
							+ConstStrA("\xEF\xBF\xBF")));
				endkey = buildKey(curKeySet);
				curKeySet.clear();
				return *this;
			case mdStart:
				return *this;
			case mdEnd:
				if (curKeySet.empty()) return *this;
				curKeySet(curKeySet.length()-1) = json(
						StringA(curKeySet(curKeySet.length()-1)->getStringUtf8()
							+ConstStrA("\xEF\xBF\xBF")));
				return *this;
			}

		}
	case isArray:
		forceArray = true;
		return *this;
	}
}

void Query::appendCustomArg(UrlFormatter &fmt, ConstStrA key, ConstStrA value ) {
	fmt("&%1=%2") << key << value;
}

void Query::appendCustomArg(UrlFormatter &fmt, ConstStrA key, const JSON::INode * value ) {
	if (value->getType() == JSON::ndString) {
		appendCustomArg(fmt,key,CouchDB::urlencode(value->getStringUtf8()));
	} else {
		ConstStrA str = json.factory->toString(*value);
		appendCustomArg(fmt,key,CouchDB::urlencode(str));
	}
}

JCValue Query::exec(CouchDB &db) {
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

		return db.jsonGET(urlline.getArray());
	} else if (keys->length() == 1) {
		urlformat("&key=%1") << (hlp=CouchDB::urlencode(json.factory->toString(*(keys[0]))));
		return db.jsonGET(urlline.getArray());
	} else {
		if (viewDefinition.flags & View::forceGETMethod) {
			urlformat("&keys=%1") << (hlp=CouchDB::urlencode(json.factory->toString(*keys)));
			return db.jsonGET(urlline.getArray());
		} else {
			JSON::Value req = json("keys",keys);
			return db.jsonPOST(urlline.getArray(), req);
		}
	}

}

Query& Query::group(natural level) {
	groupLevel = level;
	return *this;
}

JSON::Value Query::buildKey(ConstStringT<JSON::Value> values) {
	if (!forceArray && values.length() == 1) return values[0];
	else return json(values);
}

Query& Query::reverseOrder() {
	descent = !descent;
	return *this;
}

Query& Query::limit(natural limit) {
	this->maxlimit = limit;
	this->offset = 0;
	this->offset_doc = StringA();
	return *this;
}

Query& Query::limit(natural offset, natural limit) {
	this->maxlimit = limit;
	this->offset = offset;
	this->offset_doc = StringA();
	return *this;
}

Query& Query::limit(ConstStrA docId, natural limit) {
	this->maxlimit = limit;
	this->offset_doc = docId;
	this->offset = 0;
	return *this;
}

Query& Query::updateAfter() {
	staleMode = smUpdateAfter;
	return *this;
}

Query& Query::stale() {
	staleMode = smStale;
	return *this;
}

Query::Query(const Query& other)
	:json(other.json),db(other.db)
	,viewDefinition(other.viewDefinition)
{
}


void Query::finishCurrent()
{
	if (curKeySet.empty())
		return;
	JSON::Value arr = buildKey(curKeySet);
	curKeySet.clear();
	switch (mode) {
		case mdKeys: {
			if (keys == nil) {
				startkey = endkey = nil;
				keys = json.array();
			}
			keys->add(arr);
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


Query::Result::Result(JCValue jsonResult)
:rows(jsonResult["rows"])
,rowIter(jsonResult["rows"]->getFwIter())
{
	JSON::INode *jtotal = jsonResult->getPtr("total_rows");
	if (jtotal) total = jtotal->getUInt(); else total = rows->length();
	JSON::INode *joffset = jsonResult->getPtr("offset");
	if (joffset) offset = joffset->getUInt(); else offset = 0;
}

const JCValue& Query::Result::getNext() {
	out = rowIter.getNext();
	return out;
}

const JCValue& Query::Result::peek() const {
	out = rowIter.peek();
	return out;
}

bool Query::Result::hasItems() const {
	return rowIter.hasItems();
}

natural Query::Result::getTotal() const {
	return total;
}

natural Query::Result::getOffset() const {
	return offset;
}

natural Query::Result::length() const {
	return rows->length();
}

natural Query::Result::getRemain() const {
	return rowIter.getRemain();
}

void Query::Result::rewind() {
	rowIter = rows->getFwIter();
}

static ConstStrA getIDFromRow(const JValue& jrow) {
	const JSON::INode *nd = jrow->getPtr("id");
	if (nd) return nd->getStringUtf8();
	else return ConstStrA();
}

Query::Row::Row(const JCValue& jrow)
	:key(jrow->getPtr("key"))
	,value(jrow->getPtr("value"))
	,doc(jrow->getPtr("doc"))
	,id(getIDFromRow(jrow))
{}

JSON::Value Query::initArgs() {
	if (args==null) args = json.object();
	return args;
}


JCValue Query::exec() {
	return exec(db);
}
Query::~Query() {
}

}

