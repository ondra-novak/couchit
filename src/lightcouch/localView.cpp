/*
 * localView.cpp
 *
 *  Created on: 5. 6. 2016
 *      Author: ondra
 */

#include "lightspeed/base/containers/map.tcc"
#include "localView.h"

#include "couchDB.h"

#include "lightspeed/base/containers/autoArray.tcc"
namespace LightCouch {

LocalView::LocalView():json(JSON::create()) {

}

LocalView::LocalView(const Json &json):json(json) {

}

LocalView::~LocalView() {
}


void LocalView::updateDoc(const ConstValue& doc) {
	Exclusive _(lock);
	updateDocLk(doc);
}
void LocalView::updateDocLk(const ConstValue& doc) {
	ConstStrA docId = doc["_id"].getStringA();
	eraseDocLk(docId);
	ConstValue delFlag = doc["_deleted"];
	if (delFlag == null || delFlag.getBool() == false) {
		curDoc=doc;
		map(doc);
		curDoc = null;
	}
}

void LocalView::map(const ConstValue& doc)  {
	emit();
}


void LocalView::emit(const ConstValue& key, const ConstValue& value) {
	addDocLk(curDoc, key, value);
}

void LocalView::emit(const ConstValue& key) {
	addDocLk(curDoc, key, json(null));
}

void LocalView::emit() {
	addDocLk(curDoc, json(null), json(null));
}

void LocalView::eraseDocLk( ConstStrA docId) {
	DocToKey::ListIter list = docToKeyMap.find(docId);
	while (list.hasItems()) {
		const ConstValue &k = list.getNext();
		keyToValueMap.erase(KeyAndDocId(k,docId));
	}
	docToKeyMap.erase(docId);
}

void LocalView::loadFromView(CouchDB& db, const View& view, bool runMapFn) {
	LightCouch::Query q = db.createQuery(view);
	LightCouch::Query::Result res = q.select(Query::any).exec();

	Exclusive _(lock);

	while (res.hasItems()) {
		const Query::Row &row = res.getNext();

		ConstValue doc = row.doc;

		//runMapFn is true and there is document
		if (runMapFn && doc != null) {
			//process through the map function
			updateDocLk(doc);
		} else {
			//otherwise if there is no document
			if (doc == null) {
				//create fake one to store docId
				doc = db.json("_id",row.id);
			}
			//add this document
			addDocLk(doc,row.key,row.value);
		}
	}

}

void LocalView::eraseDoc(ConstStrA docId) {

	Exclusive _(lock);
	eraseDocLk(docId);

}

void LocalView::addDoc(const ConstValue& doc, const ConstValue& key,
		const ConstValue& value) {

	Exclusive _(lock);
	addDocLk(doc, key, value);
}

ConstValue LocalView::getDocument(const ConstStrA docId) const {
	Shared _(lock);
	DocToKey::ListIter iter = docToKeyMap.find(docId);
	if (iter.hasItems()) {
		const ValueAndDoc *v = keyToValueMap.find(KeyAndDocId(iter.getNext(),docId));
		if (v) return v->doc;
	}
	return 0;
}


ConstValue LocalView::reduce(const ConstStringT<KeyAndDocId>&,const ConstStringT<ConstValue>&, bool rereduce) const {
	return nil;
}

void LocalView::addDocLk(const ConstValue &doc, const ConstValue& key, const ConstValue& value) {

	ConstStrA docId = doc["_id"].getStringA();
	bool exist= false;
	keyToValueMap.insert(KeyAndDocId(key,docId),ValueAndDoc(value,doc),&exist);
	if (!exist) {
		docToKeyMap.insert(docId,key);
	}
}

static CompareResult compareJson(const ConstValue &left, const ConstValue &right) {
	if (left->getType() != right->getType()) {
		if (left->isNull()) return cmpResultLess;
		if (left->isObject()) return cmpResultGreater;
		if (left->isBool()) return right->isNull()?cmpResultGreater:cmpResultLess;
		if (left->isNumber()) return right->isNull() || right->isBool()?cmpResultGreater:cmpResultLess;
		if (left->isString()) return right->isArray() || right->isObject()?cmpResultLess:cmpResultGreater;
		if (left->isArray()) return right->isObject()?cmpResultLess:cmpResultGreater;
	} else {
		switch (left->getType()) {
		case JSON::ndNull:return cmpResultEqual;
		case JSON::ndBool: return left->getBool() == right->getBool()?cmpResultEqual:(left->getBool() == false?cmpResultLess:cmpResultGreater);
		case JSON::ndFloat:
		case JSON::ndInt: {
			double l = left->getFloat();
			double r = right->getFloat();
			if (l<r) return cmpResultLess;
			else if (l>r) return cmpResultGreater;
			else return cmpResultEqual;
		}
		case JSON::ndString: return left->getStringUtf8().compare(right->getStringUtf8());
		case JSON::ndArray: {
				JSON::ConstIterator li = left->getFwConstIter();
				JSON::ConstIterator ri = right->getFwConstIter();
				while (li.hasItems() && ri.hasItems()) {
					CompareResult r = compareJson(li.getNext(),ri.getNext());
					if (r != cmpResultEqual) return r;
				}
				if (li.hasItems()) return cmpResultGreater;
				if (ri.hasItems()) return cmpResultLess;
				return cmpResultEqual;
			}
		case JSON::ndObject: {
				JSON::ConstIterator li = left->getFwConstIter();
				JSON::ConstIterator ri = right->getFwConstIter();
				while (li.hasItems() && ri.hasItems()) {
					const JSON::ConstKeyValue &kvl = li.getNext();
					const JSON::ConstKeyValue &kvr = ri.getNext();

					CompareResult r = kvl.getStringKey().compare(kvr.getStringKey());
					if (r == cmpResultEqual) {
						r = compareJson(kvl,kvr);
						if (r == cmpResultEqual)
							continue;
					}
					return r;
				}
				if (li.hasItems()) return cmpResultGreater;
				if (ri.hasItems()) return cmpResultLess;
				return cmpResultEqual;
			}
		}
	}
}

CompareResult LocalView::KeyAndDocId::compare(const KeyAndDocId& other) const {
	CompareResult c = compareJson(key,other.key);
	if (c == cmpResultEqual) {
		return docId.compare(other.docId);
	} else {
		return c;
	}
}

ConstValue LocalView::searchKeys(const ConstValue &keys, natural groupLevel) const {

	Shared _(lock);

	Container rows = json.array();
	bool reduced = groupLevel != naturalNull;

	for (natural i = 0; i < keys.length(); i++) {

		ConstValue subrows = searchOneKey(keys[i]);
		if (reduced) {
			ConstValue r = runReduce(subrows);
			if (r == null) reduced = false;
			else {
				subrows = json("key", keys[i])
					      ("value", r);
			}
		}

		rows.load(subrows);

	}

	if (groupLevel == 0 && reduced) {
		 ConstValue r = runReduce(rows);
		 if (r != null) {
			 rows = json << json("key",null)
			 	 	 	 	    ("value",r);
		 }

	}
	return json("rows",rows);

}

ConstValue LocalView::searchOneKey(const ConstValue &key) const {

	KeyToValue::Iterator iter = keyToValueMap.seek(KeyAndDocId(key, ConstStrA()));
	Container res = json.array();

	while (iter.hasItems()) {
		const KeyToValue::KeyValue &kv = iter.getNext();
		if (compareJson(key, kv.key.key) != cmpResultEqual) break;

		res.add(json("id",kv.key.docId)
				    ("key",kv.key.key)
					("value",kv.value.value)
					("doc",kv.value.doc));
	}
	return res;
}


ConstValue LocalView::runReduce(const ConstValue &rows) const {

	AutoArray<KeyAndDocId, SmallAlloc<256> > keylist;
	AutoArray<ConstValue, SmallAlloc<256> > values;
	keylist.reserve(rows.length());
	values.reserve(rows.length());
	for (JSON::ConstIterator iter = rows->getFwConstIter(); iter.hasItems();) {
		const JSON::ConstValue &v = iter.getNext();
		keylist.add(KeyAndDocId(v["key"],v["id"].getStringA()));
		values.add(v["value"]);
	}
	return reduce(keylist,values,false);

}

ConstValue LocalView::searchRange(const ConstValue &startKey, const ConstValue &endKey,
natural groupLevel, bool descending, natural offset, natural limit, ConstStrA offsetDoc,
bool excludeEnd) const {

	Shared _(lock);

	KeyAndDocId startK(startKey,offsetDoc);
	KeyAndDocId endK(endKey,excludeEnd?ConstStrA():ConstStrA("\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF"));

	KeyAndDocId *seekPos;
	KeyAndDocId *stopPos;

	if (descending) {
		seekPos = &endK;
		stopPos = &startK;
	} else {
		seekPos = &startK;
		stopPos = &endK;
	}

	KeyToValue::Iterator iter = keyToValueMap.seek(*seekPos);
	if (descending) iter.setDir(Direction::backward);
	natural whLimit = naturalNull - offset < limit? offset+limit:naturalNull;

	while (iter.hasItems()) {

	}



}
LocalView::Query::Query(const LocalView& lview, const Json& json, natural viewFlags)
	:QueryBase(json,viewFlags),lview(lview) {}

ConstValue LocalView::Query::exec() {
	if (keys.empty()) {
		return lview.searchRange(startkey,endkey, groupLevel, descent, offset, maxlimit,offset_doc,(viewFlags & View::exludeEnd) != 0);
	} else {
		return lview.searchKeys(keys,groupLevel);
	}
}




} /* namespace LightCouch */
