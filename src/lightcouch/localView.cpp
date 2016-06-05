/*
 * localView.cpp
 *
 *  Created on: 5. 6. 2016
 *      Author: ondra
 */

#include "lightspeed/base/containers/map.tcc"
#include "localView.h"

#include "couchDB.h"

namespace LightCouch {

LocalView::LocalView():QueryBase(Json(JSON::create().get()),0) {
	// TODO Auto-generated constructor stub

}

LocalView::~LocalView() {
	// TODO Auto-generated destructor stub
}

ConstValue LocalView::exec() {
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

void LocalView::map(const ConstValue& doc) {
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
	Query q = db.createQuery(view);
	Query::Result res = q.select(Query::any).exec();

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
				doc = json("_id",row.id);
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
	Shared _(static_cast<RWLock::ReadLock &>(lock));
	DocToKey::ListIter iter = docToKeyMap.find(docId);
	if (iter.hasItems()) {
		const ValueAndDoc *v = keyToValueMap.find(KeyAndDocId(iter.getNext(),docId));
		if (v) return v->doc;
	}
	return 0;
}


ConstValue LocalView::reduce(const ConstStringT<KeyAndDocId>&,const ConstStringT<ConstValue>&, bool rereduce) {
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


} /* namespace LightCouch */

