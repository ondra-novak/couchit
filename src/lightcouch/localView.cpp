/*
 * localView.cpp
 *
 *  Created on: 5. 6. 2016
 *      Author: ondra
 */

#include "lightspeed/base/containers/map.tcc"
#include "localView.h"

#include <lightspeed/base/constructor.h>
#include <lightspeed/base/streams/utf.h>
#include "collation.h"
#include "couchDB.h"

#include "lightspeed/base/containers/autoArray.tcc"
#include "lightspeed/base/actions/promise.tcc"

namespace LightCouch {

LocalView::LocalView():queryable(*this) {

}


LocalView::~LocalView() {
}


void LocalView::updateDoc(const Value& doc) {
	Exclusive _(lock);
	updateDocLk(doc);
}
void LocalView::updateDocLk(const Value& doc) {
	StringRef docId = doc["_id"].getString();
	eraseDocLk(docId);
	Value delFlag = doc["_deleted"];
	if (delFlag == null || delFlag.getBool() == false) {
		curDoc=doc;
		map(doc);
		curDoc = null;
	}
}

void LocalView::map(const Value&)  {
	emit();
}


void LocalView::emit(const Value& key, const Value& value) {
	addDocLk(curDoc, key, value);
}

void LocalView::emit(const Value& key) {
	addDocLk(curDoc, key,nullptr);
}

void LocalView::emit() {
	addDocLk(curDoc,nullptr, nullptr);
}

void LocalView::eraseDocLk(const StringRef &docId) {
	DocToKey::ListIter list = docToKeyMap.find(docId);
	while (list.hasItems()) {
		const Value &k = list.getNext();
		keyToValueMap.erase(KeyAndDocId(k,docId));
	}
	docToKeyMap.erase(docId);
}

void LocalView::loadFromView(CouchDB& db, const View& view, bool runMapFn) {
	LightCouch::Query q = db.createQuery(view);
	LightCouch::Result res = q.exec();

	Exclusive _(lock);

	while (res.hasItems()) {
		const Row &row = res.getNext();

		Value doc = row.doc;

		//runMapFn is true and there is document
		if (runMapFn && doc != null) {
			//process through the map function
			updateDocLk(doc);
		} else {
			//otherwise if there is no document
			if (doc == null) {
				//create fake one to store docId
				doc = Object("_id",row.id);
			}
			//add this document
			addDocLk(doc,row.key,row.value);
		}
	}

}

void LocalView::eraseDoc(const StringRef &docId) {

	Exclusive _(lock);
	eraseDocLk(docId);

}

void LocalView::addDoc(const Value& doc, const Value& key,
		const Value& value) {

	Exclusive _(lock);
	addDocLk(doc, key, value);
}

Value LocalView::getDocument(const StringRef &docId) const {
	Shared _(lock);
	DocToKey::ListIter iter = docToKeyMap.find(docId);
	if (iter.hasItems()) {
		const ValueAndDoc *v = keyToValueMap.find(KeyAndDocId(iter.getNext(),docId));
		if (v) return v->doc;
	}
	return 0;
}


Value LocalView::reduce(const ConstStringT<KeyAndDocId>&,const ConstStringT<Value>&, bool ) const {
	return Value();
}

void LocalView::addDocLk(const Value &doc, const Value& key, const Value& value) {

	StringRef docId = doc["_id"].getString();
	bool exist= false;
	keyToValueMap.insert(KeyAndDocId(key,docId),ValueAndDoc(value,doc),&exist);
	if (!exist) {
		docToKeyMap.insert(docId,key);
	}
}



CompareResult LocalView::KeyAndDocId::compare(const KeyAndDocId& other) const {
	CompareResult c = compareJson(key,other.key);
	if (c == cmpResultEqual) {
		return compareStringsUnicode(docId,other.docId);
	} else {
		return c;
	}
}

Value LocalView::searchKeys(const Value &keys, natural groupLevel) const {

	Shared _(lock);

	Array rows;
	bool grouped = groupLevel > 0 && groupLevel < naturalNull;

	keys.forEach([&](Value k) {

		Value subrows = searchOneKey(k);
		if (grouped) {
			Value r = runReduce(subrows);
			if (r.type() == json::undefined) grouped = false;
			else {
				subrows = Object("key", k)
					    		("value", r);
			}
		}

		subrows.forEach([&rows](Value v) {rows.add(v);return true;});
		return true;
	});

	if (groupLevel == 0) {
		 Value r = runReduce(rows);
		 if (r.type() == json::undefined) {
			 rows.clear();
			 rows.add(Object("key",nullptr)
			 	 	 	 	    ("value",r));
		 }

	}
	return Object("rows",rows)
				("total_rows",keyToValueMap.length());

}

Value LocalView::searchOneKey(const Value &key) const {

	KeyToValue::Iterator iter = keyToValueMap.seek(KeyAndDocId(key, StringRef()));
	Array res;

	while (iter.hasItems()) {
		const KeyToValue::KeyValue &kv = iter.getNext();
		if (compareJson(key, kv.key.key) != cmpResultEqual) break;

		res.add(Object("id",kv.key.docId)
				    ("key",kv.key.key)
					("value",kv.value.value)
					("doc",kv.value.doc));
	}
	return res;
}

Query LocalView::createQuery(natural viewFlags) const {
	View v(StringA(),viewFlags);
	return Query(v, queryable);
}

Value LocalView::runReduce(const Value &rows) const {

	AutoArray<KeyAndDocId, SmallAlloc<256> > keylist;
	AutoArray<Value, SmallAlloc<256> > values;
	keylist.reserve(rows.size());
	values.reserve(rows.size());
	rows.forEach([&](const Value &v) {
		keylist.add(KeyAndDocId(v["key"],v["id"].getString()));
		values.add(v["value"]);
		return true;
	});
	return reduce(keylist,values,false);

}

static bool canGroupKeys(const Value &subj, const Value &sliced) {
	if (sliced == null) return false;
	if (subj.type() == json::array) {
		natural cnt = subj.size();
		if (cnt >= sliced.size()) {
			cnt = sliced.size();
		} else {
			return false;
		}

		for (natural i = 0; i < cnt; i++) {
			if (compareJson(subj[i],sliced[i]) != cmpResultEqual) return false;
		}
		return true;
	} else {
		return compareJson(subj,sliced) == cmpResultEqual;
	}
}

static Value sliceKey(const Value &key, natural groupLevel) {
	if (key.type() == json::array) {
		if (key.size() <= groupLevel) return key;
		Array out;
		for (natural i = 0; i < groupLevel; i++)
			out.add(key[i]);
		return out;
	} else {
		return key;
	}

}

Value LocalView::searchRange(const Value &startKey, const Value &endKey,
natural groupLevel, bool descending, natural offset, natural limit,
const StringRef &offsetDoc,
bool excludeEnd) const {

	Shared _(lock);

	KeyAndDocId startK(startKey,offsetDoc);
	KeyAndDocId endK(endKey,excludeEnd?StringRef():StringRef("\xEF\xBF\xBF\xEF\xBF\xBF\xEF\xBF\xBF\xEF\xBF\xBF"));

	KeyAndDocId *seekPos;
	KeyAndDocId *stopPos;
	Direction::Type dir;

	if (descending) {
		seekPos = &endK;
		stopPos = &startK;
		dir = Direction::backward;
	} else {
		seekPos = &startK;
		stopPos = &endK;
		dir = Direction::forward;
	}





	Optional<KeyToValue::Iterator> iter;
	if (seekPos->key != null) {
		iter = keyToValueMap.seek(*seekPos,0,dir);
	} else {
		if (descending) iter = keyToValueMap.getBkIter();
		else iter = keyToValueMap.getFwIter();
	}

	Optional<KeyToValue::Iterator> iend;
	if (stopPos->key != null) {
		bool found;
		iend = keyToValueMap.seek(*stopPos,&found,dir);
	}
	natural whLimit = naturalNull - offset < limit? offset+limit:naturalNull;

	Array rows;
	Value grows;


	while (iter->hasItems() && (iend == null || iend.value() != iter.value()) && whLimit > 0) {

		const KeyToValue::KeyValue &kv = iter->getNext();

		if (offset == 0)
			rows.add(Object("id",kv.key.docId)
				    ("key",kv.key.key)
					("value",kv.value.value)
					("doc",kv.value.doc));
		else
			offset--;

		whLimit --;

	}

	if (groupLevel != naturalNull) {
		if (groupLevel == 0) {
			grows = runReduce(rows);
		} else {
			Array res;
			Array collect;
			Value lastKey = null;


			for (auto &&val : rows) {

				Value key = val["key"];
				if (!canGroupKeys(key, lastKey)) {

					if (lastKey != null) {
						Value z = runReduce(collect);
						res.add(Object("key",lastKey)("value", z));
					}
					lastKey = sliceKey(key, groupLevel);
					collect.clear();

				}
				collect.add(val);

				return false;
			}

			if (!collect.empty()) {
				Value z = runReduce(collect);
				res.add(Object("key",lastKey)("value", z));
			}

			grows = res;
		}
	} else {
		grows = rows;
	}

	return Object("rows",grows)
			("total_rows",keyToValueMap.length());


}


} /* namespace LightCouch */
