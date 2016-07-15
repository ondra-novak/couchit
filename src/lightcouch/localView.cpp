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

LocalView::LocalView():json(JSON::create()),updateReceiver(*this) {

}

LocalView::LocalView(const Json &json):json(json),updateReceiver(*this) {

}

LocalView::~LocalView() {
	cancelStream();
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

void LocalView::map(const ConstValue&)  {
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
	LightCouch::Result res = q.select(Query::any).exec();

	Exclusive _(lock);

	while (res.hasItems()) {
		const Row &row = res.getNext();

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


ConstValue LocalView::reduce(const ConstStringT<KeyAndDocId>&,const ConstStringT<ConstValue>&, bool ) const {
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



CompareResult LocalView::KeyAndDocId::compare(const KeyAndDocId& other) const {
	CompareResult c = compareJson(key,other.key);
	if (c == cmpResultEqual) {
		return compareStringsUnicode(docId,other.docId);
	} else {
		return c;
	}
}

ConstValue LocalView::searchKeys(const ConstValue &keys, natural groupLevel) const {

	Shared _(lock);

	Container rows = json.array();
	bool grouped = groupLevel > 0 && groupLevel < naturalNull;

	for (natural i = 0; i < keys.length(); i++) {

		ConstValue subrows = searchOneKey(keys[i]);
		if (grouped) {
			ConstValue r = runReduce(subrows);
			if (r == null) grouped = false;
			else {
				subrows = json("key", keys[i])
					      ("value", r);
			}
		}

		rows.load(subrows);

	}

	if (groupLevel == 0) {
		 ConstValue r = runReduce(rows);
		 if (r != null) {
			 rows = json << json("key",null)
			 	 	 	 	    ("value",r);
		 }

	}
	return json("rows",rows)("total_rows",keyToValueMap.length());

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

LocalView::Query LocalView::createQuery(natural viewFlags) const {
	return Query(*this,json,viewFlags, PostProcessFn());
}

void LocalView::cancelStream() {
	UpdateStream str = updateStream;
	updateStream.clear();
	if (str != null) {
		str.removeObserver(&updateReceiver);
		if (str.getState() == IPromiseControl::stateResolving) {
			str.wait();
		}

	}
}

void LocalView::setUpdateStream(const UpdateStream& stream) {
	cancelStream();
	if (stream != null)  {

		Exclusive _(lock);
		setUpdateStreamLk(stream);
	}
}

LocalView::Query LocalView::createQuery(natural viewFlags, PostProcessFn fn) const {
	return Query(*this,json,viewFlags,fn);
}

void LocalView::setUpdateStreamLk(UpdateStream stream) {

	updateStream = stream;
	stream.addObserver(&updateReceiver);
}


LocalView::UpdateStream LocalView::getUpdateStream() const {
	Shared _(lock);
	return updateStream;
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

static bool canGroupKeys(const ConstValue &subj, const ConstValue &sliced) {
	if (sliced == null) return false;
	if (subj->isArray()) {
		natural cnt = subj.length();
		if (cnt >= sliced.length()) {
			cnt = sliced.length();
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

static ConstValue sliceKey(const ConstValue &key, natural groupLevel, const Json &json) {
	if (key->isArray()) {
		if (key.length() <= groupLevel) return key;
		Container out = json.array();
		for (natural i = 0; i < groupLevel; i++)
			out.add(key[i]);
		return out;
	} else {
		return key;
	}

}

ConstValue LocalView::searchRange(const ConstValue &startKey, const ConstValue &endKey,
natural groupLevel, bool descending, natural offset, natural limit, ConstStrA offsetDoc,
bool excludeEnd) const {

	Shared _(lock);

	KeyAndDocId startK(startKey,offsetDoc);
	KeyAndDocId endK(endKey,excludeEnd?ConstStrA():ConstStrA("\xEF\xBF\xBF\xEF\xBF\xBF\xEF\xBF\xBF\xEF\xBF\xBF"));

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

	Container rows = json.array();
	ConstValue grows;


	while (iter->hasItems() && (iend == null || iend.value() != iter.value()) && whLimit > 0) {

		const KeyToValue::KeyValue &kv = iter->getNext();

		if (offset == 0)
			rows.add(json("id",kv.key.docId)
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
			Container res;
			Container collect;
			ConstValue lastKey = null;
			res = json.array();
			collect = json.array();

			rows->enumEntries(JSON::IEntryEnum::lambda([&](const ConstValue &val, ConstStrA, natural){

				ConstValue key = val["key"];
				if (!canGroupKeys(key, lastKey)) {

					if (lastKey != null) {
						ConstValue z = runReduce(collect);
						res.add(json("key",lastKey)("value", z));
					}
					lastKey = sliceKey(key, groupLevel, json);
					collect.clear();

				}
				collect.add(val);

				return false;
			}));

			if (!collect.empty()) {
				ConstValue z = runReduce(collect);
				res.add(json("key",lastKey)("value", z));
			}

			grows = res;
		}
	} else {
		grows = rows;
	}

	return json("rows",grows)("total_rows",keyToValueMap.length());


}
LocalView::Query::Query(const LocalView& lview, const Json& json, natural viewFlags, PostProcessFn ppfn)
	:QueryBase(json,viewFlags),lview(lview),ppfn(ppfn) {

}

ConstValue LocalView::Query::exec() {
	finishCurrent();
	ConstValue result;
	if (keys == null || keys.empty()) {
		result = lview.searchRange(startkey,endkey, groupLevel, descent, offset, maxlimit,offset_doc,(viewFlags & View::exludeEnd) != 0);
	} else {
		result = lview.searchKeys(keys,groupLevel);
	}
	if (ppfn) result = ppfn(json, args, result);
	return result;
}


LocalView::UpdateReceiver::UpdateReceiver(LocalView& view):view(view) {

}

void LocalView::UpdateReceiver::resolve(
		const UpdateStreamItem& result) throw () {
	try {
		Exclusive _(view.lock);
		view.updateDocLk(result.document);
		view.setUpdateStreamLk(result.nextItem);
	} catch (...) {

	}
}

void LocalView::UpdateReceiver::resolve(const PException&) throw () {
	//ignore exceptions
}

LocalView::UpdateStream LocalView::DocumentSource::createStream() {
	UpdateStream res;
	nextItem = res.getPromise();
	return res;
}

void LocalView::DocumentSource::updateDoc(const ConstValue &doc) {
	Promise<UpdateStreamItem> toResolve = nextItem;
	toResolve.resolve(Constructor2<UpdateStreamItem,ConstValue,UpdateStream>(doc, createStream()));
}


} /* namespace LightCouch */
