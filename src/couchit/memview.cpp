#include "memview.h"

#include "iterrange.h"


namespace couchit {

StrViewA MapReq::getID() const {
	return (*this)["_id"].getString();
}

StrViewA MapReq::getRev() const {
	return (*this)["_rev"].getString();
}

Value MapReq::getIDValue() const {
	return (*this)["_id"];
}

Value MapReq::getRevValue() const {
	return (*this)["_rev"];
}

Value MapReq::getConflicts() const {
	return (*this)["_conflicts"];
}

void MemView::load(const Query& q) {

	clear();
	Result res = q.exec();

	for (Row rw : res) {
		addDoc(rw.doc,rw.key, rw.value);
	}
	updateSeq = res.getUpdateSeq();

}

void MemView::load(CouchDB& db, const View& view) {
	load(db.createQuery(view));
}

void MemView::eraseDoc(const String& docId) {
	IterRange<DocToKey::const_iterator> rng ( docToKeyMap.equal_range(docId));
	for (auto &&itm : rng) {
		keyToValueMap.erase(KeyAndDocId(itm.second, itm.first));
	}
	docToKeyMap.erase(docId);
}

void MemView::addDoc(const Value& doc, const Value& key, const Value& value) {
	String id (doc["_id"]);
	Value idocs = flags & flgIncludeDocs? doc : Value(nullptr);

	keyToValueMap.insert(std::make_pair(KeyAndDocId(key,id),ValueAndDoc(value,idocs)));
	docToKeyMap.insert(std::make_pair(String(id),key));
}

Value MemView::getDocument(const String& docId) const {
	auto it = docToKeyMap.find(docId);
	if (it == docToKeyMap.end()) return Value();
	auto it2 = keyToValueMap.find(KeyAndDocId(it->second, docId));
	if (it2 == keyToValueMap.end()) return Value();
	return it2->second.doc;
}

Query MemView::createQuery(std::size_t viewFlags) const {
	View v(String(),viewFlags);
	return Query(v, queryable);
}

Query MemView::createQuery(std::size_t viewFlags, View::Postprocessing fn, CouchDB* db) const {
	View v(String(),viewFlags,fn);
	return Query(v, queryable);
}

void MemView::clear() {
	docToKeyMap.clear();
	keyToValueMap.clear();
	updateSeq = Value(json::undefined);
}


bool MemView::empty() const {
	return keyToValueMap.empty();
}


MemView::Queryable::Queryable(const MemView& mview):mview(mview) {
}

Value MemView::Queryable::executeQuery(const QueryRequest& r) {

	return mview.runQuery(r);

}

int MemView::KeyAndDocId::compare(const KeyAndDocId& other) const {
	int c = compareJson(key,other.key);
	if (c == 0) {
		return compareStringsUnicode(docId,other.docId);
	} else {
		return c;
	}
}

Value MemView::runQuery(const QueryRequest& r) const {

	Value out;
	switch (r.mode) {
	case qmAllItems:
		out = getAllItems();
		break;
	case qmKeyList:
		out = getItemsByKeys(r.keys);
		break;
	case qmStringPrefix: {
		Value k = r.keys[0];
		Value from = k;
		Value to;
		if (k.type() == json::array) {
			Array hlp(k);
			String tail ({hlp[hlp.size()-1].toString(), Query::maxString});
			hlp.erase(hlp.size()-1);
			hlp.push_back(tail);
			to = hlp;
		}
		out = getItemsByRange(from,to,false,false);
		} break;
	case qmKeyRange:
		out = getItemsByRange(r.keys[0],r.keys[1],r.exclude_end,r.docIdFromGetKey);
		break;
	case qmKeyPrefix: {
		Array from (r.keys[0]);
		Array to (r.keys[0]);
		to.push_back(Query::maxKey);
		out = getItemsByRange(from,to,false,false);
		} break;
	}

	if (r.reversedOrder) {
		out = out.reverse();
	}

	return out;

}

template<typename Iter>
static Value resultToValue(const Iter &itm) {
	return Object("id",itm.first.docId)
					("key",itm.first.key)
					("value",itm.second.value)
					("doc",itm.second.doc);
}

Value MemView::getAllItems() const {
	Array out;
	out.reserve(keyToValueMap.size());

	for (auto &&itm : keyToValueMap) {
		out.push_back(resultToValue(itm));
	}
	return out;
}

Value MemView::getItemsByKeys(const json::Array& keys) const {
	Array out;
	for (Value k : keys) {
		IterRange<KeyToValue::const_iterator> r(
			keyToValueMap.lower_bound(KeyAndDocId(k,String())),
			keyToValueMap.upper_bound(KeyAndDocId(k,Query::maxString)));
		for (auto &&itm : r) {
			out.push_back(resultToValue(itm));
		}
	}
	return out;
}

Value MemView::getItemsByRange(const json::Value& from, const json::Value& to, bool exclude_end, bool extractDocIDs) const {

	Array out;

	String minDoc = extractDocIDs?String(from.getKey()):String();
	String maxDoc = extractDocIDs?String(to.getKey()):exclude_end?String():Query::maxString;

	IterRange<KeyToValue::const_iterator> r(
		keyToValueMap.lower_bound(KeyAndDocId(from,minDoc)),
		exclude_end?keyToValueMap.lower_bound(KeyAndDocId(to,maxDoc))
				   :keyToValueMap.upper_bound(KeyAndDocId(to,maxDoc)));


	out.reserve(std::distance(r.begin(),r.end()));
	for (auto &&itm: r) {
		out.push_back(resultToValue(itm));
	}

	return out;
}

}
