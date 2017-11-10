/*
 * cachedLookup.cpp
 *
 *  Created on: 10. 11. 2017
 *      Author: ondra
 */

#include "cachedLookup.h"

#include "couchDB.h"

#include "changes.h"

namespace couchit {

couchit::CachedLookup::CachedLookup(CouchDB& db, const View& view):query(db.createQuery(view)) {

}

couchit::CachedLookup::CachedLookup(const Query& query):query(query) {
}

couchit::CachedLookup::CachedLookup(CouchDB& db, unsigned int flags ):query(db.createQuery(flags)) {
}

Result couchit::CachedLookup::lookup(const json::Value keys) {

	Array rows;
	Array keysToAsk;
	auto iend = keyToRes.end();

	Sync _(lock);
	for (Value v : keys) {
		auto iter = keyToRes.find(v);
		if (iter == iend) keysToAsk.push_back(v);
		else {
			rows.addSet(iter->second);
		}
	}

	if (!keysToAsk.empty()) {
		query.keys(keysToAsk);
		Result res = query.exec();
		Array vals;
		Value curKey;
		for (Row v : res) {
			if (v.key != curKey) {
				if (!vals.empty()) {
					mockLk(curKey, vals);
					vals.clear();
				}
				curKey = v.key;
			}
			vals.push_back(v);
		}
		if (!vals.empty()) {
			mockLk(curKey,vals);
		}

		for (Value v : keysToAsk) {
			auto iter = keyToRes.find(v);
			if (iter == iend) {
				keyToRes.insert(std::make_pair(v, Value()));
			} else {
				rows.addSet(iter->second);
			}
		}
		resHdr = res.replace("rows",Value());
	}
	return resHdr.replace("rows",rows);
}

void couchit::CachedLookup::invalidate() {
	Sync _(lock);
	keyToRes.clear();
	docToKey.clear();
}

void couchit::CachedLookup::invalidate(const json::Value& id) {
	Sync _(lock);
	auto rng = docToKey.equal_range(id);
	for(decltype(rng.first) it = rng.first; it != rng.second; ++it)
		keyToRes.erase(it->second);
	docToKey.erase(id);
}

void couchit::CachedLookup::mock(json::Value key, json::Value docs) {
	Sync _(lock);
	mockLk(key,docs);
}
void CachedLookup::mockLk(json::Value key, json::Value docs) {

	keyToRes.insert(std::make_pair(Value(key),Value(docs)));
	for (Value v: docs) {
		Value id = v["id"];
		if (id.type() != json::string) {
			for (Value z: id) {
				docToKey.insert(std::make_pair(Value(z),v["key"]));
			}
		} else if (id.defined()) {
			docToKey.insert(std::make_pair(Value(id),v["key"]));
		}
	}

}

void couchit::CachedLookup::onChange(const ChangedDoc& doc) {
	invalidate(doc.operator []("id"));
}



} /* namespace couchit */

