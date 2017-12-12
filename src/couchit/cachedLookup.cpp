/*
 * cachedLookup.cpp
 *
 *  Created on: 10. 11. 2017
 *      Author: ondra
 */

#include "cachedLookup.h"

#include "couchDB.h"

#include "changes.h"
#include <imtjson/binjson.tcc>

namespace couchit {

couchit::CachedLookup::CachedLookup(CouchDB& db, const View& view, bool forceUpdate):query(db.createQuery(view)) {
	if (forceUpdate) query.update();

}

couchit::CachedLookup::CachedLookup(const Query& query):query(query) {
}

couchit::CachedLookup::CachedLookup(CouchDB& db, unsigned int flags ):query(db.createQuery(flags)) {
}

static void binser(std::ostream &s, json::Value v) {
	v.serializeBinary([&](unsigned int x) {s << x << " ";});
}

Result couchit::CachedLookup::lookup(const json::Value keys) {

	Array rows;
	Array keysToAsk;
	std::vector<std::size_t> offsets;
	auto iend = keyToRes.end();
	bool hasMissing = false;

	Sync _(lock);
	for (Value v : keys) {
		auto iter = keyToRes.find(v);
		if (iter == iend) {
/*			if (!hasMissing) {
				auto iter2 = unknownKeys.find(v);
				if (iter2*/
			keysToAsk.push_back(v);
			offsets.push_back(rows.size());
		} else {
			rows.addSet(iter->second);
		}
	}

	if (!keysToAsk.empty()) {
		query.keys(keysToAsk);
		Result res = query.exec();
		Array vals;
		Value curKey;
		for (Row v : res) {
			Value k = v.key.stripKey();
			if (k != curKey) {
				if (!vals.empty()) {
					mockLk(curKey, vals);
					vals.clear();
				}
				curKey = k;
			}
			vals.push_back(v);
		}
		if (!vals.empty()) {
//			std::cerr << curKey.toString() << " "; binser(std::cerr, curKey); std::cerr << std::endl;
			mockLk(curKey,vals);
		}

		Array newRows;
		std::size_t pos = 0;
		for (std::size_t i = 0, cnt = rows.size(), kcnt = keysToAsk.size();i< cnt || pos < kcnt;) {
			if (pos<kcnt && offsets[pos] == i) {
				Value v = keysToAsk[pos];
				auto iter = keyToRes.find(v);
				if (iter == iend) {
					keyToRes.insert(std::make_pair(v, Value()));
				} else {
					newRows.addSet(iter->second);
				}
				pos++;
			} else if (i < cnt) {
				newRows.push_back(rows[i]);
					i++;
			}
		}
		std::swap(rows,newRows);

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

