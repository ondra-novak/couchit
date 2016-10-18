/*
 * queryCache.cpp
 *
 *  Created on: 31. 3. 2016
 *      Author: ondra
 */

#include "queryCache.h"

#include "lightspeed/base/sync/synchronize.h"
#include "lightspeed/base/actions/promise.tcc"
#include "lightspeed/base/containers/map.tcc"

namespace LightCouch {


QueryCache::CachedItem QueryCache::find(ConstStrA url) {

	Synchronized<FastLock> _(lock);

	const CachedItem *itm = itemMap.find(StrKey(url));
	if (itm) {
		return *itm;
	} else {
		return CachedItem();
	}

}

void QueryCache::clear() {
	itemMap.clear();
}

void QueryCache::set(ConstStrA url, const CachedItem& item) {
	Synchronized<FastLock> _(lock);
	StrKey k((StringA(url)));
	itemMap.erase(k);
	itemMap.insert(k, item);
}

QueryCache::~QueryCache() {
	clear();
}

atomicValue& QueryCache::trackSeqNumbers(StringRef databaseName) {
	Synchronized<FastLock> _(lock);
	atomicValue *p = seqMap.find(StrKey(databaseName));
	if (p) {
		return *p;
	} else {
		seqMap.insert(StrKey((StringA(databaseName))),0);
		return *seqMap.find(StrKey(databaseName));
	}
}


} /* namespace LightCouch */

