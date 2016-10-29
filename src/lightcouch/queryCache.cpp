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
#include "fnv.h"

namespace LightCouch {


natural QueryCache::maxlru = 4;

static std::uintptr_t hashUrl(ConstStrA url) {
	typedef FNV1a<sizeof(std::uintptr_t)> HashFn;
	ConstStrA::Iterator iter = url.getFwIter();
	return HashFn::hash([&]() -> int {
		if (iter.hasItems()) return (byte)iter.getNext();
		else return -1;
	});

}


QueryCache::CachedItem QueryCache::find(ConstStrA url) {

	Synchronized<FastLock> _(lock);

	auto f = itemMap.find(hashUrl(url));
	if (f == itemMap.end()) return CachedItem();
	else {
		f->second.lru = maxlru;
		return f->second;
	}

}

void QueryCache::clear() {
	itemMap.clear();
}

void QueryCache::set(ConstStrA url, const CachedItem& item) {
	Synchronized<FastLock> _(lock);

	if (itemMap.size() >= maxSize) {
		optimize();
	}

	std::uintptr_t hash = hashUrl(url);
	itemMap.erase(hash);
	itemMap.insert(std::make_pair(hash, item));

}

void QueryCache::optimize() {
	decltype(itemMap) newMap;
	for(auto &&item : itemMap) {
		item.second.lru--;
		if (item.second.lru) {
			newMap.insert(item);
		}
	}
	maxSize = newMap.size()*maxlru/(maxlru-1);
	if (maxSize < initialMaxSize) maxSize = initialMaxSize;
	newMap.swap(itemMap);
}

QueryCache::~QueryCache() {
	clear();
}



} /* namespace LightCouch */

