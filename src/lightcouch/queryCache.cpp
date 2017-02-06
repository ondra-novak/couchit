/*
 * queryCache.cpp
 *
 *  Created on: 31. 3. 2016
 *      Author: ondra
 */

#include "queryCache.h"

#include "fnv.h"

namespace LightCouch {


std::size_t QueryCache::maxlru = 4;

static std::uintptr_t hashUrl(StrViewA url) {
	typedef FNV1a<sizeof(std::uintptr_t)> HashFn;
	std::size_t sz = url.length,pos = 0;
	return HashFn::hash([&]() -> int {
		if (pos < sz) return (unsigned char)url[pos++];
		else return -1;
	});

}


QueryCache::CachedItem QueryCache::find(StrViewA url) {

	Sync _(lock);

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

void QueryCache::set(StrViewA url, const CachedItem& item) {
	Sync _(lock);

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

