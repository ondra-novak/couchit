/*
 * queryCache.cpp
 *
 *  Created on: 31. 3. 2016
 *      Author: ondra
 */

#include "queryCache.h"

#include "fnv.h"

namespace couchit {


std::size_t QueryCache::maxlru = 4;

static std::uintptr_t hashUrl(std::string_view url) {
	typedef FNV1a<sizeof(std::uintptr_t)> HashFn;
	std::size_t sz = url.length(),pos = 0;
	return HashFn::hash([&]() -> int {
		if (pos < sz) return (unsigned char)url[pos++];
		else return -1;
	});

}


QueryCache::CachedItem QueryCache::find(std::string_view url) {

	Sync _(lock);

	auto f = itemMap.find(url);
	if (f == itemMap.end()) return CachedItem();
	else {
		f->second.lru = maxlru;
		return f->second;
	}

}

void QueryCache::clear() {
	itemMap.clear();
}

void QueryCache::set(const CachedItem& item) {
	Sync _(lock);

	if (itemMap.size() >= maxSize) {
		optimize();
	}

	itemMap.erase(item.url);
	itemMap.insert(std::make_pair(std::string_view(item.url), item));

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



std::size_t QueryCache::CalcHash::operator()(const std::string_view str) const
{
	return hashUrl(str);
}

} /* namespace couchit */

