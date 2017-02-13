/*
 * queryCache.h
 *
 *  Created on: 31. 3. 2016
 *      Author: ondra
 */

#ifndef LIBS_LIGHTCOUCH_SRC_LIGHTCOUCH_QUERYCACHE_H_
#define LIBS_LIGHTCOUCH_SRC_LIGHTCOUCH_QUERYCACHE_H_

#include <unordered_map>
#include <mutex>

#include "json.h"
namespace couchit {


///Query cache stores results of various queries to the CouchDB
/**
 * Query cache can store just GET query only,when JSON is result. It cannot store
 * attachments and other results. The main benefit of this limitation is that
 * result is stored as parsed JSON variable.
 *
 * Every query is stored along with ETag, Later Etag can be used to determine, whether
 * data has been changed. If ETag matches, no download and parsing is required
 *
 * Default size of cache is 100 urls. The cache can dynamically grow and shrink depend
 * to achieve best hit ratio. If hit-ratio is above 50%, then cache can grow to until
 * the hit ratio drops under 50%. If hit ratio is below 50% the maximum cache size can shrink.
 * Cache is optimized everytime the count of items reaches calculated maximum. Items without
 * hit are removed, items with one or more hits are kept. All hits are reset and size of
 * cache is calculated as 2x count of kept items.
 *
 * If you generate a lot of unique queries, the cache can shrink a lot. Cache will never
 * shring below initial size. Unique queries reduces efectivity of the cache. You can
 * control caching by the query. It is better to disable caching for unique queries.
 *
 */
class QueryCache {
public:

	///Specifies max lru for item
	/**
	 * LRU defines when the cache item is removed and how much the cache can grow. Everytime
	 * the cache is compacted, LRU-counter for all items is decreased and the cache can grow twice.
	 * Once LRU-counter reaches zero, item is removed from the cache.
	 */
	static std::size_t maxlru;

	QueryCache():maxSize(1000),initialMaxSize(1000) {}
	QueryCache(std::size_t hint_size):maxSize(hint_size),initialMaxSize(hint_size) {}

	struct CachedItem {
		String etag;
		Value value;
		std::size_t lru;

		CachedItem() {}
		///Create cached item
		/**
		 *
		 * @param etag last known ETag
		 * @param seqNum seq. number known when value is stored
		 * @param value value to store
		 */
		CachedItem(String etag,const Value &value)
			:etag(etag),value(value),lru(maxlru) {}
		bool isDefined() const {return value.defined();}
	};

	///search for url in the cache
	CachedItem  find(StrViewA url);

	///set content to cache (override if exists)
	void set(StrViewA url, const CachedItem &item);

	///clear the cache
	void clear();

	void optimize();


	~QueryCache();


protected:

	std::size_t maxSize;
	std::size_t initialMaxSize;


	typedef std::unordered_map<std::size_t, CachedItem  > ItemMap;

	ItemMap itemMap;

	std::mutex lock;
	typedef std::lock_guard<std::mutex> Sync;
};

} /* namespace couchit */

#endif /* LIBS_LIGHTCOUCH_SRC_LIGHTCOUCH_QUERYCACHE_H_ */
