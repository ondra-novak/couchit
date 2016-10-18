/*
 * queryCache.h
 *
 *  Created on: 31. 3. 2016
 *      Author: ondra
 */

#ifndef LIBS_LIGHTCOUCH_SRC_LIGHTCOUCH_QUERYCACHE_H_
#define LIBS_LIGHTCOUCH_SRC_LIGHTCOUCH_QUERYCACHE_H_

#include <lightspeed/base/types.h>
#include "lightspeed/base/actions/promise.h"
#include "lightspeed/base/containers/constStr.h"
#include "lightspeed/base/containers/stringKey.h"
#include "lightspeed/base/containers/map.h"

#include "json.h"
namespace LightCouch {

using namespace LightSpeed;

///Query cache stores results of various queries to the CouchDB
/**
 * Query cache can store just GET query only,when JSON is result. It cannot store
 * attachments and other results. The main benefit of this limitation is that
 * result is stored as parsed JSON variable. On other hand, it requires, that nobody
 * will change data stored in the cache.
 *
 * Every query is stored along with ETag, Later Etag can be used to determine, whether
 * data has been changed. If ETag matches, no download and parsing is performed
 */
class QueryCache {
public:

	struct CachedItem {
		const StringA etag;
		const Value value;

		CachedItem() {}
		///Create cached item
		/**
		 *
		 * @param etag last known ETag
		 * @param seqNum seq. number known when value is stored
		 * @param value value to store
		 */
		CachedItem(StringA etag,const Value &value)
			:etag(etag),value(value) {}
		bool isDefined() const {return value != null;}
	};

	///search for url in the cache
	CachedItem  find(ConstStrA url);

	///set content to cache (override if exists)
	void set(ConstStrA url, const CachedItem &item);

	///clear the cache
	void clear();




	~QueryCache();


protected:

	typedef StringKey<StringA> StrKey;


	typedef Map<StrKey, CachedItem  > ItemMap;

	ItemMap itemMap;

	FastLock lock;
};

} /* namespace LightCouch */

#endif /* LIBS_LIGHTCOUCH_SRC_LIGHTCOUCH_QUERYCACHE_H_ */
