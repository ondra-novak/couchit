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
		atomicValue seqNum;
		const Value value;

		CachedItem() {}
		///Create cached item
		/**
		 *
		 * @param etag last known ETag
		 * @param seqNum seq. number known when value is stored
		 * @param value value to store
		 */
		CachedItem(StringA etag, natural seqNum, const Value &value)
			:etag(etag),seqNum(seqNum), value(value) {}
		bool isDefined() const {return value != null;}
	};

	///search for url in the cache
	CachedItem  find(ConstStrA url);

	///set content to cache (override if exists)
	void set(ConstStrA url, const CachedItem &item);

	///clear the cache
	void clear();


	///Starts tracking sequence numbers
	/** Creates a record for sequence numbers for specified database.
	 *
	 * @param databaseName name of database
	 * @return reference to created record. You can use the reference to store and retrieve sequence numbers.
	 */
	atomicValue &trackSeqNumbers(StringRef databaseName);


	~QueryCache();


protected:

	typedef StringKey<StringA> StrKey;


	typedef Map<StrKey, CachedItem  > ItemMap;
	typedef Map<StrKey, atomicValue> SeqMap;

	ItemMap itemMap;
	SeqMap seqMap;

	FastLock lock;
};

} /* namespace LightCouch */

#endif /* LIBS_LIGHTCOUCH_SRC_LIGHTCOUCH_QUERYCACHE_H_ */
