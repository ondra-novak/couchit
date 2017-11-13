#pragma once

#include <mutex>
#include <unordered_map>
#include "changeObserver.h"

#include "query.h"


namespace couchit {

class Result;
/// Simple class which is connected with a lookup-view. It can speedup lookups because it caches results
/** The object should be also connected to _changes listener to invalidate old entries */

class CachedLookup: public IChangeObserver {
public:

	///Create lookup on database and view
	CachedLookup(CouchDB &db, const View &view, bool forceUpdate = true);

	///Create lookup from prepared query
	explicit CachedLookup(const Query &query);

	///Create lookup on "_all_docs"
	explicit CachedLookup(CouchDB &db, unsigned int flags = View::includeDocs);


	///perform lookup
	/**
	 * @param keys list of keys
	 * @return list of results
	 *
	 * @note Result header can be old, especially when results are served from the cache complete
	 */
	Result lookup (const json::Value keys);
	///clear whole cache
	void invalidate();
	///clear sigle document from the cache
	void invalidate(const json::Value &id);


	///Mocks whole key
	/**
	 * @param key key to lookup
	 * @param docs array of results. The result must be in format returned by the query.
	 *   {id, key, value, doc}. If the docs is undefined, then result will be considered as not-exists
	 *
	 *
	 */
	void mock(json::Value key, json::Value docs);


protected:



	///invalidates updated documents
	virtual void onChange(const ChangedDoc &doc);

	void mockLk(json::Value key, json::Value docs);

	typedef std::unordered_map<Value, Value> ValValMap;
	typedef std::unordered_multimap<Value, Value> ValMultiValMap;

	Query query;
	ValValMap keyToRes;
	ValMultiValMap docToKey;
	json::Value resHdr;
	std::mutex lock;
	typedef std::lock_guard<std::mutex> Sync;


};

} /* namespace couchit */


