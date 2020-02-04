/*
 * docmap.h
 *
 *  Created on: 5. 9. 2019
 *      Author: ondra
 */

#ifndef SRC_COUCHIT_SRC_COUCHIT_DOCMAP_H_
#define SRC_COUCHIT_SRC_COUCHIT_DOCMAP_H_
#include "shared/refcnt.h"
#include <mutex>
#include <functional>
#include <imtjson/string.h>
#include <imtjson/value.h>
#include <unordered_map>



namespace couchit {

///Map with cache
/** Maps document to C++ type and stores result to cache
 *
 * It acts as function with operator(), however it has cache. When document is mapped
 * by the first time, it calls the map function and stores result to the internal cache. If
 * the operator is called again for the same document, it is retrieved from the cache. This
 * can speedup operation
 *
 * The result is stored to refcounted immutable object
 */
template<typename T>
class DocMap {
public:
	using MapFn = std::function<T(Value)>;

	class Item: public ondra_shared::RefCntObj {
	public:
		const json::String rev;
		const T data;
		std::chrono::steady_clock::time_point store_time;

		Item(json::String &&rev, T &&data, std::chrono::steady_clock::time_point &&store_time)
			:rev(std::move(rev))
			,data(std::move(data))
			,store_time(std::move(store_time)){}
	};

	using Ref = ondra_shared::RefCntPtr<Item>;

	DocMap(MapFn &&fn):mapfn(std::move(fn)) {}

	///Perform document tranform with caching
	/**
	 * @param doc document to transform
	 * @param ttl optional time to live in seconds. Default value stores document for infinite time. If ttl is specified, transformed document
	 * is discarded when ttl expires. To rerun transformation, use 0, which invalidates the cache immediately.
	 *
	 */
	Ref operator()(json::Value doc, int ttl = -1) const {
		Sync _(lock);
		auto id = doc["_id"].toString();
		auto rev = doc["_rev"].toString();
		auto iter = map.find(id);
		std::chrono::steady_clock::time_point now = std::chrono::steady_clock::now();
		bool hasExp = ttl >= 0;
		if (iter == map.end() || iter->second->rev != rev || (hasExp && iter->second->store_time+std::chrono::seconds(ttl) <= now)) {
			Ref newValue(new Item(std::move(rev), mapfn(doc), std::chrono::steady_clock::now()));
			auto r = map.insert(std::make_pair(id, newValue));
			if (!r.second) r.first->second = newValue;
			return newValue;
		} else {
			return iter->second;
		}
	}



protected:
	struct StrHash {
		auto operator()(const String &a) const {
			return std::hash<Value>()(Value(a));
		}
	};
	using Sync = std::unique_lock<std::recursive_mutex>;
	using Map = std::unordered_map<json::String, Ref, StrHash>;

	mutable std::recursive_mutex lock;
	mutable Map map;
	MapFn mapfn;


};


}


#endif /* SRC_COUCHIT_SRC_COUCHIT_DOCMAP_H_ */
