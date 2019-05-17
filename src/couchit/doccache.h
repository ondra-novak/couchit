/*
 * objcache.h
 *
 *  Created on: 10. 5. 2019
 *      Author: ondra
 */

#ifndef SRC_COUCHIT_SRC_COUCHIT_DOCCACHE_H_
#define SRC_COUCHIT_SRC_COUCHIT_DOCCACHE_H_
#include <unordered_map>

#include "changeObserver.h"
#include "changes.h"
#include "couchDB.h"

namespace couchit {

///Implements document cache
/** Every read of the document causes request to the database,
serializing and parsing data. Many accesses to the documents
costs a power and affects performance.

The DocCache object implements a document cache. When a document
is accessed, it is stored in the cache ready for reuse. The object
also tracks changes and updates the cache the document is changed


@note There is a small window between storing document and updating
the cache, so be careful and don't use cache if you need to
read currently stored version of the document

*/

class DocCache {
public:


	struct Config {
		///limit of the cache in the items
		/** If zero is given there is no limit. Otherwise the cache
		 * performs garbage collection. Note that garbage collection
		 * runs excludive blocking other threads during
		 */
		std::size_t limit = 0;
		///Retrieve documents with revision log (CouchDB 2+)
		bool revisions = false;
		///Retrieve documents with conflicts fields (ignored when revisions is active)
		bool conflicts = false;
		///also stores results for missing documents
		bool missing = false;
	};

	///Inicialize the cache
	/**
	 * @param db couchdb database instance. The object must not be
	 * destroyed before this object.
	 *
	 * @param distributor Pointer to the changes distributor. Pointer
	 * can be null to disable updates. The object registers self to
	 * the changes distributor. The changes distributor should
	 * deliver changes with documents. In case, that document is
	 * missing, the document is removed from the cache and is
	 * read back on first access (cache miss)
	 *
	 * @param config cache configuration
	 */
	DocCache(CouchDB &db, ChangesDistributor *distributor, Config config);
	virtual ~DocCache();

	///Retrieves document from the cache or directly asking the databse
	/** Function retrieves document or null if document is not found
	 *
	 *
	 * */
	Value get(StrViewA name);

	///Puts document to the cache.
	/**
	 * @param doc document to put
	 * @retval true success
	 */
	void put(Value doc);

	///Puts document in missing state
	void put_missing(String id);

	///Erases document from the cache
	void erase(String id);

	///Receieves associated database object
	CouchDB &getDB() {return db;}

protected:
	std::recursive_mutex lock;
	using Sync = std::unique_lock<std::recursive_mutex>;

	struct Item {
		Value data;
		mutable bool accessed = false;
		char lru = 0;
	};

	struct Hash {
		std::size_t operator()(StrViewA data) const;
	};

	using DataMap = std::unordered_map<String, Item, Hash>;

	CouchDB &db;
	ChangesDistributor *chdist;
	Config config;
	DataMap dataMap;
	ChangesDistributor::RegistrationID regid;

	void unreg();

	void rungc();

	void update(const ChangeEvent &ev);


	class Update;


};




}




#endif /* SRC_COUCHIT_SRC_COUCHIT_DOCCACHE_H_ */
