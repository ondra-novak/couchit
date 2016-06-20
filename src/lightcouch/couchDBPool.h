/*
 * couchDBPool.h
 *
 *  Created on: 27. 4. 2016
 *      Author: ondra
 */

#ifndef LIBS_LIGHTCOUCH_SRC_LIGHTCOUCH_COUCHDBPOOL_H_
#define LIBS_LIGHTCOUCH_SRC_LIGHTCOUCH_COUCHDBPOOL_H_
#include <lightspeed/base/containers/resourcePool.h>

#include "couchDB.h"
namespace LightCouch {

using namespace LightSpeed;

///CouchDB instance managed by the resource pool
/** Resource pool is responsible to create and release resources allocated for every thread
 * on the server. This class is just CouchDB put together with AbstractResource. It is
 * constructed by CouchDBPool
 */
class CouchDBManaged: public CouchDB, public AbstractResource {
	public:
		CouchDBManaged(const Config &cfg):CouchDB(cfg) {}
};


///Pointer which carries managed couchDB instance.
/** You have to create the instance passing a reference to the CouchDBPool into its constructor.
 * Once the resource is no longer needed, it is released.
 *
 * You can copy resoruce reference, but it will still refer the same instance. Pointer
 * track count of its copies. However, you have to avoid to share pointer between threads.
 * If you need this, use proper synchronization
 */
typedef ResourcePtr<CouchDBManaged> MCouchDB;

///Pool of CouchDB connections.
/**
 * To receive new connection, use CouchDBPool to construct MCouchDB reference. It holds
 * reference to CouchDB object
 */
class CouchDBPool: public AbstractResourcePool {
public:
	///Construct the pool
	/**
	 * @param cfg configuration of the pool
	 * @param limit maximum count of concurrent connections
	 * @param resTimeout how long resource can stay idle before it is released (in ms)
	 * @param waitTimeout how long thread can (in ms) wait before TimeoutException is returned
	 */
	CouchDBPool(const Config &cfg, natural limit, natural resTimeout, natural waitTimeout);

protected:
	virtual CouchDBManaged *createResource();
	virtual const char *getResourceName() const;

	Config cfg;
};


} /* namespace LightCouch */

#endif /* LIBS_LIGHTCOUCH_SRC_LIGHTCOUCH_COUCHDBPOOL_H_ */
