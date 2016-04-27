/*
 * couchDBPool.cpp
 *
 *  Created on: 27. 4. 2016
 *      Author: ondra
 */

#include "couchDBPool.h"

namespace LightCouch {

CouchDBPool::CouchDBPool(
		const CouchDB::Config &cfg,
		natural limit,
		natural resTimeout,
		natural waitTimeout)
:AbstractResourcePool(limit,resTimeout,waitTimeout),cfg(cfg) {}

CouchDBManaged* CouchDBPool::createResource() {
	return new CouchDBManaged(cfg);
}

const char* CouchDBPool::getResourceName() const {
	return "CouchDB instance";
}

} /* namespace LightCouch */
