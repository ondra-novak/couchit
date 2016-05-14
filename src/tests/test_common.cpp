/*
 * common.cpp
 *
 *  Created on: 10. 3. 2016
 *      Author: ondra
 */

#include "test_common.h"

#include "lightspeed/base/streams/netio.h"
namespace LightCouch {
using namespace LightSpeed;

CouchDB::Config getTestCouch() {
	NetworkStreamSource src(NetworkAddress("localhost",5984),naturalNull, 10000,10000);
	CouchDB::Config cfg;
	cfg.baseUrl = "http://localhost:5984/";
	return cfg;

}



}

