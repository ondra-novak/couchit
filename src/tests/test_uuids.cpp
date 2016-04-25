/*
 * test_uuids.cpp
 *
 *  Created on: 10. 3. 2016
 *      Author: ondra
 */

#include "../lightcouch/uid.h"
#include "lightspeed/base/framework/testapp.h"
#include "lightspeed/base/text/textstream.tcc"
#include "../lightcouch/couchDB.h"

#include "test_common.h"

#include "lightspeed/base/containers/set.tcc"
namespace LightCouch {
using namespace LightSpeed;
using namespace BredyHttpClient;


static void genUUIDS(PrintTextA &print) {

	CouchDB db(getTestCouch());
	Set<ConstStrA> uuidmap;

	UIDIterator iter =  db.genUID(50);
	while (iter.hasItems()) {
		ConstStrA docId = iter.getNext();
		uuidmap.insert(docId);
	}
	print("%1") << uuidmap.size();
}

static void genFastUUIDS(PrintTextA &print) {

	Set<StringA> uuidmap;

	for (natural i = 0; i < 50; i++) {
		StringA uuid = CouchDB::genUIDFast();
//		print("%1\n") << uuid;
		uuidmap.insert(uuid);
	}
	print("%1") << uuidmap.size();
}



defineTest test_genuuids("couchdb.genuuids","50",&genUUIDS);
defineTest test_genfastuuids("couchdb.genfastuid","50",&genFastUUIDS);
}
