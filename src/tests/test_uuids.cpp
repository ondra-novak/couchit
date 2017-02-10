/*
 * test_uuids.cpp
 *
 *  Created on: 10. 3. 2016
 *      Author: ondra
 */

#include <unistd.h>
#include <iostream>
#include <set>

#include "../lightcouch/couchDB.h"

#include "test_common.h"
#include "testClass.h"

namespace LightCouch {

using namespace json;



static void genFastUUIDS(std::ostream &print) {

	std::set<String> uuidmap;
	CouchDB db(getTestCouch());

	for (std::size_t i = 0; i < 50; i++) {
		String uuid = db.genUID("test-");
		std::cout << uuid << std::endl;
		uuidmap.insert(uuid);
		usleep(100000);
	}
	print << uuidmap.size();
}


void testUUIDs(TestSimple &tst) {
	tst.test("couchdb.genfastuid","50") >> &genFastUUIDS;
}
}
