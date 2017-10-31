/*
 * test_uuids.cpp
 *
 *  Created on: 10. 3. 2016
 *      Author: ondra
 */

#include <iostream>
#include <set>
#include <chrono>
#include <thread>

#include "../couchit/couchDB.h"

#include "test_common.h"
#include "testClass.h"

namespace couchit {

using namespace json;



static void genFastUUIDS(std::ostream &print) {

	std::set<String> uuidmap;
	CouchDB db(getTestCouch());

	for (std::size_t i = 0; i < 50; i++) {
		String uuid ( db.genUID("test-"));
		std::cout << uuid << std::endl;
		uuidmap.insert(uuid);
		std::this_thread::sleep_for(std::chrono::milliseconds(100));
	}
	print << uuidmap.size();
}


void testUUIDs(TestSimple &tst) {
	tst.test("couchdb.genfastuid","50") >> &genFastUUIDS;
}
}
