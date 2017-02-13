/*
 * common.cpp
 *
 *  Created on: 10. 3. 2016
 *      Author: ondra
 */

#include "test_common.h"

namespace couchit {

Config getTestCouch() {
	Config cfg;
	cfg.baseUrl = "http://localhost:5984/";
	return cfg;

}



}

