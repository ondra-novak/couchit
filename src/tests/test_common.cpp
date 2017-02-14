/*
 * common.cpp
 *
 *  Created on: 10. 3. 2016
 *      Author: ondra
 */

#include "../couchit/json.h"
#include "../couchit/num2str.h"
#include "test_common.h"

namespace couchit {

Config getTestCouch() {
	Config cfg;
	cfg.baseUrl = "http://localhost:5984/";
	return cfg;

}

json::String UIntToStr(std::size_t id, int base) {
	return String(21, [&](char *c) {
		return unsignedToStringImpl([&](char z) {
			*c++ = z;
		},id,8,true,base);
	});
}



}

