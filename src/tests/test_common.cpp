/*
 * common.cpp
 *
 *  Created on: 10. 3. 2016
 *      Author: ondra
 */

#include <fstream>
#include "../couchit/json.h"
#include "../couchit/num2str.h"
#include "test_common.h"


namespace couchit {

Config loadConfig() {


	Config cfg;
	cfg.baseUrl = "http://localhost:5984/";

	std::ifstream fcfg(".couchit_test", std::ios::in);
	if (!(!fcfg)) {
		json::Value jcfg = json::Value::fromStream(fcfg);
		json::Value x = jcfg["baseUrl"];
		if (x.defined()) cfg.baseUrl = x.getString();
		x = jcfg["databaseName"];
		if (x.defined()) cfg.databaseName = x.getString();
		x = jcfg["username"];
		if (x.defined()) cfg.authInfo.username= x.getString();
		x = jcfg["password"];
		if (x.defined()) cfg.authInfo.password= x.getString();
	}
	return cfg;




}

Config getTestCouch() {
	static Config cfg = loadConfig();
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

