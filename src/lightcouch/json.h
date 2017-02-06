/*
 * json.h
 *
 *  Created on: 14. 10. 2016
 *      Author: ondra
 */

#ifndef LIGHTCOUCH_JSON_H_
#define LIGHTCOUCH_JSON_H_

#include <imtjson/json.h>
#include <imtjson/stringview.h>

namespace LightCouch {

using namespace json;


static inline Value addToArray(Value v, Value add) {
	Array c(v);
	if (c.empty()) c.add(v);
	c.add(add);
	return c;
}

static inline Value addSuffix(Value v,const String &suffix) {
		if (!v.empty()) {
			Array x(v);
			std::size_t sz = v.size()-1;
			x.trunc(sz);
			x.add(v[sz].toString()+suffix);
			return x;
		} else {
			return v.toString() + suffix;
		}
	}


}



#endif /* LIGHTCOUCH_JSON_H_ */

