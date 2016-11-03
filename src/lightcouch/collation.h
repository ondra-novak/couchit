/*
 * colation.h
 *
 *  Created on: 25. 6. 2016
 *      Author: ondra
 */

#ifndef SRC_LIGHTCOUCH_COLLATION_H_1278AEOLBBD456800
#define SRC_LIGHTCOUCH_COLLATION_H_1278AEOLBBD456800
#include <lightspeed/base/containers/constStr.h>
#include "lightspeed/base/compare.h"
#include "json.h"




namespace LightCouch {

using namespace LightSpeed;

CompareResult compareStringsUnicode(StrView str1, StrView str2);
CompareResult compareJson(const Value &left, const Value &right);

struct JsonIsLess {
	bool operator()(const Value &v1, const Value &v2) const;
};

struct JsonIsGreater {
	bool operator()(const Value &v1, const Value &v2) const;
};

}



#endif /* SRC_LIGHTCOUCH_COLLATION_H_1278AEOLBBD456800 */
