/*
 * colation.h
 *
 *  Created on: 25. 6. 2016
 *      Author: ondra
 */

#ifndef SRC_LIGHTCOUCH_COLLATION_H_1278AEOLBBD456800
#define SRC_LIGHTCOUCH_COLLATION_H_1278AEOLBBD456800


#include <lightspeed/utils/json.h>
#include "object.h"

namespace LightCouch {

using namespace LightSpeed;

CompareResult compareStringsUnicode(ConstStrA str1, ConstStrA str2);
CompareResult compareJson(const ConstValue &left, const ConstValue &right);

struct JsonIsLess {
	bool operator()(const ConstValue &v1, const ConstValue &v2) const;
};

struct JsonIsGreater {
	bool operator()(const ConstValue &v1, const ConstValue &v2) const;
};

}



#endif /* SRC_LIGHTCOUCH_COLLATION_H_1278AEOLBBD456800 */
