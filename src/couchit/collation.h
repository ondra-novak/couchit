/*
 * colation.h
 *
 *  Created on: 25. 6. 2016
 *      Author: ondra
 */

#ifndef SRC_LIGHTCOUCH_COLLATION_H_1278AEOLBBD456800
#define SRC_LIGHTCOUCH_COLLATION_H_1278AEOLBBD456800
#include "json.h"




namespace couchit {

typedef int CompareResult;
static const int cmpResultLess = -1;
static const int cmpResultEqual = 0;
static const int cmpResultGreater = 1;

CompareResult compareStringsUnicode(StrViewA str1, StrViewA str2);
CompareResult compareJson(const Value &left, const Value &right);

struct JsonIsLess {
	bool operator()(const Value &v1, const Value &v2) const;
};

struct JsonIsGreater {
	bool operator()(const Value &v1, const Value &v2) const;
};

}



#endif /* SRC_LIGHTCOUCH_COLLATION_H_1278AEOLBBD456800 */
