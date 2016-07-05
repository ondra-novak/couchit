/*
 * collation.cpp
 *
 *  Created on: 25. 6. 2016
 *      Author: ondra
 */



#include "collation.h"
#include <lightspeed/base/streams/utf.h>


namespace LightCouch {


 CompareResult compareStringsUnicode(ConstStrA str1, ConstStrA str2) {
	Utf8ToWideReader<ConstStrA::Iterator> iter1(str1.getFwIter()), iter2(str2.getFwIter());
	iter1.enableSkipInvalidChars(true);
	iter2.enableSkipInvalidChars(true);

	while (iter1.hasItems() && iter2.hasItems()) {
		wchar_t c1 = iter1.getNext();
		wchar_t c2 = iter2.getNext();
		if (c1 < c2) return cmpResultLess;
		else if (c1 > c2) return cmpResultGreater;
	}
	if (iter1.hasItems())
		return cmpResultGreater;
	else if (iter2.hasItems())
		return cmpResultLess;
	else
		return cmpResultEqual;
}


CompareResult compareJson(const ConstValue &left, const ConstValue &right) {
	if (left->getType() != right->getType()) {
		if (left->isNull()) return cmpResultLess;
		if (left->isObject()) return cmpResultGreater;
		if (left->isBool()) return right->isNull()?cmpResultGreater:cmpResultLess;
		if (left->isNumber()) return right->isNull() || right->isBool()?cmpResultGreater:cmpResultLess;
		if (left->isString()) return right->isArray() || right->isObject()?cmpResultLess:cmpResultGreater;
		if (left->isArray()) return right->isObject()?cmpResultLess:cmpResultGreater;
		return cmpResultEqual;
	} else {
		switch (left->getType()) {
		case JSON::ndNull:return cmpResultEqual;
		case JSON::ndBool: return left->getBool() == right->getBool()?cmpResultEqual:(left->getBool() == false?cmpResultLess:cmpResultGreater);
		case JSON::ndFloat:
		case JSON::ndInt: {
			double l = left->getFloat();
			double r = right->getFloat();
			if (l<r) return cmpResultLess;
			else if (l>r) return cmpResultGreater;
			else return cmpResultEqual;
		}
		case JSON::ndString: return compareStringsUnicode(left->getStringUtf8(),right->getStringUtf8());
		case JSON::ndArray: {
				JSON::ConstIterator li = left->getFwConstIter();
				JSON::ConstIterator ri = right->getFwConstIter();
				while (li.hasItems() && ri.hasItems()) {
					CompareResult r = compareJson(li.getNext(),ri.getNext());
					if (r != cmpResultEqual) return r;
				}
				if (li.hasItems()) return cmpResultGreater;
				if (ri.hasItems()) return cmpResultLess;
				return cmpResultEqual;
			}
		case JSON::ndObject: {
				JSON::ConstIterator li = left->getFwConstIter();
				JSON::ConstIterator ri = right->getFwConstIter();
				while (li.hasItems() && ri.hasItems()) {
					const JSON::ConstKeyValue &kvl = li.getNext();
					const JSON::ConstKeyValue &kvr = ri.getNext();

					CompareResult r = kvl.getStringKey().compare(kvr.getStringKey());
					if (r == cmpResultEqual) {
						r = compareJson(kvl,kvr);
						if (r == cmpResultEqual)
							continue;
					}
					return r;
				}
				if (li.hasItems()) return cmpResultGreater;
				if (ri.hasItems()) return cmpResultLess;
				return cmpResultEqual;
			}
		default:
			return cmpResultEqual;
		}
	}
}

bool JsonIsLess::operator ()(const ConstValue& v1,const ConstValue& v2) const {
	return compareJson(v1,v2) == cmpResultLess;
}

bool JsonIsGreater::operator ()(const ConstValue& v1,const ConstValue& v2) const {
	return compareJson(v1,v2) == cmpResultGreater;
}



}

