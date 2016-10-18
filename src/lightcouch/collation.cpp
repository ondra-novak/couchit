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


CompareResult compareJson(const Value &left, const Value &right) {
	if (left.type() != right.type()) {
		if (left.type()==json::null) return cmpResultLess;
		if (left.type()==json::object) return cmpResultGreater;
		if (left.type()==json::boolean) return right.type()==json::null?cmpResultGreater:cmpResultLess;
		if (left.type()==json::number) return right.type()==json::null || right.type()==json::boolean?cmpResultGreater:cmpResultLess;
		if (left.type()==json::string) return right.type()==json::array || right.type()==json::object?cmpResultLess:cmpResultGreater;
		if (left.type()==json::array) return right.type()==json::object?cmpResultLess:cmpResultGreater;
		return cmpResultEqual;
	} else {
		switch (left.type()) {
		case json::null:return cmpResultEqual;
		case json::boolean: return left.getBool() == right.getBool()?cmpResultEqual:(left.getBool() == false?cmpResultLess:cmpResultGreater);
		case json::number: {
			double l = left.getNumber();
			double r = right.getNumber();
			if (l<r) return cmpResultLess;
			else if (l>r) return cmpResultGreater;
			else return cmpResultEqual;
		}
		case json::string: return compareStringsUnicode(StringRef(left.getString()),StringRef(right.getString()));
		case json::array: {
				auto li = left.begin(),le= left.end();
				auto ri = right.begin(), re =right.end();
				while (li!=le && li!=le) {
					CompareResult r = compareJson(*li,*ri);
					if (r != cmpResultEqual) return r;
					++li;++ri;
				}
				if (li!=le) return cmpResultGreater;
				if (ri!=re) return cmpResultLess;
				return cmpResultEqual;
			}
		case json::object: {
			auto li = left.begin(),le= left.end();
			auto ri = right.begin(), re =right.end();
			while (li!=le && li!=le) {
				CompareResult r = compareStringsUnicode(StringRef((*li).getKey()),StringRef((*ri).getKey()));
				if (r != cmpResultEqual) return r;
				r = compareJson(*li,*ri);
				if (r != cmpResultEqual) return r;
				++li;++ri;
			}
			if (li!=le) return cmpResultGreater;
			if (ri!=re) return cmpResultLess;
			return cmpResultEqual;
			}
		default:
			return cmpResultEqual;
		}
	}
}

bool JsonIsLess::operator ()(const Value& v1,const Value& v2) const {
	return compareJson(v1,v2) == cmpResultLess;
}

bool JsonIsGreater::operator ()(const Value& v1,const Value& v2) const {
	return compareJson(v1,v2) == cmpResultGreater;
}



}

