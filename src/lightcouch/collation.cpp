/*
 * collation.cpp
 *
 *  Created on: 25. 6. 2016
 *      Author: ondra
 */



#include "collation.h"



namespace LightCouch {


template<typename Fn>
class Utf8Reader {
public:
	Utf8Reader(const Fn &fn):fn(fn) {}

	int operator()() const {
		int c = fn();
		if (c == -1) return -1;
		if (c & 0x80 == 0) return c;
		int uchar = 0;
		if ((c & 0xe0) == 0xc0) {
			uchar = (c & 0x3F) << 6;
			uchar |= fn() & 0x3F;
		}
		else if ((c & 0xf0) == 0xe0) {
			uchar = (c & 0x1F) << 12;
			uchar |= (fn() & 0x3F) << 6;
			uchar |= (fn() & 0x3F);
		}
		else if ((c & 0xF8) == 0xF0) {
			uchar = (c & 0x0F) << 18;
			uchar |= (fn() & 0x3F) << 12;
			uchar |= (fn() & 0x3F) << 6;
			uchar |= (fn() & 0x3F);
		} else {
			uchar = 0;
		}
		return uchar;
	}

protected:
	Fn fn;
};

template<typename Fn>
Utf8Reader<Fn> createUtf82WideReader(const Fn &fn) {
	return Utf8Reader<Fn>(fn);
}


CompareResult compareStringsUnicode(StrViewA str1, StrViewA str2) {
	unsigned int pos1 = 0;
	unsigned int pos2 = 0;
	auto iter1 = createUtf82WideReader([&]() -> int {
		if (pos1 < str1.length) return str1[pos1++]; else return -1;
	});
	auto iter2 = createUtf82WideReader([&]() -> int {
		if (pos2 < str2.length) return str2[pos2++]; else return -1;
	});


	int c1 = iter1();
	int c2 = iter2();
	while (c1 > 0 && c2 > 0) {
		if (c1 < c2) return cmpResultLess;
		if (c1 > c2) return cmpResultGreater;
		c1 = iter1();
		c2 = iter2();
	}
	if (c1 != 0) return cmpResultGreater;
	if (c2 != 0) return cmpResultLess;
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
		case json::string: return compareStringsUnicode(left.getString(),right.getString());
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
				CompareResult r = compareStringsUnicode((*li).getKey(),(*ri).getKey());
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

