/*
 * revision.cpp
 *
 *  Created on: May 28, 2016
 *      Author: ondra
 */

#include "revision.h"

#include "num2str.h"
namespace LightCouch {

Revision::Revision():revId(0),tagsize(0) {
}

Revision::Revision(std::size_t revId, StrViewA tag):revId(revId),tagsize(tag.length) {
	if (tag.length>sizeof(this->tag)) {
		String s({"Revision tag is too long:", tag});
		throw std::runtime_error(s.c_str());
	}

	std::copy(tag.begin(),tag.end(), this->tag);

}

String Revision::toString() const {
	return String(21+tagsize, [&](char *c){
		char *s = c;
		c = unsignedToString(c,revId,20,10);
		*c++='-';
		std::copy(tag,tag+tagsize,c);
		c+=tagsize;
		return c-s;
	});
}

Revision::Revision(StrViewA revStr) {
	std::size_t p = revStr.indexOf("-",0);
	if (p = revStr.npos) {
		String s({"Invalid revision:", revStr});
		throw std::runtime_error(s.c_str());
	}
	std::size_t r = std::strtod(revStr.data,0);
	this->revId = r;

	p++;
	StrViewA tagStr = revStr.substr(p);
	if (tagStr.length > sizeof(tag)) {
		String s({"Revision tag is too long:", revStr});
		throw std::runtime_error(s.c_str());
	}
	std::copy(tagStr.begin(), tagStr.end(), tag);
	tagsize = tagStr.length;

}

CompareResult Revision::compare(const Revision& other) const {
	if (revId < other.revId) return cmpResultLess;
	if (revId > other.revId) return cmpResultGreater;
	StrViewA mtag(tag,tagsize);
	StrViewA otag(other.tag, other.tagsize);
	if (mtag < otag) return cmpResultLess;
	if (mtag > otag) return cmpResultGreater;
	return cmpResultEqual;
}

} /* namespace assetex */
