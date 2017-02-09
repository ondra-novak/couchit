/*
 * urlBuilder.cpp
 *
 *  Created on: 24. 10. 2016
 *      Author: ondra
 */

#include "urlBuilder.h"

#include "minihttp/urlencode.h"

namespace LightCouch {


template<typename V, typename X>
static std::vector<V> &operator+=(std::vector<V> &v, const X &x) {
	for (auto &&a : x) {
		v.push_back(a);
	}
}

void LightCouch::UrlBuilder::init(StrViewA basicUrl, StrViewA dbname, StrViewA resourcePath) {
	buffer.clear();
	if (resourcePath.empty()) {
		buffer += basicUrl;
		buffer += dbname;
	} else if (resourcePath[0] == '/') {
		buffer += basicUrl;
		buffer += resourcePath.substr(1);
	} else {
		buffer += basicUrl;
		buffer += dbname;
		buffer.push_back('/');
		buffer += resourcePath;
	}
	curSep = '/';
}

UrlBuilder &LightCouch::UrlBuilder::add(StrViewA path) {
	UrlEncoder enc;
	enc(json::fromString(path),[&](char c) {buffer.push_back(c);});
	return *this;
}

UrlBuilder &LightCouch::UrlBuilder::add(StrViewA key, StrViewA value) {
	if (curSep == '/') curSep = '?'; else curSep = '&';
	UrlEncoder enc;
	auto wr = [&](char c) {buffer.push_back(c);};
	buffer.push_back(curSep);
	enc(json::fromString(key),wr);
	buffer.push_back('=');
	enc(json::fromString(value),wr);
	return *this;
}

UrlBuilder &LightCouch::UrlBuilder::addJson(StrViewA key, Value value) {
	if (curSep == '/') curSep = '?'; else curSep = '&';
	auto wr = [&](char c) {buffer.push_back(c);};
	buffer.push_back(curSep);
	UrlEncoder enc;
	enc(json::fromString(key),wr);
	buffer.push_back('=');
	value.serialize(json::emitUtf8, [&](char c) {
		enc(json::oneCharStream(c), wr);
	});
	return *this;
}

void UrlBuilder::init() {
}





} /* namespace LightCouch */

