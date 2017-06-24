/*
 * urlBuilder.cpp
 *
 *  Created on: 24. 10. 2016
 *      Author: ondra
 */

#include "urlBuilder.h"

#include "minihttp/urlencode.h"
#include "num2str.h"

namespace couchit {


template<typename V, typename X>
static std::vector<V> &operator+=(std::vector<V> &v, const X &x) {
	for (auto &&a : x) {
		v.push_back(a);
	}
	return v;
}

void couchit::UrlBuilder::init(StrViewA basicUrl, StrViewA dbname, StrViewA resourcePath) {
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

namespace {
class PutToBuffer {
public:
	std::vector<char> &buffer;
	PutToBuffer(std::vector<char> &buffer):buffer(buffer) {}
	void operator()(char c) {buffer.push_back(c);}
};
}
UrlBuilder &couchit::UrlBuilder::add(StrViewA path) {
	buffer.push_back(curSep);
	UrlEncoder enc;
	enc(json::fromString(path), PutToBuffer(buffer));
	return *this;
}

UrlBuilder &couchit::UrlBuilder::add(StrViewA key, StrViewA value) {
	addKey(key);
	UrlEncoder enc;
	enc(json::fromString(value), PutToBuffer(buffer));
	return *this;
}

UrlBuilder &couchit::UrlBuilder::addJson(StrViewA key, Value value) {
	if (!value.defined()) return *this;
	addKey(key);
	UrlEncoder enc;
	value.serialize(json::emitUtf8, [&](char c) {
		enc(json::oneCharStream(c), PutToBuffer(buffer));
	});
	return *this;
}

void UrlBuilder::init() {

}

UrlBuilder& UrlBuilder::add(StrViewA key, std::size_t value) {
	addKey(key);
	unsignedToString( PutToBuffer(buffer),value,21,10);
	return *this;
}

void UrlBuilder::addKey(const StrViewA& key) {
	if (curSep == '/') curSep = '?'; else curSep = '&';
	UrlEncoder enc;
	buffer.push_back(curSep);
	enc(json::fromString(key), PutToBuffer(buffer));
	buffer.push_back('=');
}




} /* namespace couchit */

