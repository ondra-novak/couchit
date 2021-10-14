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

void couchit::UrlBuilder::init(std::string_view basicUrl, std::string_view dbname, std::string_view resourcePath) {
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
UrlBuilder &couchit::UrlBuilder::add(std::string_view path) {
	buffer.push_back(curSep);
	UrlEncoder enc;
	enc(json::fromString(path), PutToBuffer(buffer));
	return *this;
}

UrlBuilder &couchit::UrlBuilder::add(std::string_view key, std::string_view value) {
	addKey(key);
	UrlEncoder enc;
	enc(json::fromString(value), PutToBuffer(buffer));
	return *this;
}

UrlBuilder &couchit::UrlBuilder::addJson(std::string_view key, Value value) {
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

UrlBuilder& UrlBuilder::add(std::string_view key, std::size_t value) {
	addKey(key);
	unsignedToString( PutToBuffer(buffer),value,21,10);
	return *this;
}

void UrlBuilder::addKey(const std::string_view& key) {
	if (curSep == '/') curSep = '?'; else curSep = '&';
	UrlEncoder enc;
	buffer.push_back(curSep);
	enc(json::fromString(key), PutToBuffer(buffer));
	buffer.push_back('=');
}




} /* namespace couchit */

