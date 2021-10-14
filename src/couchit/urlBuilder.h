/*
 * urlBuilder.h
 *
 *  Created on: 24. 10. 2016
 *      Author: ondra
 */

#ifndef LIGHTCOUCH_URLBUILDER_H_
#define LIGHTCOUCH_URLBUILDER_H_
#include <vector>
#include "json.h"

namespace couchit {


class UrlBuilder {
public:


	void init(std::string_view basicUrl, std::string_view dbname, std::string_view resourcePath);
	void init();
	UrlBuilder &add(std::string_view path);
	UrlBuilder &add(std::string_view key, std::string_view value);
	UrlBuilder &addJson(std::string_view key, Value value);
	UrlBuilder &add(std::string_view key, std::size_t value);
	operator std::string_view() const {return std::string_view(buffer.data(), buffer.size());}

protected:

	char curSep;

	std::vector<char> buffer;

	void addKey(const std::string_view &key);


};

} /* namespace couchit */

#endif /* LIGHTCOUCH_URLBUILDER_H_ */
