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

namespace LightCouch {


class UrlBuilder {
public:


	void init(StrViewA basicUrl, StrViewA dbname, StrViewA resourcePath);
	void init();
	UrlBuilder &add(StrViewA path);
	UrlBuilder &add(StrViewA key, StrViewA value);
	UrlBuilder &addJson(StrViewA key, Value value);
	UrlBuilder &add(StrViewA key, std::size_t value);
	operator StrViewA() const {return buffer;}

protected:

	char curSep;

	std::vector<char> buffer;

	void addKey(const StrViewA &key);


};

} /* namespace LightCouch */

#endif /* LIGHTCOUCH_URLBUILDER_H_ */
