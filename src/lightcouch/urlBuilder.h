/*
 * urlBuilder.h
 *
 *  Created on: 24. 10. 2016
 *      Author: ondra
 */

#ifndef LIGHTCOUCH_URLBUILDER_H_
#define LIGHTCOUCH_URLBUILDER_H_
#include "json.h"

namespace LightCouch {

using namespace LightSpeed;

class UrlBuilder {
public:

	void init(ConstStrA basicUrl, ConstStrA dbname, ConstStrA resourcePath);
	void init();
	UrlBuilder &add(StrViewA path);
	UrlBuilder &add(StrViewA key, StrViewA value);
	UrlBuilder &addJson(StrViewA key, Value value);
	operator StrViewA() const {return ~ConstStrA(buffer.getArray());}
	operator ConstStrA() const {return buffer.getArray();}

protected:

	char curSep;


	AutoArrayStream<char> buffer;


};

} /* namespace LightCouch */

#endif /* LIGHTCOUCH_URLBUILDER_H_ */
