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
	UrlBuilder &add(StrView path);
	UrlBuilder &add(StrView key, StrView value);
	UrlBuilder &addJson(StrView key, Value value);
	operator StrView() const {return buffer.getArray();}
	operator ConstStrA() const {return buffer.getArray();}

protected:

	char curSep;


	AutoArrayStream<char> buffer;


};

} /* namespace LightCouch */

#endif /* LIGHTCOUCH_URLBUILDER_H_ */
