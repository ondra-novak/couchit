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
	UrlBuilder &add(ConstStrA path);
	UrlBuilder &add(ConstStrA key, ConstStrA value);
	UrlBuilder &addJson(ConstStrA key, Value value);
	ConstStrA toString() const;
	operator StringRef() const {return StringRef(buffer.getArray());}
	operator ConstStrA() const {return ConstStrA(StringRef(buffer.getArray()));}

protected:

	char curSep;


	AutoArrayStream<char> buffer;


};

} /* namespace LightCouch */

#endif /* LIGHTCOUCH_URLBUILDER_H_ */
