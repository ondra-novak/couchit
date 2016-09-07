/*
 * defaultUIDGen.h
 *
 *  Created on: 7. 9. 2016
 *      Author: ondra
 */

#ifndef LIGHTCOUCH_DEFAULTUIDGEN_H_
#define LIGHTCOUCH_DEFAULTUIDGEN_H_

#include "iidgen.h"

namespace LightCouch {

///Default UID generator
class DefaultUIDGen: public IIDGen {
public:
	virtual ConstStrA operator()(AutoArray<char> &buffer, ConstStrA prefix);

	static atomic gcounter;


};

} /* namespace LightCouch */

#endif /* LIGHTCOUCH_DEFAULTUIDGEN_H_ */
