/*
 * defaultUIDGen.cpp
 *
 *  Created on: 7. 9. 2016
 *      Author: ondra
 */

#include "defaultUIDGen.h"

namespace LightCouch {


atomic DefaultUIDGen::gcounter = 0;


ConstStrA DefaultUIDGen::operator ()(AutoArray<char>& buffer, ConstStrA prefix) {

	buffer.clear();
	buffer.append(prefix);

	time_t now;
	time(&now);

	AutoArray<char>::WriteIter iter = buffer.getWriteIterator();
	TextOut<AutoArray<char>::WriteIter &, SmallAlloc<256> > out;
	out.setBase(32);
	out("%{07}1") << lnatural(now);

	//not finished




}


} /* namespace LightCouch */
