/*
 * defaultUIDGen.cpp
 *
 *  Created on: 7. 9. 2016
 *      Author: ondra
 */


#include <lightspeed/base/text/textOut.tcc>
#include <lightspeed/base/containers/autoArray.tcc>
#include <lightspeed/base/memory/smallAlloc.h>
#include <lightspeed/base/streams/secureRandom.h>
#include <lightspeed/base/memory/singleton.h>

#include "defaultUIDGen.h"

namespace LightCouch {



DefaultUIDGen::DefaultUIDGen() {
	SecureRandom secrand;
	int32_t seed;
	secrand.blockRead(&seed, sizeof(seed),true);
	secrand.blockRead(&counter,sizeof(counter),true);
	rgn = Rand(seed);
	counter &= 0x7FFFFF;

}

ConstStrA DefaultUIDGen::operator ()(AutoArray<char>& buffer, ConstStrA prefix) {

	Synchronized<FastLock> _(lock);
	time_t now;
	time(&now);

	counter = (counter+1) & 0x7FFFFF;
	return generateUID(buffer,prefix, now, counter,&rgn, 20);
}

ConstStrA DefaultUIDGen::generateUID(AutoArray<char>& buffer, ConstStrA prefix,
		natural timeparam, natural counterparam, Rand* randomGen,
		natural totalCount) {

	buffer.clear();
	buffer.append(prefix);
	AutoArray<char>::WriteIter iter = buffer.getWriteIterator();
	TextOut<AutoArray<char>::WriteIter &, SmallAlloc<256> > out(iter);
	out.setBase(62);

	out("%{06}1%%{04}2") << timeparam << counterparam;

	if (randomGen) {
		while (buffer.length() < totalCount+prefix.length()) {
			out("%1") << (randomGen->getNext() % 62);
		}
	}

	return buffer;
}

DefaultUIDGen &DefaultUIDGen::getInstance() {
	return Singleton<DefaultUIDGen>::getInstance();
}

StringA DefaultUIDGen::operator()(ConstStrA prefix) {
	AutoArray<char> buffer;
	return this->operator ()(buffer,prefix);
}

} /* namespace LightCouch */

