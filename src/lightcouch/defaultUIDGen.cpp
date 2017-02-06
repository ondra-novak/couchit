/*
 * defaultUIDGen.cpp
 *
 *  Created on: 7. 9. 2016
 *      Author: ondra
 */



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

StrViewA DefaultUIDGen::operator ()(AutoArray<char>& buffer, StrViewA prefix) {

	Synchronized<FastLock> _(lock);
	time_t now;
	time(&now);

	counter = (counter+1) & 0x7FFFFF;
	return generateUID(buffer,prefix, now, counter,&rgn, 20);
}

StrViewA DefaultUIDGen::generateUID(AutoArray<char>& buffer, StrViewA prefix,
		std::size_t timeparam, std::size_t counterparam, Rand* randomGen,
		std::size_t totalCount) {

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

String DefaultUIDGen::operator()(StrViewA prefix) {
	AutoArray<char> buffer;
	return this->operator ()(buffer,prefix);
}

} /* namespace LightCouch */

