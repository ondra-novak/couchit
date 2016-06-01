/*
 * uid.cpp
 *
 *  Created on: 9. 3. 2016
 *      Author: ondra
 */

#include "uid.h"

#include <time.h>
#include "lightspeed/base/containers/autoArray.tcc"
#include <lightspeed/base/text/textOut.tcc>
#include <lightspeed/mt/linux/atomic.h>


namespace LightCouch {


static atomic counter = 0;




UID::UID(ConstStrA dbsuffix, ConstStrA userSuffix) {

	time_t tval;
	time(&tval);
	TextOut<UID::WriteIter, SmallAlloc<256> > fmt(getWriteIterator());
	fmt.setBase(62);
	atomicValue v = lockInc(counter);
	fmt("%{08}1%%{06}2%%3%%4")
		<< (lnatural)tval
		<< (Bin::natural32)(v & 0x3FFFFFFF)
		<< dbsuffix << userSuffix;

}

}

