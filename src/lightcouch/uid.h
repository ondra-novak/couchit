/*
 * uid.h
 *
 *  Created on: 9. 3. 2016
 *      Author: ondra
 */

#ifndef LIBS_LIGHTCOUCH_SRC_LIGHTCOUCH_UID_H_
#define LIBS_LIGHTCOUCH_SRC_LIGHTCOUCH_UID_H_
#include <lightspeed/base/containers/constStr.h>
#include "lightspeed/base/containers/autoArray.h"

#include "lightspeed/base/memory/smallAlloc.h"


namespace LightCouch {
using namespace LightSpeed;


class UID: public AutoArray<char, SmallAlloc<32> > {
public:

	UID(ConstStrA dbsuffix, ConstStrA userPrefix);

};

}



#endif /* LIBS_LIGHTCOUCH_SRC_LIGHTCOUCH_UID_H_ */
