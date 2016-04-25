/*
 * uid.h
 *
 *  Created on: 9. 3. 2016
 *      Author: ondra
 */

#ifndef LIBS_LIGHTCOUCH_SRC_LIGHTCOUCH_UID_H_
#define LIBS_LIGHTCOUCH_SRC_LIGHTCOUCH_UID_H_
#include <lightspeed/base/containers/constStr.h>
#include <lightspeed/utils/json/json.h>
#include "lightspeed/base/iter/iterator.h"

#include "lightspeed/base/containers/carray.h"
using LightSpeed::IteratorBase;



namespace LightCouch {
using namespace LightSpeed;

class UIDIterator: public IteratorBase<ConstStrA, UIDIterator> {
public:

	UIDIterator(JSON::Value data);

	const ConstStrA &getNext();
	const ConstStrA &peek() const;
	bool hasItems() const;
	natural getRemain() const;


protected:

	JSON::Value data;
	JSON::Iterator iter;
	mutable ConstStrA tmp;
};



typedef CArray<char,25> LocalUID;


}



#endif /* LIBS_LIGHTCOUCH_SRC_LIGHTCOUCH_UID_H_ */
