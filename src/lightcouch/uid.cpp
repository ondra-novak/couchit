/*
 * uid.cpp
 *
 *  Created on: 9. 3. 2016
 *      Author: ondra
 */

#include "uid.h"

namespace LightCouch {




UIDIterator::UIDIterator(JSON::Value data)
	:data(data),iter(data->getFwIter())
{

}

const ConstStrA& UIDIterator::getNext() {
	tmp = iter.getNext()->getStringUtf8();
	return tmp;
}

const ConstStrA& UIDIterator::peek() const {
	tmp = iter.peek()->getStringUtf8();
	return tmp;
}

bool UIDIterator::hasItems() const {
	return iter.hasItems();
}

natural UIDIterator::getRemain() const {
	return iter.getRemain();
}




}



