/*
 * validator.cpp
 *
 *  Created on: 22. 5. 2016
 *      Author: ondra
 */

#include "lightspeed/base/containers/autoArray.tcc"
#include "validator.h"



namespace LightCouch {


Validator& Validator::add(IValidationFn* fn) {
	fnList.add(fn);
	return *this;
}

bool Validator::remove(IValidationFn* fn, bool destroy) {
	for (std::size_t i = 0, cnt = fnList.length(); i < cnt; i++) {
		if (fnList[i].get() == fn) {
			PValidationFn x = fnList[i];
			fnList.erase(i);
			if (!destroy) x.detach();
			return true;
		}
	}
	return false;
}

Validator::Result Validator::validateDoc(const Value& document) const {
	for (std::size_t i = 0, cnt = fnList.length(); i < cnt; i++) {
		try {
			if (!fnList[i]->operator ()(document)) {
				return Result(fnList[i]->getName());
			}
		} catch (std::exception &e) {
			return Result(fnList[i]->getName(), e.what());
		}
	}
	return Result();
}

ValidationFailedException::~ValidationFailedException() throw () {
}

void ValidationFailedException::message(ExceptionMsg& msg) const {
	msg("Validation failed on function: %1, details: %2") << res.failedName << res.details;
}


} /* namespace LightCouch */

