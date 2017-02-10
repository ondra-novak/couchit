/*
 * validator.cpp
 *
 *  Created on: 22. 5. 2016
 *      Author: ondra
 */

#include "validator.h"



namespace LightCouch {


Validator& Validator::add(IValidationFn* fn) {
	fnList.push_back(PValidationFn(fn));
	return *this;
}

bool Validator::remove(IValidationFn* fn, bool destroy) {
	for (auto i = fnList.begin(); i != fnList.end(); ++i) {
		if (i->get() == fn) {
			if (!destroy) i->release();
			fnList.erase(i);
			return true;
		}
	}
	return false;
}

Validator::Result Validator::validateDoc(const Value& document) const {
	for (auto &&i : fnList) {
		try {
			if ((*i)(document)) {
				return Result(i->getName());
			}
		} catch (std::exception &e) {
			return Result(i->getName(), e.what());
		}
	}
	return Result();
}

ValidationFailedException::~ValidationFailedException() throw () {
}

String ValidationFailedException::getWhatMsg() const throw () {
	return {"Validation failed on: '", res.failedName,"' - details: '", res.details, "'."};
}


} /* namespace LightCouch */

