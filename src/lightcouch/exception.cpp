/*
 * exception.cpp
 *
 *  Created on: 9. 3. 2016
 *      Author: ondra
 */

#include "exception.h"

namespace LightCouch {

RequestError::RequestError(const ProgramLocation& loc, natural code,
		ConstStrA message, JSON::Value extraInfo)
	:Exception(loc), code(code),msg(message),extraInfo(extraInfo)
{
}

RequestError::~RequestError() throw () {
}

void RequestError::message(ExceptionMsg& msg) const {
	msg("CouchDB error: status=%1, message=%2") << code << this->msg;
}




}
