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
	JSON::PFactory f = JSON::create();
	ConstStrA details;
	if (this->extraInfo != null) {
		details = f->toString(*extraInfo);
	}
	msg("CouchDB error: status=%1, message=%2, details=%3") << code << this->msg << details;
}

const char *DocumentNotEditedException::msgText = "Document %1 is not edited. You have to call edit() first";
const char *DocumentNotEditedException::msgNone = "<n/a>";



}
