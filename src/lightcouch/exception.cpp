/*
 * exception.cpp
 *
 *  Created on: 9. 3. 2016
 *      Author: ondra
 */

#include "exception.h"

namespace LightCouch {

RequestError::RequestError(const ProgramLocation& loc, StringA url, natural code,
		StringA message,Value extraInfo)
	:HttpStatusException(loc,url,code,message), extraInfo(extraInfo)
{
}

RequestError::~RequestError() throw () {
}

void RequestError::message(ExceptionMsg& msg) const {
	std::string details;
	if (this->extraInfo != null) {
		details = extraInfo.stringify();
	}
	msg("CouchDB error: url=%1, status=%2, message=%3, details=%4")
		<< this->url
		<< this->status
		<< this->statusMsg
		<< StringRef(details);
}


UpdateException::UpdateException(
		const ProgramLocation& loc, const StringCore<ErrorItem>& errors)

	:Exception(loc),errors(errors)
{
}

ConstStringT<UpdateException::ErrorItem> UpdateException::getErrors() const {
	return errors;
}

void UpdateException::message(ExceptionMsg& msg) const {
	msg(msgText) << errors.length();
}


void LightCouch::CanceledException::message(ExceptionMsg& msg) const {
	msg(msgText);
}

const char *DocumentNotEditedException::msgText = "Document %1 is not edited. You have to call edit() first";
const char *DocumentNotEditedException::msgNone = "<n/a>";
const char *UpdateException::msgText = "Update exception - some items was not written: %1";
const char *CanceledException::msgText = "Operation has been canceled";


bool UpdateException::ErrorItem::isConflict() const {
	return errorType == "conflict";
}


const UpdateException::ErrorItem& UpdateException::getError(natural index) const {
	return errors[index];
}

natural UpdateException::getErrorCnt() const {
	return errors.length();
}


}

