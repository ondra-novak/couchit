/*
 * exception.h
 *
 *  Created on: 9. 3. 2016
 *      Author: ondra
 */

#ifndef LIBS_LIGHTCOUCH_SRC_EXCEPTION_H_
#define LIBS_LIGHTCOUCH_SRC_EXCEPTION_H_
#include <lightspeed/base/containers/string.h>
#include <lightspeed/base/exceptions/exception.h>
#include <lightspeed/base/containers/constStr.h>
#include <lightspeed/base/exceptions/httpStatusException.h>
#include <lightspeed/utils/json/json.h>
#include "object.h"

namespace LightCouch {

using namespace LightSpeed;

class RequestError: public HttpStatusException {
public:
	LIGHTSPEED_EXCEPTIONFINAL;

	RequestError(const ProgramLocation &loc, StringA url, natural code, StringA message, JSON::Value extraInfo);

	JSON::Value getExtraInfo() const {return extraInfo;}
	virtual ~RequestError() throw();
protected:
	JSON::Value extraInfo;

    virtual void message(ExceptionMsg &msg) const;

};


class DocumentNotEditedException: public Exception {
public:
	LIGHTSPEED_EXCEPTIONFINAL;

	DocumentNotEditedException(const ProgramLocation &location, ConstValue docId):Exception(location),documentId(docId) {}
	virtual ~DocumentNotEditedException() throw() {}

	const ConstValue documentId;

	static const char *msgText;
	static const char *msgNone;
	virtual void message(ExceptionMsg &msg) const {
		if (documentId == null) msg(msgText) << msgNone;
		else msg(msgText) << documentId.getStringA();
	}

};

class UpdateException: public Exception{
public:
	struct ErrorItem {
		ConstStrA errorType;
		ConstStrA reason;
		JSON::ConstValue document;
		JSON::ConstValue errorDetails;
		bool isConflict() const;
	};


	LIGHTSPEED_EXCEPTIONFINAL;
	UpdateException(const ProgramLocation &loc, const StringCore<ErrorItem> &errors);
	ConstStringT<ErrorItem> getErrors() const;
	const ErrorItem &getError(natural index) const;
	natural getErrorCnt() const;

	static const char *msgText;


protected:
	StringCore<ErrorItem> errors;

	void message(ExceptionMsg &msg) const;
};

class CanceledException: public Exception{
public:

	LIGHTSPEED_EXCEPTIONFINAL;
	CanceledException(const ProgramLocation &loc):Exception(loc) {}

	static const char *msgText;


protected:

	void message(ExceptionMsg &msg) const;
};


}



#endif /* LIBS_LIGHTCOUCH_SRC_EXCEPTION_H_ */
