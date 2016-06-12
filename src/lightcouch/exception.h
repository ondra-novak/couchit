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
#include <lightspeed/utils/json/json.h>
#include "object.h"

namespace LightCouch {

using namespace LightSpeed;

class RequestError: public Exception {
public:
	LIGHTSPEED_EXCEPTIONFINAL;

	RequestError(const ProgramLocation &loc, natural code, ConstStrA message, JSON::Value extraInfo);

	natural getStatus() const {return code;}
	ConstStrA getMessage() const {return msg;}
	JSON::Value getExtraInfo() const {return extraInfo;}
	virtual ~RequestError() throw();
protected:
	natural code;
	StringA msg;
	JSON::Value extraInfo;

    virtual void message(ExceptionMsg &msg) const;

};


class DocumentNotEditedException: public Exception {
public:
	LIGHTSPEED_EXCEPTIONFINAL;

	DocumentNotEditedException(const ProgramLocation &location, ConstValue docId):Exception(location),documentId(docId) {}

	const ConstValue documentId;

	static const char *msgText;
	static const char *msgNone;
	virtual void message(ExceptionMsg &msg) const {
		if (documentId == null) msg(msgText) << msgNone;
		else msg(msgText) << documentId.getStringA();
	}

};

}



#endif /* LIBS_LIGHTCOUCH_SRC_EXCEPTION_H_ */
