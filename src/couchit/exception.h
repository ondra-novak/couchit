/*
 * exception.h
 *
 *  Created on: 9. 3. 2016
 *      Author: ondra
 */

#ifndef LIBS_LIGHTCOUCH_SRC_EXCEPTION_H_
#define LIBS_LIGHTCOUCH_SRC_EXCEPTION_H_
#include "json.h"

namespace couchit {

class Exception: public std::exception {
public:

	virtual const char *what() const throw() override;
	virtual ~Exception() throw() {}

protected:
	mutable String whatMsg;
	virtual String getWhatMsg() const throw() = 0;

};

class HttpStatusException: public Exception {
public:
	HttpStatusException(const String &url, unsigned int code, const String &message);

	unsigned int getCode() const throw() {return code;}
	const String &getURL() const throw() {return url;}
	const String &getMessage() const throw() {return message;}

protected:
	virtual String getWhatMsg() const throw();

	unsigned int code;
	String url;
	String message;

};



class RequestError: public HttpStatusException {
public:

	RequestError(const String &url, unsigned int code, const String& message, const Value &extraInfo);

	Value getExtraInfo() const {return extraInfo;}
	virtual ~RequestError() throw();
protected:
	Value extraInfo;

	virtual String getWhatMsg() const throw();

};

class DocumentHasNoID: public Exception {
public:

	DocumentHasNoID(Value document):document(document) {}
	Value getDocument() const {return document;}
protected:
	Value document;

	virtual String getWhatMsg() const throw();

};


class UpdateException: public Exception{
public:
	struct ErrorItem {
		String errorType;
		String reason;
		Value document;
		Value errorDetails;
		bool isConflict() const;
	};


	UpdateException(std::vector<ErrorItem> &&errors);
	const std::vector<ErrorItem> &getErrors() const;
	const ErrorItem &getError(std::size_t index) const;
	std::size_t getErrorCnt() const;

protected:
	std::vector<ErrorItem> errors;

	virtual String getWhatMsg() const throw();
};

class SystemException: public Exception {
public:

	SystemException(String text, int errn):text(text),errn(errn) {}
	int getErrorNo() const {return errn;}
protected:
	String text;
	int errn;
	virtual String getWhatMsg() const throw();

};

/*
class CanceledException: public Exception{
public:

	CanceledException() {}


protected:

	virtual String getWhatMsg() const throw();

};
*/

}



#endif /* LIBS_LIGHTCOUCH_SRC_EXCEPTION_H_ */
