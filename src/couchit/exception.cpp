/*
 * exception.cpp
 *
 *  Created on: 9. 3. 2016
 *      Author: ondra
 */

#include "exception.h"

namespace couchit {

RequestError::RequestError(const String &url, unsigned int code, const String& message, const Value &extraInfo)
	:HttpStatusException(url,code,message), extraInfo(extraInfo)
{
}

RequestError::~RequestError() throw () {
}

String RequestError::getWhatMsg() const throw() {
	String details;
	if (this->extraInfo.defined()) {
		details = extraInfo.stringify();
	}
	std::ostringstream buff;
	buff << "CouchDB error: url=" << url << ", status=" << code << ", message=" << message << ", details=" << details;
	return String(buff.str());
}


UpdateException::UpdateException(const StringView<ErrorItem> &errors)
{
	this->errors.reserve(errors.length);
	for (auto &&x : errors) this->errors.push_back(x);
}
UpdateException::UpdateException(std::vector<ErrorItem> &&errors)
	:errors(std::move(errors))
{

}

StringView<UpdateException::ErrorItem>  UpdateException::getErrors() const {
	return errors;
}

String UpdateException::getWhatMsg() const throw() {
	std::ostringstream buff;
	for (auto x : errors) {
		buff << "Update error '" << x.errorType << "' for document '" << x.document["_id"].getString() << "'. ";
	}
	return String(buff.str());
}

/*

String CanceledException::getWhatMsg() const throw() {
	return "Operation has been canceled.";
}
*/


bool UpdateException::ErrorItem::isConflict() const {
	return errorType == "conflict";
}


const UpdateException::ErrorItem& UpdateException::getError(std::size_t index) const {
	return errors[index];
}

std::size_t UpdateException::getErrorCnt() const {
	return errors.size();
}

String DocumentHasNoID::getWhatMsg() const throw() {
	return String({"Document has no id: ", document.toString()});
}


const char* Exception::what() const throw () {
	if (whatMsg.empty()) {
		whatMsg = getWhatMsg();
	}
	return whatMsg.c_str();
}

HttpStatusException::HttpStatusException(const String& url, unsigned int code, const String& message)
	:code(code),url(url),message(message)
{
}

String HttpStatusException::getWhatMsg() const throw () {
	std::ostringstream buff;
	buff << "Unexpected HTTP status: url=" << url << ", status=" << code << ", message=" << message << ".";
	return String(buff.str());
}

String couchit::SystemException::getWhatMsg() const throw () {
	std::ostringstream buff;
	buff << StrViewA(text) << " errno=" << errn;
	return String(buff.str());
}

}

