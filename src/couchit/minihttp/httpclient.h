/*
 * httpclient.h
 *
 *  Created on: 11. 11. 2016
 *      Author: ondra
 */

#ifndef LIGHTCOUCH_MINIHTTP_HTTPCLIENT_H_
#define LIGHTCOUCH_MINIHTTP_HTTPCLIENT_H_
#include "../json.h"
#include "abstractio.h"
#include "netio.h"

#pragma once

namespace couchit {

class HttpClient {
public:

	HttpClient();
	HttpClient(std::string_view userAgent);

	///Opens http connection
	/**
	 *
	 * @param url url to open
	 * @param method HTTP method
	 * @param keepAlive set true to request keep alive connection
	 *
	 * Function can reuse currently opened connection if the target host is same. However
	 * it requires that response has been complete read, otherwise connection cannot be reused
	 *
	 * Also note that keepAlive is impossible, when response is sent as identity without content type
	 * @return reference to this object that allows to make chains
	 *
	 */
	HttpClient &open(std::string_view url, std::string_view method, bool keepAlive = true);

	///Sets i/o timeout
	void setTimeout(std::uintptr_t timeoutInMS);

	///Constructs CancelFunction object
	static CancelFunction initCancelFunction();

	///Sets request header
	/**
	 * @param headers header as json-object
	 * @return reference to this object that allows to make chains
	 * @note some headers can be replaces as the request is send. For example Content-Length
	 * will be set to apropriate body length
	 */
	HttpClient &setHeaders(Value headers);

	///Starts to generate a request body
	/** This is useful when you need to stream the body. Request is send in chunks
	 *
	 * @return OutputStream object
	 *
	 * @note to finish writting, call the function send(). After this you should destroy
	 * the returned object, because writting anything to it can cause undefined behaviour.
	 *
	 * @note by starting the body, you are unable to change headers, because headers are already
	 * sent.
	 */
	OutputStream beginBody();

	///Sends the request with or without the body
	/**
	 * If the body has been opened by beginBody(), the function finish the body and receives
	 * the response. Otherwise, without the body, the function generates header and sends
	 * empty request, then it receives the response.
	 *
	 * @return status code of the response. When error happens, function returns negative value
	 * which may contain platform specific code of the error. If unspecified error happened
	 * (for example timeout, or peer closed), function returns 0. Otherwise, function returns
	 * the status code. So you should expected 200 in most of cases
	 */
	int send();
	///Sends the request with the prepared body
	/**
	 * @param body string body.
	 * @return status code of the response. When error happens, function returns negative value
	 * which may contain platform specific code of the error. If unspecified error happened
	 * (for example timeout, or peer closed), function returns 0. Otherwise, function returns
	 * the status code. So you should expected 200 in most of cases
	 *
	 * @note if the body has been started by beginBody(), the function ignores the argument
	 */
	int send(const std::string_view &body);
	///Sends the request with the prepared body
	/**
	 * @param body pointer to binary body
	 * @param body_length length of the body in bytes
	 * @return status code of the response. When error happens, function returns negative value
	 * which may contain platform specific code of the error. If unspecified error happened
	 * (for example timeout, or peer closed), function returns 0. Otherwise, function returns
	 * the status code. So you should expected 200 in most of cases
	 *
	 * @note if the body has been started by beginBody(), the function ignores the argument
	 * */
	int send(const void *body, std::size_t body_length);

	///Retrieves headers after response is retrieved
	/**
	 * @return contains all headers including status ("_status"), message ("_message) and the http version ("_version")
	 */
	Value getHeaders();
	///Retrieves response stream
	/** You should always read whole response or call the function discardResponse() */
	InputStream getResponse();
	///Function waits for data during the reading.
	/** Allows to wait on data during the reading the stream. Note that even this function
	 * returns true, it still cause that data will not be ready.
	 *
	 * @param timeout in miliseconds
	 * @retval true, data arrived
	 * @retval false timeout happened
	 */
	bool waitForData(int timeout);

	///Discards response data to allow to reuse connection
	/** You have to call this function to finish response. This is not
	 * made automatically.
	 */
	void discardResponse();


	void close();

	void abort();

	int getStatus();

	json::String getStatusMessage();

	///ensures that connection exists
	/** This allows to have connection before request.
	 *  Once you have connection, then abort() can cancel this connection
	 *  (otherwise, calling abort() before request is sent is not MT safe)
	 */
	void connect();





protected:
	PNetworkConection conn;
	json::String curTarget;
	json::String userAgent;
	json::String curPath;
	json::String auth;
	json::String reqMethod;
	json::Value customHeaders;
	json::Value responseHeaders;
	uintptr_t curTimeout;
	bool keepAlive = false;
	int curStatus;
	bool headersSent;
	CancelFunction cancelFunction;


	void initRequest(bool haveBody, std::size_t contentLength);
	int readResponse();


	json::RefCntPtr<AbstractInputStream> responseData;

	static bool everythingRead(AbstractInputStream *stream);
	bool handleSendError();


	virtual void connectTarget();
	virtual json::String crackURL(std::string_view urlWithoutProtocol);
	virtual json::String custromPotocol(std::string_view url);

	void initConnection();

};



}



#endif /* LIGHTCOUCH_MINIHTTP_HTTPCLIENT_H_ */
