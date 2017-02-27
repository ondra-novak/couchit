/*
 * httpclient.cpp
 *
 *  Created on: 13. 11. 2016
 *      Author: ondra
 */

#include "httpclient.h"



#include "chunked.h"
#include "hdrrd.h"
#include "hdrwr.h"
namespace couchit {


couchit::HttpClient::HttpClient()
	:curTimeout(70000)
{
}

couchit::HttpClient::HttpClient(StrViewA userAgent)
	:userAgent(userAgent),curTimeout(70000)
{
}

HttpClient& couchit::HttpClient::open(StrViewA url, StrViewA method, bool keepAlive) {
	this->keepAlive = keepAlive;

	curStatus = 0;
	json::String newTarget;

	if (url.substr(0,7) == "http://") {
		StrViewA u1 = url.substr(7);
		newTarget = crackURL(u1);
	} else {
		newTarget = custromPotocol(url);
	}
	//ask to different target - close current connection
	if (newTarget != curTarget || (conn!= nullptr &&  conn->hasErrors())
			|| (responseData != nullptr && !everythingRead(responseData)))
		conn = nullptr;
	curTarget = newTarget;
	responseData = nullptr;
	reqMethod = method;
	customHeaders = json::Value();
	responseHeaders = json::Value();
	headersSent = false;
	return *this;
}

HttpClient& couchit::HttpClient::setHeaders(Value headers) {
	customHeaders = headers;
	return *this;

}

bool HttpClient::handleSendError() {
	if (conn == nullptr)
		return true;
	if (conn->getLastSendError()) {
		curStatus =  -conn->getLastSendError();
		conn = nullptr;
		return true;
	}
	if (conn->isTimeout()) {
		conn = nullptr;
		curStatus = 0;
		return true;;
	}
	return false;

}

OutputStream couchit::HttpClient::beginBody() {

	class ChunkedStream: public IOutputStream {
	public:

		ChunkedStream(const OutputStream &stream):wr(stream) {}
		virtual bool write(const unsigned char *buffer, std::size_t size, std::size_t *written = 0) {
			return wr(buffer,size,written);
		}
		~ChunkedStream() {
			wr(0,0,0);
		}

	protected:
		ChunkedWrite<OutputStream> wr;

	};

	class ErrorStream: public IOutputStream {
	public:
		virtual bool write(const unsigned char *, std::size_t , std::size_t *) {
			return false;
		}

	};

	if (curTarget.empty()) {
		return OutputStream(new ErrorStream);
	}else {
		initRequest(true,-1);
		if (handleSendError()) {
			return OutputStream(new ErrorStream);
		} else {
			return OutputStream(new ChunkedStream(OutputStream(conn)));
		}
	}
}

int couchit::HttpClient::send() {
	if (!headersSent) {
		initRequest(false,0);
	}
	if (handleSendError()) {
		return curStatus;
	}
	return readResponse();
}

int couchit::HttpClient::send(const StrViewA& body) {
	return send(body.data,body.length);
}

int couchit::HttpClient::send(const void* body, std::size_t body_length) {
	if (!headersSent) {
		initRequest(true,body_length);
		if (handleSendError()) {
			return curStatus;
		}
		OutputStream stream(conn);
		stream(reinterpret_cast<const unsigned char *>(body), body_length);
	}
	if (handleSendError()) {
		return curStatus;
	}
	return readResponse();
}

Value couchit::HttpClient::getHeaders() {
	return responseHeaders;
}

InputStream couchit::HttpClient::getResponse() {

	class EmptyStream: public IInputStream {
	public:
		virtual const unsigned char *read(std::size_t , std::size_t *readready) {
			if (readready) *readready = 0;
			return 0;
		}
		virtual json::BinaryView read(std::size_t processed) {
			return json::BinaryView(nullptr, 0);
		}
	};
	if (responseData == nullptr) {
		return new EmptyStream;
	} else {
		return static_cast<IInputStream *>(responseData);
	}
}

bool couchit::HttpClient::waitForData(unsigned int timeout) {
	return conn->waitForInput(timeout);
}

void couchit::HttpClient::discardResponse() {
	if (responseData != nullptr) {
		std::size_t p = 0;
		BinaryView b = responseData->read(0);
		while (!b.empty()) {
			b = responseData->read(b.length);
		}
	}
}

void HttpClient::initRequest(bool haveBody, std::size_t contentLength) {

	if (conn == nullptr) {
		connectTarget();
		if (conn == nullptr) {
			curStatus = 0;
			return;
		} else {
			initConnection();
		}
	}

	json::Object hdr(customHeaders);
	hdr("_method",reqMethod)
	   ("_uri",curPath)
	   ("_version","HTTP/1.1")
	   ("Host",curTarget)
	   ("User-Agent",userAgent);

	if (haveBody) {
		if (contentLength == std::size_t(-1)) {
			hdr("Transfer-Encoding","chunked");
		} else {
			hdr("Content-Length",contentLength);
		}
	}

	if (!keepAlive) {
		hdr("Connection","close");
	}

	if (!auth.empty()) {
		String authstr((auth.length()+2)*4/3+7,[&](char *c) {
			char *s = c;
			StrViewA basic("Basic ");
			for (auto x: basic) *c++ = x;
			json::base64->encodeBinaryValue(BinaryView(auth.str()),[&](StrViewA z){
				for (auto x: z) *c++=x;
			});
			return c - s;
		});
		hdr("Authorization", authstr);
	}

	OutputStream stream(conn);
	BufferedWrite<OutputStream> bufferedOutput(stream);

	HeaderWrite<BufferedWrite<OutputStream> > hdrwr(bufferedOutput);

	hdrwr.serialize(hdr);
	bufferedOutput.flush();

	headersSent = true;
}

int HttpClient::readResponse() {

	class ChunkedStream: public IInputStream {
	public:
		ChunkedStream(const InputStream &stream):chread(stream) {}
		virtual json::BinaryView read(std::size_t processed) {
			return chread(processed);
		}


	protected:
		ChunkedRead<InputStream> chread;
	};

	class LimitedStream: public IInputStream {
	public:
		LimitedStream(const InputStream &stream, std::size_t limit)
			:chread(stream) {
				chread.setLimit(limit);
		}
		virtual json::BinaryView read(std::size_t processed) {
			return chread(processed);
		}

	protected:
		BufferedReadWLimit<InputStream> chread;
	};

	if (conn == nullptr) {
		return curStatus;
	}


	InputStream stream(conn);
	BufferedRead<InputStream> bufferedInput(stream);

	HeaderRead<BufferedRead<InputStream> > hdrrd(bufferedInput);

	json::Value v = hdrrd.parseHeaders();
	if (!v.defined()) {
		int e = -conn->getLastRecvError();
		conn = nullptr;
		return e;
	}

	responseHeaders = v;

	curStatus = v["_status"].getUInt();
	StrViewA te = v["Transfer-Encoding"].getString();
	if (te == "chunked") {
		responseData = new ChunkedStream(InputStream(conn));
	} else {
		json::Value ctv = v["Content-Length"];
		if (ctv.defined()) {
			std::size_t limit = ctv.getUInt();
			responseData = new LimitedStream(InputStream(conn), limit);
		} else {
			responseData = static_cast<IInputStream *>(conn);
			conn = nullptr;
		}
	}

	if (v["Connection"].getString() == "close") {
		conn = nullptr;
	}

	return curStatus;

}

bool HttpClient::everythingRead(IInputStream* stream) {
	json::BinaryView b = stream->read(0);
	return b.empty();
}

void HttpClient::connectTarget() {
	conn = NetworkConnection::connect(curTarget,80);
}

json::String HttpClient::crackURL(StrViewA urlWithoutProtocol) {
	json::String newTarget;
	std::size_t p1 = urlWithoutProtocol.indexOf("/",0);
	if (p1 != urlWithoutProtocol.npos) {
		StrViewA adom = urlWithoutProtocol.substr(0,p1);
		StrViewA path = urlWithoutProtocol.substr(p1);
		std::size_t p2 = adom.indexOf("@",0);
		if (p2 != adom.npos) {
			auth = adom.substr(0,p2);
			newTarget = adom.substr(p2+1);
		} else {
			auth = json::String();
			newTarget = adom;
		}
		curPath = path;
	}
	return newTarget;
}

void HttpClient::close() {
	discardResponse();
}

void HttpClient::abort() {
	conn = nullptr;
}

int HttpClient::getStatus() {
	return responseHeaders["_status"].getUInt();
}

json::String HttpClient::getStatusMessage() {
	return String(responseHeaders["_message"]);
}

json::String HttpClient::custromPotocol(StrViewA) {
	return json::String();
}

void HttpClient::setCancelFunction(const CancelFunction& cancelFn) {
	cancelFunction = cancelFn;
	if (conn != nullptr) {
		conn->setCancelFunction(cancelFn);
	}
}

CancelFunction HttpClient::initCancelFunction() {
	return NetworkConnection::createCancelFunction();
}


void HttpClient::setTimeout(std::uintptr_t timeoutInMS) {
	curTimeout = timeoutInMS;
	if (conn!= nullptr) {
		conn->setTimeout(timeoutInMS);
	}
}
void HttpClient::initConnection() {
	conn->setTimeout(curTimeout);
	conn->setCancelFunction(cancelFunction);
}

}
