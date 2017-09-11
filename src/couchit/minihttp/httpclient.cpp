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

	class ChunkedStream: public AbstractOutputStream {
	public:

		ChunkedStream(const PNetworkConection &stream):stream(stream),wr(OutputStream(stream)) {}
		virtual json::BinaryView write(const json::BinaryView &data, bool) {
			wr(data);
			return json::BinaryView(0,0);
		}
		~ChunkedStream() {
			wr(nullptr);
		}
		virtual void closeOutput() {
			wr(nullptr);
		}
		virtual bool waitWrite(int tm) {
			return stream->waitWrite(tm);
		}

	protected:
		const PNetworkConection &stream;
		ChunkedWrite<OutputStream> wr;

	};

	class ErrorStream: public AbstractOutputStream {
	public:
		virtual json::BinaryView write(const json::BinaryView &, bool ) {
			return json::BinaryView(0,0);
		}
		virtual void closeOutput() {}
		virtual bool waitWrite(int) {return true;}

	};

	if (curTarget.empty()) {
		return OutputStream(new ErrorStream);
	}else {
		initRequest(true,-1);
		if (handleSendError()) {
			return OutputStream(new ErrorStream);
		} else {
			return OutputStream(new ChunkedStream(conn));
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
		stream(json::BinaryView(reinterpret_cast<const unsigned char *>(body), body_length));
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

	class EmptyStream: public AbstractInputStream {
	public:
		virtual json::BinaryView doRead(bool) {
			return json::BinaryView(0,0);
		}
		virtual bool doWait(int) {
			return true;
		}
		virtual void closeInput() {}
	};
	if (responseData == nullptr) {
		return new EmptyStream;
	} else {
		return static_cast<AbstractInputStream *>(responseData);
	}
}

bool couchit::HttpClient::waitForData(int timeout) {
	return conn->waitRead(timeout);
}

void couchit::HttpClient::discardResponse() {
	if (responseData != nullptr) {
		std::size_t p = 0;
		BinaryView b = responseData->read();
		while (!b.empty()) {
			responseData->commit(b.length);
			b = responseData->read();
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

	class ChunkedStream: public AbstractInputStream {
	public:
		ChunkedStream(const InputStream &stream, PNetworkConection conn):chread(stream),conn(conn) {}
		virtual json::BinaryView doRead(bool) {
			chread(commitSize);
			auto x = chread(0);
			commitSize = x.length;
			return x;
		}

		virtual bool doWait(int ms) {
			return conn->waitRead(ms);
		}

		virtual void closeInput() {
			conn->closeInput();
		}


	protected:
		ChunkedRead<InputStream> chread;
		PNetworkConection conn;
		std::size_t commitSize = 0;
	};

	class LimitedStream: public AbstractInputStream {
	public:
		LimitedStream(const InputStream &stream, std::size_t limit, PNetworkConection conn)
			:chread(stream),conn(conn) {
				chread.setLimit(limit);
		}
		virtual json::BinaryView doRead(bool) {
			chread(commitSize);
			auto x = chread(0);
			commitSize = x.length;
			return x;
		}

		virtual bool doWait(int ms) {
			return conn->waitRead(ms);
		}
		virtual void closeInput() {
			return conn->closeInput();
		}


	protected:
		BufferedReadWLimit<InputStream> chread;
		std::size_t commitSize = 0;
		PNetworkConection conn;
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
		responseData = new ChunkedStream(InputStream(conn),conn);
	} else {
		json::Value ctv = v["Content-Length"];
		if (ctv.defined()) {
			std::size_t limit = ctv.getUInt();
			responseData = new LimitedStream(InputStream(conn), limit,conn);
		} else {
			responseData = static_cast<AbstractInputStream *>(conn);
			conn = nullptr;
		}
	}

	if (v["Connection"].getString() == "close") {
		conn = nullptr;
	}

	return curStatus;

}

bool HttpClient::everythingRead(AbstractInputStream* stream) {
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
	if (conn != nullptr)
		discardResponse();
}

void HttpClient::abort() {
	if (conn != nullptr) {
		conn->close();
	}
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
