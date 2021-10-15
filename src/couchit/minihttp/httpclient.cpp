/*
 * httpclient.cpp
 *
 *  Created on: 13. 11. 2016
 *      Author: ondra
 */

#include "httpclient.h"



#include "chunkstream.h"
#include "hdrrd.h"
#include "hdrwr.h"
namespace couchit {


HttpClient::HttpClient()
	:curTimeout(70000)
{
}

HttpClient::HttpClient(std::string_view userAgent)
	:userAgent(userAgent),curTimeout(70000)
{
}

HttpClient& HttpClient::open(std::string_view url, std::string_view method, bool keepAlive) {
	close();
	this->keepAlive = keepAlive;

	curStatus = 0;
	json::String newTarget;

	if (url.substr(0,7) == "http://") {
		std::string_view u1 = url.substr(7);
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

HttpClient& HttpClient::setHeaders(Value headers) {
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

OutputStream HttpClient::beginBody() {


	class ErrorStream: public AbstractOutputStream {
	public:
		virtual json::BinaryView doWrite(const json::BinaryView &, bool ) override {
			return json::BinaryView(0,0);
		}
		virtual void closeOutput() override {}
		virtual bool doWaitWrite(int) override {return true;}

		virtual Buffer createBuffer() override {
			static unsigned char buff[256];
			return Buffer(buff,sizeof(buff));
		}


	};

	if (curTarget.empty()) {
		return OutputStream(new ErrorStream);
	}else {
		initRequest(true,-1);
		if (handleSendError()) {
			return OutputStream(new ErrorStream);
		} else {
			return OutputStream(new ChunkedOutputStream<>(OutputStream(conn)));
		}
	}
}

int HttpClient::send() {
	if (!headersSent) {
		initRequest(false,0);
	}
	if (handleSendError()) {
		return curStatus;
	}
	return readResponse();
}

int HttpClient::send(const std::string_view& body) {
	return send(body.data(),body.length());
}

int HttpClient::send(const void* body, std::size_t body_length) {
	if (!headersSent) {
		initRequest(true,body_length);
		if (handleSendError()) {
			return curStatus;
		}
		OutputStream stream(conn);
		stream(json::BinaryView(reinterpret_cast<const unsigned char *>(body), body_length));
		stream->commit(0,true);
	}
	if (handleSendError()) {
		return curStatus;
	}
	return readResponse();
}

Value HttpClient::getHeaders() {
	return responseHeaders;
}

InputStream HttpClient::getResponse() {

	class EmptyStream: public AbstractInputStream {
	public:
		virtual json::BinaryView doRead(bool) {
			return json::BinaryView(0,0);
		}
		virtual bool doWaitRead(int) {
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

bool HttpClient::waitForData(int timeout) {
	return conn->waitRead(timeout);
}

void HttpClient::discardResponse() {
	if (responseData != nullptr) {
//		std::size_t p = 0;
		BinaryView b = responseData->read();
		while (!b.empty()) {
			b = responseData->read();
		}
		responseData = nullptr;
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
	hdr.set("_method",reqMethod);
	hdr.set("_uri",curPath);
	hdr.set("_version","HTTP/1.1");
	hdr.set("Host",curTarget);
	hdr.set("User-Agent",userAgent);

	if (haveBody) {
		if (contentLength == std::size_t(-1)) {
			hdr.set("Transfer-Encoding","chunked");
		} else {
			hdr.set("Content-Length",contentLength);
		}
	}

	if (!keepAlive) {
		hdr.set("Connection","close");
	}

	if (!auth.empty()) {
		String authstr((auth.length()+2)*4/3+7,[&](char *c) {
			char *s = c;
			std::string_view basic("Basic ");
			for (auto x: basic) *c++ = x;
			json::base64->encodeBinaryValue(map_str2bin(auth.str()),[&](std::string_view z){
				for (auto x: z) *c++=x;
			});
			return c - s;
		});
		hdr.set("Authorization", authstr);
	}

	OutputStream stream(conn);

	HeaderWrite<OutputStream> hdrwr(stream);

	hdrwr.serialize(hdr);

	stream->commit(0,true);

	headersSent = true;
}

int HttpClient::readResponse() {

	class LimitedStream: public AbstractInputStream {
	public:
		LimitedStream(const InputStream &stream, std::size_t limit)
			:stream(stream), limit(limit) {}

		~LimitedStream() {
		}

		virtual void closeInput() {
			stream->closeInput();
		}

	protected:

		virtual json::BinaryView doRead(bool nonblock = false) {
			if (limit == 0) return eofConst;

			auto x =  stream->read(nonblock);
			auto l = std::min(limit, x.length());
			auto rest = x.substr(l);
			x = x.substr(0,l);
			limit = limit-x.length();
			stream->putBack(rest);

			return x;
		}
		virtual bool doWaitRead(int milisecs) {
			return stream->waitRead(milisecs);
		}


	protected:
		InputStream stream;
		std::size_t limit;
		std::size_t commitSize = 0;
	};

	PNetworkConection conn = this->conn;

	if (conn == nullptr) {
		return curStatus;
	}


	InputStream stream(conn);

	HeaderRead<InputStream> hdrrd(stream);

	json::Value v = hdrrd.parseHeaders();
	if (!v.defined()) {
		int e = -conn->getLastRecvError();
		conn = nullptr;
		return e;
	}

	responseHeaders = v;

	curStatus = v["_status"].getUInt();
	std::string_view te = v["Transfer-Encoding"].getString();
	if (te == "chunked") {
		responseData = new ChunkedInputStream(InputStream(conn));
	} else {
		json::Value ctv = v["Content-Length"];
		if (ctv.defined()) {
			std::size_t limit = ctv.getUInt();
			responseData = new LimitedStream(InputStream(conn), limit);
		} else {
			responseData = static_cast<AbstractInputStream *>(conn);
			this->conn = nullptr;
		}
	}

	if (v["Connection"].getString() == "close") {
		this->keepAlive = false;
	}

	return curStatus;

}

bool HttpClient::everythingRead(AbstractInputStream* stream) {
	json::BinaryView b = stream->read(0);
	return b.empty();
}

void HttpClient::connect() {
	if (conn == nullptr) {
		connectTarget();
	}
}

void HttpClient::connectTarget() {
	conn = NetworkConnection::connect(curTarget,80);
}

json::String HttpClient::crackURL(std::string_view urlWithoutProtocol) {
	json::String newTarget;
	std::size_t p1 = urlWithoutProtocol.find('/');
	if (p1 != urlWithoutProtocol.npos) {
		std::string_view adom = urlWithoutProtocol.substr(0,p1);
		std::string_view path = urlWithoutProtocol.substr(p1);
		std::size_t p2 = adom.find('@');
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
	if (conn != nullptr) {
		if (keepAlive) {
			discardResponse();
		} else {
			conn = nullptr;
		}
	}
}

void HttpClient::abort() {
	PNetworkConection c (conn);
	keepAlive = false;

	if (c != nullptr) {
		c->close();
		conn = nullptr;
	}

}

int HttpClient::getStatus() {
	return responseHeaders["_status"].getUInt();
}

json::String HttpClient::getStatusMessage() {
	return String(responseHeaders["_message"]);
}

json::String HttpClient::custromPotocol(std::string_view) {
	return json::String();
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
