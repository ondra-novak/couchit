/*
 * netio.cpp
 *
 *  Created on: 10. 11. 2016
 *      Author: ondra
 */

#define WIN32_LEAN_AND_MEAN
#include <windows.h>
#include <WinSock2.h>
#include <Ws2tcpip.h>
#include <atomic>

#undef min
#undef max

#pragma comment (lib,"ws2_32.lib")

#include <cstring>
#include "netio.h"

#include <imtjson/string.h>

namespace couchit {

class LinuxCancel: public ICancelWait {
public:

	LinuxCancel() {
		hStopEv = CreateEvent(0, 0, 0, 0);
	}

	~LinuxCancel() {
		CloseHandle(hStopEv);
	}

	HANDLE getStopHandle() const {
		return hStopEv;
	}

	void clear() {
		ResetEvent(hStopEv);
	}

	void cancelWait() {
		SetEvent(hStopEv);
	}



protected:
	HANDLE hStopEv;

};

class WSAInit : public WSADATA {
public:
	WSAInit() {
		WSAStartup(MAKEWORD(2, 2), this);
	}
	~WSAInit() {
		WSACleanup();
	}

};

class Async {
public:
	static Async &get(void *waitHandle) {
		return *reinterpret_cast<Async *>(waitHandle);
	}

	Async();
	WSAOVERLAPPED rdovr;
	WSAOVERLAPPED wrovr;
	DWORD rcount;
	DWORD wcount;
	DWORD rerror;
	DWORD werror;
	bool readPending;
	bool eof;
	bool writePending;
	std::atomic<unsigned int> refs;

	void readToBuffer(SOCKET s, unsigned char *buffer, unsigned int length);
	bool waitForPending(bool &state, unsigned int timeout, ICancelWait *cancel);
	void writeBuffer(SOCKET s, const unsigned char *buffer, unsigned int length);
	bool waitForWrite(unsigned int timeout, ICancelWait *cancel);
	bool hasData() const {
		return eof || rcount;
	}

};


NetworkConnection* couchit::NetworkConnection::connect(const StrViewA &addr_ddot_port, int defaultPort) {

	static WSAInit init;
	struct addrinfo req;
	std::memset(&req,0,sizeof(req));
	req.ai_family = AF_UNSPEC;
	req.ai_socktype = SOCK_STREAM;

	struct addrinfo *res;


	std::size_t pos = addr_ddot_port.lastIndexOf(":");
	if (pos != ((std::size_t)-1)) {

		json::String host = addr_ddot_port.substr(0,pos);
		json::String service = addr_ddot_port.substr(pos+1);


		if (host == "localhost") 
			req.ai_family = AF_INET;

		if (getaddrinfo(host.c_str(),service.c_str(),&req,&res) != 0)
			return 0;
	} else {
		char buff[100];
		snprintf(buff,99,"%d",defaultPort);
		json::String host = addr_ddot_port;

		if (host == "localhost") 
			req.ai_family = AF_INET;

		if (getaddrinfo(host.c_str(),buff,&req,&res) != 0)
			return 0;
	}

	int socket = ::WSASocket(res->ai_family,res->ai_socktype,res->ai_protocol,0,0, WSA_FLAG_OVERLAPPED);
	if (socket == INVALID_SOCKET) {
		freeaddrinfo(res);
		return 0;
	}

	int c = ::connect(socket,res->ai_addr, res->ai_addrlen);
	if (c == -1) {
		freeaddrinfo(res);
		return 0;
	}

	freeaddrinfo(res);
	return new NetworkConnection(socket);
}

couchit::NetworkConnection::NetworkConnection(int socket)
	:socket(socket)
	,buffUsed(0)
	,rdPos(0)
	,eofFound(false)
	,lastSendError(0)
	,lastRecvError(0)
	,timeout(false)
	,timeoutTime(30000)
{
	setNonBlock();
	waitHandle = new Async;

}

NetworkConnection::~NetworkConnection() {
	::closesocket(socket);	
	Async &a = Async::get(waitHandle);
	while (a.readPending || a.writePending) {
		SleepEx(0, TRUE);
	}

}

ICancelWait* NetworkConnection::createCancelFunction() {
	return new LinuxCancel;
}

void NetworkConnection::setCancelFunction(const CancelFunction& fn) {
	this->cancelFunction = fn;
}

void NetworkConnection::setTimeout(std::uintptr_t timeout) {
	timeoutTime = timeout;
}

int NetworkConnection::readToBuff() {
	return -1;
}

const unsigned char* NetworkConnection::read(std::size_t processed, std::size_t* readready) {
	//calculate remain space
	std::size_t remain = buffUsed - rdPos;
	//if processed remain space, reset position and used counters
	if (remain <= processed) rdPos = buffUsed = 0;
	//otherwise advance the position
	else rdPos += processed;

	//caller request the bytes
	if (readready) {
		//if there are bytes or eof
		if (rdPos < buffUsed || eofFound) {
			//set available ready bytes
			*readready = buffUsed - rdPos;
			//return pointer to buffer
			return inputBuff + rdPos;
		}
		else {

			Async &async = Async::get(waitHandle);

			if (async.rcount == (DWORD)-1 && !async.readPending) {
				async.readToBuffer(socket, this->inputBuff, sizeof(this->inputBuff));
			}
			
			if (!async.waitForPending(async.readPending, timeoutTime, cancelFunction)) {
				timeout = true;
				return 0;
			}

			if (async.rerror) {
				lastRecvError = async.rerror;
				*readready = 0;
				return 0;
			}

			rdPos = 0;
			buffUsed = async.rcount;
			async.rcount = (DWORD)-1;
			*readready = buffUsed;
			eofFound = buffUsed == 0;
		}
	}
	else {
		if (rdPos < buffUsed || eofFound) {
			return inputBuff + rdPos;
		}
		else {
			Async &async = Async::get(waitHandle);

			if (async.rcount == (DWORD)-1 && !async.readPending) {
				async.readToBuffer(socket, this->inputBuff, sizeof(this->inputBuff));
			}

			if (async.readPending) return 0;
			if (async.rerror) {
				lastRecvError = async.rerror;
				return 0;
			}

			rdPos = 0;
			buffUsed = async.rcount;
			async.rcount = (DWORD)-1;
			eofFound = buffUsed == 0;
		}
	}
	return inputBuff;
}

bool NetworkConnection::waitForOutput(int  timeout_in_ms) {
	Async &async = Async::get(waitHandle);
	return async.waitForPending(async.writePending, timeout_in_ms, cancelFunction);
}

bool couchit::NetworkConnection::write(const unsigned char* buffer, std::size_t size, std::size_t *written) {
	if (lastSendError) return false;
	if (size == 0) {
		if (written) *written = 0;
		return true;
	}
	if (written) {
		*written = 0;
		Async &async = Async::get(waitHandle);
		async.writeBuffer(socket, buffer, size);
		if (!async.waitForPending(async.writePending, timeoutTime, cancelFunction)) {
			timeout = true;
			return false;
		}
		if (async.werror) {
			lastSendError = async.werror;
			return false;
		}
		*written = async.wcount;
		return true;
	}
	else {
		do {
			std::size_t written;
			if (!write(buffer, size, &written)) return false;
			buffer += written;
			size -= written;
			if (written == 0) {
				timeout = true;
				return false;
			}
		} while (size);
		timeout = false;
		return true;
	}
}

json::BinaryView NetworkConnection::read(std::size_t processed)
{
	using namespace json;


	//calculate remain space
	std::size_t remain = buffUsed - rdPos;
	//if processed remain space, reset position and used counters
	if (remain <= processed) rdPos = buffUsed = 0;
	//otherwise advance the position
	else rdPos += processed;

	//if there are still bytes or we recently processed some
	if (rdPos < buffUsed) {
		//return rest of buffer, even if there is no buffer at all
		return BinaryView(inputBuff + rdPos, buffUsed - rdPos);
	}
	else if (lastRecvError) {
		return BinaryView(nullptr, 0);
	}
	else if (processed) {
		//read without blocking
		Async &async = Async::get(waitHandle);
		//check whether pending
		if (async.waitForPending(async.readPending, 0, 0)) {
			//if not, test whether data
			if (async.hasData()) {
				buffUsed = async.rcount;
				eofFound = async.eof;
				lastRecvError = async.rerror;
				async.rcount = 0;
				//return data
				return BinaryView(inputBuff, buffUsed);
			}
			//no data ready, reade some
			async.readToBuffer(socket, this->inputBuff, sizeof(this->inputBuff));
		}
		//in all cases, return empty buffer
		return BinaryView(nullptr,0);
	}
	else {
		Async &async = Async::get(waitHandle);
		while (async.waitForPending(async.readPending, timeoutTime, cancelFunction)) {
			if (async.hasData()) {
				buffUsed = async.rcount;
				eofFound = async.eof;
				lastRecvError = async.rerror;
				async.rcount = 0;
				return BinaryView(inputBuff, buffUsed);
			}
			async.readToBuffer(socket, this->inputBuff, sizeof(this->inputBuff));
		}
		timeout = true;
		return BinaryView(nullptr,0);
	}
	
	

}

bool NetworkConnection::waitForInputInternal(int timeout_in_ms) {
	Async &async = Async::get(waitHandle);
	return async.waitForPending(async.readPending, timeout_in_ms, cancelFunction);
}

void NetworkConnection::setNonBlock() {
	
}

static void CALLBACK readCompletion(IN DWORD dwError, IN DWORD cbTransferred,
	IN LPWSAOVERLAPPED lpOverlapped, IN DWORD dwFlags) {

	Async &async = Async::get(lpOverlapped->hEvent);
	async.rcount = cbTransferred;
	async.rerror = dwError;
	async.readPending = false;
	async.eof = dwError == 0 && cbTransferred == 0;
}
static void CALLBACK writeCompletion(IN DWORD dwError, IN DWORD cbTransferred,
	IN LPWSAOVERLAPPED lpOverlapped, IN DWORD dwFlags) {

	Async &async = Async::get(lpOverlapped->hEvent);
	async.wcount = cbTransferred;
	async.werror = dwError;
	async.writePending = false;
}

Async::Async()
{
	ZeroMemory(&rdovr, sizeof(rdovr));
	ZeroMemory(&wrovr, sizeof(wrovr));
	rdovr.hEvent = (HANDLE)this;
	wrovr.hEvent = (HANDLE)this;
	readPending = false;
	writePending = false;
	eof = false;
}

void Async::readToBuffer(SOCKET s, unsigned char * buffer, unsigned int length)
{	
	WSABUF b;
	b.buf = (char *)buffer;
	b.len = length;	
	readPending = false;
	DWORD flags = 0;
	DWORD res = WSARecv(s, &b, 1, &rcount, &flags, &rdovr, &readCompletion);
	if (res != 0) {
		rcount = 0;
		DWORD err = WSAGetLastError();
		if (err == WSA_IO_PENDING) {
			readPending = true;
		}
		else {
			rerror = err;
			return;
		}
	}
	else {
		rerror = 0;		
		SleepEx(0, TRUE);
	}
}

bool Async::waitForPending(bool & state, unsigned int timeout, ICancelWait * cancel)
{
	while (state) {
		HANDLE h = 0;
		if (cancel) {
			LinuxCancel *c = dynamic_cast<LinuxCancel *>(cancel);
			if (c) {
				h = c->getStopHandle();
			}
		}
		int res;
		if (h) res = WaitForSingleObjectEx(h, timeout, TRUE);
		else res = SleepEx(timeout, TRUE);
		if (res != WAIT_IO_COMPLETION) {
			break;
		}
	}
	return !state;
}


void Async::writeBuffer(SOCKET s, const unsigned char * buffer, unsigned int length)
{
	WSABUF b;
	b.buf = (char *)buffer;
	b.len = length;
	writePending = false;
	DWORD res = WSASend(s, &b, 1, &wcount, 0, &wrovr, &writeCompletion);
	if (res != 0) {
		wcount = 0;
		DWORD err = WSAGetLastError();
		if (err == WSA_IO_PENDING)
			writePending = true;
		else {
			werror = err;
		}
	}
	else {
		werror = 0;
		SleepEx(0, TRUE);
	}
}


}

