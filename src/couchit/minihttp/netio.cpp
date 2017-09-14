/*
 * netio.cpp
 *
 *  Created on: 10. 11. 2016
 *      Author: ondra
 */


#include <cstring>
#include <poll.h>
#include "netio.h"

#include <errno.h>
#include <fcntl.h>
#include <imtjson/json.h>
#include <netdb.h>
#include <sys/socket.h>
#include <unistd.h>
#include "../exception.h"

#include <netinet/tcp.h>

#include "../json.h"

namespace couchit {

class LinuxCancel: public ICancelWait {
public:

	LinuxCancel() {
		int fds[2];
		int c = pipe2(fds,O_NONBLOCK|O_CLOEXEC);
		if (c == -1) {
			int e = errno;
			throw SystemException("Unable to create pipe2", e);
		}
		fdrecv = fds[0];
		fdsend = fds[1];
	}

	~LinuxCancel() {
		close(fdsend);
		close(fdrecv);
	}

	int getRecvFd() const {
		return fdrecv;
	}

	void clear() {
		char b;
		int res;
		do {
			res = ::read(fdrecv,&b,1);
		} while (res == 1);
	}

	void cancelWait() {
		unsigned char b = 1;
		int x = ::write(fdsend,&b,1);
		if (x == -1) {
			int e = errno;
			throw SystemException("Failed to trigger event", e);
		}
	}



protected:
	int fdsend;
	int fdrecv;

};


NetworkConnection* couchit::NetworkConnection::connect(const StrViewA &addr_ddot_port, int defaultPort) {


	struct addrinfo req;
	std::memset(&req,0,sizeof(req));
	req.ai_family = AF_INET;
	req.ai_socktype = SOCK_STREAM;

	struct addrinfo *res;


	std::size_t pos = addr_ddot_port.lastIndexOf(":");
	if (pos != ((std::size_t)-1)) {

		json::String host = addr_ddot_port.substr(0,pos);
		json::String service = addr_ddot_port.substr(pos+1);


		if (getaddrinfo(host.c_str(),service.c_str(),&req,&res) != 0)
			return 0;
	} else {
		char buff[100];
		snprintf(buff,99,"%d",defaultPort);
		json::String host = addr_ddot_port;

		if (getaddrinfo(host.c_str(),buff,&req,&res) != 0)
			return 0;
	}

	int socket = ::socket(res->ai_family,res->ai_socktype|SOCK_CLOEXEC,res->ai_protocol);

	if (socket == -1) {
		freeaddrinfo(res);
		return 0;
	}

	int c = ::connect(socket,res->ai_addr, res->ai_addrlen);
	if (c == -1) {
		freeaddrinfo(res);
		::close(socket);
		return 0;
	}

	freeaddrinfo(res);
	return new NetworkConnection(socket);
}

couchit::NetworkConnection::NetworkConnection(int socket)
	:socket(socket)
	,eofFound(false)
	,lastSendError(0)
	,lastRecvError(0)
	,timeout(false)
	,timeoutTime(30000)
{
	setNonBlock();
	disableNagle();

}

NetworkConnection::~NetworkConnection() {
	::close(socket);
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

json::BinaryView NetworkConnection::doRead(bool nonblock) {
	int rc = recv(socket,inputBuff,3000,0);
	if (rc > 0) {
//		std::cout << "Read: " << StrViewA(BinaryView(inputBuff,rc)) << std::endl;
		return json::BinaryView(inputBuff,rc);
	} else if (rc == 0) {
		eofFound = true;
		return AbstractInputStream::eofConst;
	} else {
		int err = errno;
		if (err != EWOULDBLOCK && err != EINTR && err != EAGAIN) {
			lastRecvError = err;
			eofFound = true;
			return AbstractInputStream::eofConst;
		} else if (nonblock) {
			return BinaryView(0,0);
		} else {
			if (doWaitRead(timeoutTime) == false) {
				timeout = true;
				eofFound = true;
				return AbstractInputStream::eofConst;
			}
			return doRead(nonblock);
		}
	}

}


bool NetworkConnection::doWaitWrite(int  timeout_in_ms) {
	struct pollfd poll_list[2];
	int cnt = 1;
	poll_list[0].fd = socket;
	poll_list[0].events = POLLOUT;
	LinuxCancel *lc = dynamic_cast<LinuxCancel *>(static_cast<ICancelWait *>(cancelFunction));
	if (lc != nullptr) {
		poll_list[1].fd = lc->getRecvFd();
		poll_list[1].events = POLLIN;
		cnt++;
	}
	do {

		int r = poll(poll_list,cnt,timeout_in_ms);
		if (r < 0) {
			int err = errno;
			if (err != EINTR) {
				lastRecvError = err;
				return false;
			}
		} else if (r == 0) {
			return false;
		} else {
			if (poll_list[0].revents & POLLOUT) {
				return true;
			} else {
				if (lc != nullptr) lc->clear();
				return false;
			}
		}

	} while (true);
}

json::BinaryView NetworkConnection::doWrite(const json::BinaryView &data, bool nonblock) {
	if (data.empty()) return data;
	if (lastSendError || timeout) return json::BinaryView(0,0);
	int sent = send(socket, data.data, data.length,0);
	if (sent < 0) {
		int err = errno;
		if (err != EWOULDBLOCK && err != EINTR && err != EAGAIN) {
			lastSendError = err;
			return json::BinaryView(0,0);
		}
		sent = 0;
	}
//	std::cout << "Write: " << StrViewA(BinaryView(data.data,sent)) << std::endl;

	if (nonblock || sent == data.length) {
		return data.substr(sent);
	}
	else {
		if (doWaitWrite(timeoutTime) == false) {
			timeout = true;
			return json::BinaryView(0,0);
		}
		return doWrite(data.substr(sent), nonblock);
	}
}

bool NetworkConnection::doWaitRead(int timeout_in_ms) {
	struct pollfd poll_list[2];
	poll_list[0].fd = socket;
	poll_list[0].events = POLLIN|POLLRDHUP;
	poll_list[0].revents = 0;
	int cnt = 1;
	LinuxCancel *lc = dynamic_cast<LinuxCancel *>(static_cast<ICancelWait *>(cancelFunction));
	if (lc != nullptr) {
		poll_list[1].fd = lc->getRecvFd();
		poll_list[1].events = POLLIN;
		poll_list[1].revents = 0;
		cnt++;
	}
	do {
		int r = poll(poll_list,cnt,timeout_in_ms);
		if (r < 0) {
			int err = errno;
			if (err != EINTR && err != EAGAIN && err != EWOULDBLOCK) {
				lastRecvError = err;
				return false;
			}
		} else if (r == 0) {
			return false;
		} else {
			if (poll_list[0].revents & (POLLIN|POLLRDHUP)) {
				return true;
			} else {
				if (lc != nullptr) lc->clear();
				return false;
			}
		}

	} while (true);
}

void NetworkConnection::setNonBlock() {
	if (fcntl(socket, F_SETFL, fcntl(socket, F_GETFL, 0) | O_NONBLOCK | O_CLOEXEC) != 0) {
		 int e = errno;
		 throw SystemException("Unable to setup socket (O_NONBLOCK)", e);
	}
}

void NetworkConnection::disableNagle() {
	int flag = 1;
	int result = setsockopt(socket,            /* socket affected */
	                        IPPROTO_TCP,     /* set option at TCP level */
	                        TCP_NODELAY,     /* name of option */
	                        (char *) &flag,  /* the cast is historical cruft */
	                        sizeof(int));    /* length of option value */
	 if (result < 0) {
		 int e = errno;
		 throw SystemException("Unable to setup socket (TCP_NODELA)", e);
	 }

}

void NetworkConnection::closeOutput() {
	shutdown(socket, SHUT_WR);
}
void NetworkConnection::closeInput() {
	shutdown(socket, SHUT_RD);
}
void NetworkConnection::close() {
	shutdown(socket, SHUT_RDWR);
}

NetworkConnection::Buffer NetworkConnection::createBuffer() {
	return Buffer(outputBuff,sizeof(outputBuff));
}



}

