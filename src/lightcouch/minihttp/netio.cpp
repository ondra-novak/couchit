/*
 * netio.cpp
 *
 *  Created on: 10. 11. 2016
 *      Author: ondra
 */


#include <poll.h>
#include "netio.h"

#include <errno.h>
#include <fcntl.h>
#include <imtjson/json.h>
#include <lightspeed/base/types.h>
#include <netdb.h>
#include <sys/socket.h>
#include <unistd.h>

#include "../json.h"

using LightSpeed::atomic;
namespace LightCouch {

class LinuxCancel: public ICancelWait {
public:

	LinuxCancel() {
		int fds[2];
		pipe2(fds,O_NONBLOCK|O_CLOEXEC);
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
		(void)::write(fdsend,&b,1);
	}



protected:
	int fdsend;
	int fdrecv;

};

NetworkConnection* LightCouch::NetworkConnection::connect(const StrViewA &addr_ddot_port, int defaultPort) {

	using namespace LightSpeed;

	struct addrinfo req;
	memset(&req,0,sizeof(req));
	req.ai_family = AF_UNSPEC;
	req.ai_socktype = SOCK_STREAM;

	struct addrinfo *res;


	std::size_t pos = addr_ddot_port.findLast(':');
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

	int socket = ::socket(res->ai_family,res->ai_socktype,res->ai_protocol);
	if (socket == -1) {
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

LightCouch::NetworkConnection::NetworkConnection(int socket)
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

int NetworkConnection::readToBuff() {
	int rc = recv(socket,inputBuff,3000,0);
	if (rc > 0) {
		buffUsed = rc;
		rdPos = 0;
		eofFound = false;
		return 1;
	} else if (rc == 0) {
		eofFound = true;
		return 1;
	} else {
		int err = errno;
		if (err != EWOULDBLOCK && err != EINTR && err != EAGAIN) {
			lastRecvError = err;
			eofFound = true;
			return -1;
		} else {
			return 0;
		}


	}

}

const unsigned char* NetworkConnection::read(std::size_t processed, std::size_t* readready) {
	//calculate remain space
	std::size_t remain = buffUsed - rdPos ;
	//if processed remain space, reset position and used counters
	if (remain <= processed) rdPos = buffUsed = 0;
	//otherwise advance the position
	else rdPos+=processed;

	//caller request the bytes
	if (readready) {
		//if there are bytes or eof
		if (rdPos < buffUsed || eofFound) {
			//set available ready bytes
			*readready = buffUsed - rdPos;
			//return pointer to buffer
			return inputBuff+rdPos;
		} else {
			int x;
			//no more bytes available - repeatedly read to the buffer
			while ((x = readToBuff()) == 0) {
				//when it fails, perform waiting
				if (!waitForInput(timeoutTime)) {
					//in case of timeout report error
					timeout = true;
					return 0;
				}
			}
			if (x < 0) {
				//error - set readready to 0
				*readready = 0;
				//return nullptr
				return 0;
			} else {
				//set readready (in case of error or eof it will be set to zero
				*readready = buffUsed;
				//return pointer to buffer
				return inputBuff;
			}
		}
	} else {
		if (rdPos < buffUsed || eofFound) {
			return inputBuff+rdPos;
		} else {
			int x = readToBuff();
			if (x<=0) return 0;
			else return inputBuff;
		}
	}


}

bool NetworkConnection::waitForOutput(int  timeout_in_ms) {
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

bool LightCouch::NetworkConnection::write(const unsigned char* buffer, std::size_t size, std::size_t *written) {
	if (lastSendError) return false;
	if (written) {
		int sent = send(socket,buffer,size,0);
		if (sent < 0) {
			int err = errno;
			if (err != EWOULDBLOCK || err != EINTR || err != EAGAIN) {
				lastSendError = err;
				return false;
			}
			*written = 0;
		} else {
			*written = sent;
		}
		return true;
	} else {
		do {
			std::size_t written;
			if (!write(buffer,size,&written)) return false;
			buffer+=written;
			size-=written;
			if (size && !waitForOutput(timeoutTime)) {
				timeout = true;
				return false;
			}
		} while (size);
		timeout = false;
		return true;
	}
}

bool NetworkConnection::waitForInputInternal(int timeout_in_ms) {
	struct pollfd poll_list[2];
	poll_list[0].fd = socket;
	poll_list[0].events = POLLIN|POLLRDHUP;
	int cnt = 1;
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
	fcntl(socket, F_SETFL, fcntl(socket, F_GETFL, 0) | O_NONBLOCK);
}



}

