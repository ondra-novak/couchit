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
#include <immujson/json.h>
#include <lightspeed/base/types.h>
#include <netdb.h>
#include <sys/socket.h>
#include "../json.h"
namespace LightCouch {


NetworkConnection* LightCouch::NetworkConnection::connect(const StrView &addr_ddot_port, int defaultPort) {

	using namespace LightSpeed;

	struct addrinfo req;
	memset(&req,0,sizeof(req));
	req.ai_family = AF_UNSPEC;
	req.ai_socktype = SOCK_STREAM;

	struct addrinfo *res;


	natural pos = addr_ddot_port.findLast(':');
	if (pos != naturalNull) {

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

	int socket = ::socket(res->ai_family,res->ai_socktype,res->ai_family);
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
	,lastSendError(0)
	,lastRecvError(0)
	,timeout(false)
	,timeoutTime(30000)
{
	setNonBlock();

}

unsigned char* LightCouch::NetworkConnection::read(std::size_t processed, std::size_t* readready) {
	if (lastRecvError != 0) {
		return 0;
	}
	std::size_t remain = buffUsed - rdPos ;
	if (remain <= processed) rdPos = buffUsed = 0;
	else rdPos+=processed;

	if (rdPos < buffUsed) {

		if (readready) *readready = buffUsed - rdPos;
		return inputBuff+rdPos;

	} else {

		if (readready) {
			do {
				int rc = recv(socket,inputBuff,3000,0);
				if (rc == 0) {
					*readready = 0;
					return 0;
				} else if (rc < 0) {
					int err = errno;
					if (err == EAGAIN || err == EWOULDBLOCK) {
						if (!waitForInputInternal(timeoutTime)) {
							timeout = true;
							*readready = 0;
							return 0;
						}
					} else if (err != EINTR) {
						lastRecvError = err;
						*readready = 0;
						return 0;
					}
				} else {
					buffUsed = rc;
					rdPos = 0;
					*readready = rc;
					return inputBuff;
				}
			} while(true);
		} else if (processed) {
			return 0;
		} else {
			int rc = recv(socket,inputBuff,3000,0);
			if (rc >= 0) {
				buffUsed = rc;
				rdPos = 0;
				return inputBuff;
			} else {
				int err = errno;
				if (err != EWOULDBLOCK || err != EINTR || err != EAGAIN)
					lastRecvError = err;
				return 0;
			}
		}

	}



}

bool NetworkConnection::waitForOutput(int  timeout_in_ms) {
	struct pollfd poll_list[1];
	poll_list[0].fd = socket;
	poll_list[0].events = POLLOUT;
	do {

		int r = poll(poll_list,1,timeout_in_ms);
		if (r < 0) {
			int err = errno;
			if (err != EINTR) {
				lastRecvError = err;
				return false;
			}
		} else if (r == 0) {
			return false;
		} else {
			return true;
		}

	} while (true);
}

void LightCouch::NetworkConnection::write(const unsigned char* buffer, std::size_t size) {
	if (lastSendError) return;
	do {
		int sent = send(socket,buffer,size,0);
		if (sent < 0) {
			lastSendError = errno;
			break;
		} else {
			buffer+=sent;
			size-=sent;
		}
	} while (size);
}

bool NetworkConnection::waitForInputInternal(int timeout_in_ms) {
	struct pollfd poll_list[1];
	poll_list[0].fd = socket;
	poll_list[0].events = POLLIN|POLLPRI|POLLRDHUP;
	do {

		int r = poll(poll_list,1,timeout_in_ms);
		if (r < 0) {
			int err = errno;
			if (err != EINTR) {
				lastRecvError = err;
				return false;
			}
		} else if (r == 0) {
			return false;
		} else {
			return true;
		}

	} while (true);
}

void NetworkConnection::setNonBlock() {
	fcntl(socket, F_SETFL, fcntl(socket, F_GETFL, 0) | O_NONBLOCK);
}

}

