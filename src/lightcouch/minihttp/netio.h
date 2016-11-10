/*
 * netio.h
 *
 *  Created on: 10. 11. 2016
 *      Author: ondra
 */

#ifndef LIGHTCOUCH_MINIHTTP_NETIO_H_
#define LIGHTCOUCH_MINIHTTP_NETIO_H_

#include <immujson/refcnt.h>
#include "abstractio.h"



namespace LightCouch {

class StrView;

class NetworkConnection: public IInputStream, public IOutputStream {
public:

	static NetworkConnection *connect(const StrView &addr_ddot_port, int defaultPort);

	bool waitForInput(int  timeout_in_ms) {
		if (rdPos < buffUsed) return true;
		return waitForInputInternal(timeout_in_ms);
	}
	bool waitForOutput(int  timeout_in_ms);

	int getLastRecvError() const {
		return lastRecvError;
	}

	int getLastSendError() const {
		return lastSendError;
	}

	bool isTimeout() const {
		return timeout;
	}

protected:

	NetworkConnection(int socket);
	virtual unsigned char *read(std::size_t processed, std::size_t *readready);
	virtual void write(const unsigned char *buffer, std::size_t size);

	bool waitForInputInternal(int timeout_in_ms);
	void setNonBlock();

	int socket;

	unsigned char inputBuff[3000];
	std::size_t buffUsed;
	std::size_t rdPos;
	int lastSendError;
	int lastRecvError;
	bool timeout;
	int timeoutTime;
};

typedef json::RefCntPtr<NetworkConnection> PNetworkConection;




}


#endif /* LIGHTCOUCH_MINIHTTP_NETIO_H_ */
