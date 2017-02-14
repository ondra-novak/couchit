/*
 * netio.h
 *
 *  Created on: 10. 11. 2016
 *      Author: ondra
 */

#ifndef LIGHTCOUCH_MINIHTTP_NETIO_H_
#define LIGHTCOUCH_MINIHTTP_NETIO_H_

#include <imtjson/refcnt.h>
#include <imtjson/stringview.h>
#include <stdint.h>

#include "abstractio.h"
#include "cancelFunction.h"


namespace couchit {


using json::StrViewA;


class NetworkConnection: public IInputStream, public IOutputStream {
public:

	static NetworkConnection *connect(const StrViewA &addr_ddot_port, int defaultPort);

	static ICancelWait *createCancelFunction();


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

	bool hasErrors() const {
		return timeout||lastRecvError||lastSendError;
	}


	void setCancelFunction(const CancelFunction &fn);

	void setTimeout(uintptr_t timeout);

	~NetworkConnection();



protected:

	NetworkConnection(int socket);
	virtual const unsigned char *read(std::size_t processed, std::size_t *readready);
	virtual bool write(const unsigned char *buffer, std::size_t size, std::size_t *written);

	bool waitForInputInternal(int timeout_in_ms);
	void setNonBlock();

	int socket;
	
	void *waitHandle; //<used by some platforms (Windows)

	unsigned char inputBuff[3000];
	std::size_t buffUsed;
	std::size_t rdPos;
	bool eofFound;
	int lastSendError;
	int lastRecvError;
	bool timeout;
	uintptr_t timeoutTime;

	CancelFunction cancelFunction;

	int readToBuff();

};

typedef json::RefCntPtr<NetworkConnection> PNetworkConection;




}


#endif /* LIGHTCOUCH_MINIHTTP_NETIO_H_ */
