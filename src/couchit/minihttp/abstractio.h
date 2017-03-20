/*
 * abstractio.h
 *
 *  Created on: Nov 10, 2016
 *      Author: ondra
 */

#ifndef LIGHTCOUCH_MINIHTTP_ABSTRACTIO_H_
#define LIGHTCOUCH_MINIHTTP_ABSTRACTIO_H_

#include <imtjson/refcnt.h>

#pragma once

namespace couchit {

class IInputStream: public virtual json::RefCntObj {
public:


	virtual json::BinaryView read(std::size_t processed) = 0;

	virtual ~IInputStream() {}
};


class IOutputStream: public virtual json::RefCntObj {
public:
	///Write bytes to the stream
	/**
	 * @param buffer buffer to store
	 * @param size count of bytes to store
	 * @param written pointer to a variable which receives count of bytes has been actually stored
	 *        If this argument is 0, the function can block until all bytes are written. Otherwise
	 *        the function will perform the writting in non-blocking mode. Count of written
	 *        byttes are stored into this variable. The variable can be set to zero in situation
	 *        when writting would block.
	 * @retval true stream operation complettes successfuly
	 * @retval false stream has been closed, no more writting is possible
	 */

	virtual bool write(const unsigned char *buffer, std::size_t size, std::size_t *written = 0) = 0;
	virtual ~IOutputStream() {}

};




class InputStream {
public:
	InputStream(IInputStream *impl):impl(impl) {}
	const json::BinaryView operator()(std::size_t processed) {
		return impl->read(processed);
	}
protected:
	json::RefCntPtr<IInputStream> impl;
};

class OutputStream {
public:
	OutputStream(IOutputStream *impl):impl(impl) {}
	bool operator()(const unsigned char *buffer, std::size_t size, std::size_t *written = 0) {
		return impl->write(buffer,size,written);
	}
protected:
	json::RefCntPtr<IOutputStream> impl;
};

}


#endif /* LIGHTCOUCH_MINIHTTP_ABSTRACTIO_H_ */
