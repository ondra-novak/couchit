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

namespace LightCouch {

class IInputStream: public virtual json::RefCntObj {
public:

	///Read bytes from the stream
	/**
	 * @param processed specifies count of bytes processed after previous call. If the function
	 * called for the first time, this argument must be set to zero. Subsequent calls have to
	 * set this argument to count of bytes that were processed between the calls.
	 *
	 * @param readready pointer to a variable, which receives count of bytes ready to read. This
	 * argument can be NULL. If the argument contains a valid pointer, the function always
	 * prepares at least one byte, which may block the thread while it waits for incoming data.
	 * The variable can be set to 0 when end of file is reached. If the argument is NULL, the
	 * function receive data in non-blocking mode. In this case, the return value can be used
	 * to determine whether there are data available yet.
	 *
	 * @return pointer to buffer which contains prepared data up to size stored to the variable
	 * referenced by the pointer readready. If the pointer is NULL, function returns a valid pointer
	 * only if the buffer contains some data, but the calles should not access the buffer, until
	 * they are requested with the non-null the second argument. The function can NULL in case that
	 * the second argument is null and there are no data avaialble yet.
	 *
	 * Some examples follow
	 *  - b = read(0,&y) - read bytes, 'b' receives contain pointer a buffer. 'y' receives length of the buffer
	 *  - b = read(x,&y) - commit x bytes, and read next bytes. 'b' receices contain pointer a buffer. 'y' receives length of the buffer
	 *  - b = (read(x,0) != nullptr) - commit x bytes, and determine whether there are still data available.
	 *  - b = (read(0,0) != nullptr) - only ask, whether there are available data (non-blocking style)
	 *
	 *
	 *  @note in case of EOF, the function returns a valid pointer, but the readready variable
	 *  is set to 0. You should not access the buffer as well.
	 *
	 *  @note in case of reading error, the function returns NULL and sets the *readready to zero.
	 *  Without checking the return value it appears as end of stream
	 *
	 */
	virtual const unsigned char *read(std::size_t processed, std::size_t *readready) = 0;
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
	const unsigned char *operator()(std::size_t processed, std::size_t *readready) {
		return impl->read(processed,readready);
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
