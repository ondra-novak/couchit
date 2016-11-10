/*
 * abstractio.h
 *
 *  Created on: Nov 10, 2016
 *      Author: ondra
 */

#ifndef LIGHTCOUCH_MINIHTTP_ABSTRACTIO_H_
#define LIGHTCOUCH_MINIHTTP_ABSTRACTIO_H_

#include <immujson/refcnt.h>

#pragma once

namespace LightCouch {

class IInputStream: public json::RefCntObj {
public:

	virtual unsigned char *read(std::size_t processed, std::size_t *readready) = 0;
	virtual ~IInputStream() {}
};


class IOutputStream: public json::RefCntObj {
public:

	virtual void write(const unsigned char *buffer, std::size_t size) = 0;
	virtual ~IOutputStream() {}

};

class InputStream {
public:
	InputStream(IInputStream *impl):impl(impl) {}
	unsigned char *operator()(std::size_t processed, std::size_t *readready) {
		return impl->read(processed,readready);
	}
protected:
	json::RefCntPtr<IInputStream> impl;
};

class OutputStream {
public:
	OutputStream(IOutputStream *impl):impl(impl) {}
	void operator()(const unsigned char *buffer, std::size_t size) {
		return impl->write(buffer,size);
	}
protected:
	json::RefCntPtr<IOutputStream> impl;
};


}


#endif /* LIGHTCOUCH_MINIHTTP_ABSTRACTIO_H_ */
