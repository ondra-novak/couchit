/*
 * abstractio.h
 *
 *  Created on: Nov 10, 2016
 *      Author: ondra
 */

#ifndef LIGHTCOUCH_MINIHTTP_ABSTRACTIO_H_
#define LIGHTCOUCH_MINIHTTP_ABSTRACTIO_H_

#include <imtjson/refcnt.h>
#include <cstring>

#pragma once

namespace couchit {

#if 0
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
	 * @paramstatic  size count of bytes to store
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
#endif




///basic I/O stream minimum interface
class AbstractInputStream: virtual public json::RefCntObj {
public:

	virtual ~AbstractInputStream() {}
	///a constant which always causes that isEof() returns true
	static const json::BinaryView eofConst;


	///Determines, whether the returned result is EOF or empty read
	/**
	 * @param buffer the buffer returned by the read() function
	 * @retval true returned buffer is empty because EOF
	 * @retval false returned buffer is empty because non-blocking reading is not
	 * possible (this value is also returned when the buffer is not empty)
	 */
	static bool isEof(const json::BinaryView &buffer) {
		return buffer.data() == eofConst.data();
	}

	///read some data from the stream
	/**
	 * @param nonblock true to read data in non-blocking mode
	 * @return Read data. They are stored in internal buffer. The function can return empty buffer. In non-blocking mode
	 * the empty buffer can be returned because there are no more data available
	 * without blocking. In both modes, the empty buffer can be also returned
	 * if EOF is reached. For reading in non-blocking mode, you have
	 * to use function isEof to distinguish between empty read and EOF.
	 *
	 * @note you have to call commit() to continue reading, otherwise the function
	 * returns the same data as previously
	 *
	 */
	json::BinaryView read(bool nonblock = false) {
		if (lastBuff.empty()) {
			return doRead(nonblock);
		} else {
			json::BinaryView res = lastBuff;
			lastBuff = json::BinaryView();
			return res;
		}
	}

	void putBack(json::BinaryView b) {
		lastBuff = b;
	}


	///reads next byte
	/**
	 * @return returns value of the next byte or -1 for eof
	 */
	int getNextByte() {
		auto b = read();
		if (b.empty()) return -1;
		putBack(b.substr(1));
		return b[0];
	}

	///peeks value of the next byte
	/**
	 * @return returns value of the next byte without discarding it and returns -1
	 * if there is eof
	 *
	 * @note function can block
	 */
	int peekNextByte() {
		auto b = read();
		if (b.empty()) return -1;
		putBack(b);
		return b[0];
	}

	///Wait for data
	/**
	 * Allows to wait defined time to data. Some streams doesn't support the function
	 * @param milisecs miliseconds to wait, set -1 to infinite
	 * @retval true data available
	 * @retval false timeout
	 */
	virtual bool waitRead(int milisecs) {
		if (lastBuff.empty()) return doWaitRead(milisecs);
		return true;
	}

	///closes input, so no reading is possible
	/**
	 * Note function doesn't discard current buffer. But after the buffer
	 * is commited, the eof appears in the stream and no more reading
	 * is possible
	 */
	virtual void closeInput() = 0;

protected:
	json::BinaryView lastBuff = json::BinaryView(0,0);

	virtual json::BinaryView doRead(bool nonblock = false) = 0;
	virtual bool doWaitRead(int milisecs) = 0;
};



class AbstractOutputStream:  virtual public json::RefCntObj {
public:

	virtual ~AbstractOutputStream() {}


	struct Buffer {
		unsigned char * buff;
		std::size_t size;


		Buffer (unsigned char *buff,std::size_t size):buff(buff),size(size) {}
		Buffer ():buff(0),size(0) {}
	};


	///Creates and prepares buffer for writting
	/**
	 * @param reqSize required size to write. Default value 0 means, that
	 *   size of the buffer can vary depend on current stream state. You can specify
	 *   a size, but this is taken as a hint. However, in most of cases, for small
	 *   values up to 256 bytes, the function should always supply a buffer of specified
	 *   size (with exception see noblock). Highter value can cause, that smaller buffer
	 *   will be returned.
	 * @param nonblock set true to perform this operation in nonblocking mode. Note that
	 *  if operation cannot be performed in nonblocking mode, smaller or empty buffer
	 *  can be returned
	 *
	 * @return returns buffer. Note that in nonblocking mode, none or empty buffer can be
	 * returned.
	 */
	Buffer getBuffer(std::size_t reqSize = 0, bool nonblock = false) {
		if (availBuff.buff == nullptr) {
			availBuff = createBuffer();
			wrpos = 0;
		}
		std::size_t avl = availBuff.size - wrpos;
		if (reqSize == 0) reqSize = availBuff.size/2+1;
		if (reqSize > avl) {
			json::BinaryView b (availBuff.buff, wrpos);
			json::BinaryView c = doWrite(b, nonblock);
			if (!c.empty()) {
				if (c != b) {
					std::memmove(availBuff.buff, c.data(), c.length());
					wrpos = c.length();
				}
			} else {
				wrpos = 0;
			}
		}
		return Buffer(availBuff.buff+ wrpos, availBuff.size - wrpos);
	}



	///Commits writes to the buffer
	/**
	 * @param size count of bytes written to the buffer
	 * @param flush set true to flush data to the stream. Note that operation is blocking.
	 *
	 */
	void commit(std::size_t size, bool flush = false) {
		wrpos+=size;
		if (flush && wrpos) {
			json::BinaryView b (availBuff.buff, wrpos);
			doWrite(b,false);
			availBuff = createBuffer();
			wrpos = 0;
		}
	}


	///Writes data to the stream
	/**
	 * Function can buffer the data at internal buffer until the buffer is filled.
	 *
	 * @param data data to write
	 * @param mode choose one of modes
	 * @return reference to data which were not stored. For the full mode,
	 * the returned value should be empty buffer. For partial, nonblock and tobuffer mode
	 * the returned value can contain any part of the original buffer which were
	 * not written. For the mode nonblock and tobuffer, function can return
	 * the argument data in case, that there is no room to write more data
	 *
	 * If there is error during the writting, the exception is thrown
	 */
	json::BinaryView write(const json::BinaryView &data, bool nonblock = false) {
		Buffer b = getBuffer(data.length(), nonblock);
		if (b.size >= data.length()) {

			std::size_t towr = std::min(b.size, data.length());
			std::memmove(b.buff,data.data(), towr);
			commit(towr);
			return data.substr(towr);
		} else {
			return doWrite(data, nonblock);
		}
	}


	///Closes the output by writting the eof character
	virtual void closeOutput() {}

	///flush the buffer and all buffers in the chain.
	/** function calls commit(0,true); but can be overwritten by handling
	 * additional flushes
	 */
	virtual void flush() {this->commit(0,true);}
	///Wait for write
	/**
	 * @param milisecs miliseconds to wait, set -1 to infinite
	 * @retval true data available
	 * @retval false timeout
	 */
	bool waitWrite(int milisecs)  {
		if (wrpos < availBuff.size) return true;
		return doWaitWrite(milisecs);

	}
protected:

	Buffer availBuff;
	std::size_t wrpos = 0;

	virtual Buffer createBuffer() = 0;
	virtual json::BinaryView doWrite(const json::BinaryView &data, bool nonblock) = 0;
	virtual bool doWaitWrite(int milisecs) = 0;

};


class InputStream {
public:
	InputStream(AbstractInputStream *impl):impl(impl) {}
	json::BinaryView read(bool nonblock = false) {return impl->read(nonblock);}
	void putBack(const json::BinaryView &b) {impl->putBack(b);}

	AbstractInputStream *operator->() const {return impl;}
	AbstractInputStream *get() const {return impl;}

	int operator()() const {
		return impl->getNextByte();
	}


protected:
	json::RefCntPtr<AbstractInputStream> impl;
};

class OutputStream {
public:
	OutputStream(AbstractOutputStream *impl):impl(impl) {}
	void operator()(const json::BinaryView &data) {
		impl->write(data);
	}
	///sending nullptr causes closing the output
	void operator()(std::nullptr_t) {
		impl->closeOutput();
	}
	void operator()(char c) {
		AbstractOutputStream::Buffer b = impl->getBuffer(1);
		*b.buff = c;
		impl->commit(1);
	}
	void flush() {
		impl->flush();
	}


	AbstractOutputStream *operator->() const {return impl;}

protected:
	json::RefCntPtr<AbstractOutputStream> impl;
};


}


#endif /* LIGHTCOUCH_MINIHTTP_ABSTRACTIO_H_ */
