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
		return buffer.data == eofConst.data;
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
			lastBuff = doRead(nonblock);
		}
		return lastBuff;
	}

	///commits alread read data and allows to read next data
	/**
	 * @param sz commited size, the value must be between zero and size of the
	 * last returned buffer
	 */
	json::BinaryView commit(std::size_t sz) {
		lastBuff = lastBuff.substr(sz);
		return lastBuff;
	}

	///get data available in the buffer without reading
	json::BinaryView getBuffer() const {
		return lastBuff;
	}

	///reads next byte
	/**
	 * @return returns value of the next byte or -1 for eof
	 */
	int getNextByte() {
		if (lastBuff.empty()) {
			lastBuff = doRead();
			if (lastBuff.empty()) return -1;
		}
		int res = lastBuff[0];
		lastBuff = lastBuff.substr(1);
		return res;
	}

	///peeks value of the next byte
	/**
	 * @return returns value of the next byte without discarding it and returns -1
	 * if there is eof
	 *
	 * @note function can block
	 */
	int peekNextByte() {
		if (lastBuff.empty()) {
			lastBuff = doRead();
			if (lastBuff.empty()) return -1;
		}
		int res = lastBuff[0];
		return res;
	}

	///Wait for data
	/**
	 * Allows to wait defined time to data. Some streams doesn't support the function
	 * @param milisecs miliseconds to wait, set -1 to infinite
	 * @retval true data available
	 * @retval false timeout
	 */
	virtual bool waitRead(int milisecs) {
		if (lastBuff.empty()) return doWait(milisecs);
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
	virtual bool doWait(int milisecs) = 0;
};



class AbstractOutputStream:  virtual public json::RefCntObj {
public:

	virtual ~AbstractOutputStream() {}
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
	 *
	 * @note writting can be synced / flushed at the end. Please use maximum buffering, do not write per character
	 *
	 */
	virtual json::BinaryView write(const json::BinaryView &data, bool nonblock = false) = 0;


	///Closes the output by writting the eof character
	virtual void closeOutput() = 0;


	///Wait for write
	/**
	 * @param milisecs miliseconds to wait, set -1 to infinite
	 * @retval true data available
	 * @retval false timeout
	 */
	virtual bool waitWrite(int milisecs)  = 0;
};


class InputStream {
public:
	InputStream(AbstractInputStream *impl):impl(impl) {}
	const json::BinaryView operator()(std::size_t processed) {
		if (processed) return impl->commit(processed);
		else return impl->read();
	}
	AbstractInputStream *operator->() const {return impl;}

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

	AbstractOutputStream *operator->() const {return impl;}

protected:
	json::RefCntPtr<AbstractOutputStream> impl;
};


}


#endif /* LIGHTCOUCH_MINIHTTP_ABSTRACTIO_H_ */
