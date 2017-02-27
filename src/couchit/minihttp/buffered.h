/*
 * buffered.h
 *
 *  Created on: 10. 11. 2016
 *      Author: ondra
 */

#ifndef LIGHTCOUCH_MINIHTTP_BUFFERED_H_
#define LIGHTCOUCH_MINIHTTP_BUFFERED_H_

#include <cstring>

///
/**
 * @tparam OutFn a function which accepts two arguments. A pointer and size to buffer to write to the output
 *
 * @code
 * void OutFn(const void *buffer, std::size_t size)
 * @endcode
 */
template<typename OutFn, std::size_t buffSize = 4096>
class BufferedWrite {
public:

	BufferedWrite(const OutFn &fn):outFn(fn),bufferUsed(0) {}
	~BufferedWrite() {
		flush();
	}

	void operator()(int b) {
		if (b == -1) {
			close();
		} else {
			buffer[bufferUsed++] = b;
			if (bufferUsed == buffSize) flush();
		}
	}

	bool operator()(const unsigned char *data, std::size_t sz) {
		if (sz == 0) {
			close();
		} else if (sz < (buffSize - bufferUsed)) {
			std::memcpy(buffer+bufferUsed,data,sz);
			bufferUsed+=sz;
		} else if (sz < buffSize) {
			if (!flush()) return false;
			std::memcpy(buffer,data,sz);
			bufferUsed+=sz	;
		} else {
			if (!flush()) return false;
			return outFn(data,sz);
		}
		return true;
	}


	void close() {
		flush();
		outFn(0,0,0);
	}

	bool flush() {
		if (bufferUsed) {
			std::size_t x = bufferUsed;
			bufferUsed = 0;
			return outFn(buffer,x,0);
		} else {
			return true;
		}
	}


protected:
	OutFn outFn;
	unsigned char buffer[buffSize];
	std::size_t bufferUsed;
};

///
/**
 * @tparam InFn
 *
 * @code
 * const unsigned char *InFn(std::size_t bytesRead, std::size_t *bytesReady = 0)
 * @endcode
 *
 */
template<typename InFn>
class BufferedRead {
public:

	BufferedRead(const InFn &fn):inFn(fn),pos(0),curBuffer(nullptr,0) {}
	~BufferedRead() {
		commit();
	}


	void commit() {
		if (pos) inFn(pos);
		pos = 0;
		curBuffer = json::BinaryView(nullptr, 0);
	}

	int operator()() {
		return readNext();
	}

	json::BinaryView operator()(std::size_t procesed) {
		if (pos < curBuffer.length) {
			commit();
		}
		return inFn(procesed);
	}


protected:

	InFn inFn;

	json::BinaryView curBuffer;
	std::size_t pos;	

	int readNext() {
		if (pos < curBuffer.length) {
			return curBuffer[pos++];
		} else {
			if (pos > curBuffer.length) 
				return -1;
			curBuffer = inFn(pos);
			if (!curBuffer.empty()) {
				pos = 0;
				return curBuffer[pos++];
			} else {
				if (pos) curBuffer = inFn(0);
				if (!curBuffer.empty()) {
					pos = 0;
					return curBuffer[pos++];
				}
				pos = 1;
				return -1;
			}
		}
	}
};

template<typename OutFn, std::size_t buffSize = 65536>
class BufferedWriteWLimit: public BufferedWrite<OutFn> {
public:


		typedef BufferedWrite<OutFn> Super;
	BufferedWriteWLimit(const OutFn &fn):Super(fn),limit(-1) {}

	void operator()(int b) {
		if (limit) {
			Super::operator()(b);
			limit--;
		}
	}

	void operator()(const unsigned char *data, std::size_t sz) {
		std::size_t limsz = std::min(limit,sz);
		if (limsz) {
			Super::operator ()(data,limsz);
			limit -= limsz;
		}
	}

	void setLimit(std::size_t limit) {
		this->limit = limit;
	}


protected:
	std::size_t limit;

};

template<typename InFn>
class BufferedReadWLimit: public BufferedRead<InFn> {
public:
	typedef BufferedRead<InFn> Super;


	BufferedReadWLimit(const InFn &fn):Super(fn),limit(-1) {}

	int operator()() {
		if (limit) {
			limit--;
			return Super::operator ()();
		} else {
			return -1;
		}
	}

	json::BinaryView operator()(std::size_t processed) {
		limit -= std::min(limit,processed);
		if (limit) {
			json::BinaryView b = Super::operator()(processed);
			return b.substr(0,limit);
		} else {
			return json::BinaryView(nullptr, 0);
		}
	}

	void setLimit(std::size_t limit) {
		this->limit = limit;
	}

protected:
	std::size_t limit;
};




#endif /* LIGHTCOUCH_MINIHTTP_BUFFERED_H_ */
