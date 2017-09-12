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

	void operator()(const json::BinaryView &buff) {
		if (buff.empty()) {
			close();
		} else if (buff.length < (buffSize - bufferUsed)) {
			std::memcpy(buffer+bufferUsed,buff.data,buff.length);
			bufferUsed+=buff.length;
		} else if (buff.length < buffSize) {
			std::memcpy(buffer,buff.data,buff.length);
			bufferUsed+=buff.length;
		} else {
			return outFn(buff);
		}
	}


	void close() {
		flush();
		outFn(nullptr);
	}

	void flush() {
		if (bufferUsed) {
			std::size_t x = bufferUsed;
			bufferUsed = 0;
			outFn(json::BinaryView(buffer,x));
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
			if (curBuffer.empty())
				curBuffer = inFn(0);
			if (!curBuffer.empty()) {
				pos = 0;
				return curBuffer[pos++];
			} else {
				pos = 0;
				curBuffer = inFn(0);
				if (curBuffer.empty()) {
					return -1;
				}
				return curBuffer[pos++];
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
		processed = std::min(limit, processed);
		limit -= processed;
		if (limit) {
			json::BinaryView b = Super::operator()(processed);
			return b.substr(0,limit);
		} else if (processed) {
			Super::operator()(processed);
		}
		return json::BinaryView(nullptr, 0);
	}

	void setLimit(std::size_t limit) {
		this->limit = limit;
	}

protected:
	std::size_t limit;
};




#endif /* LIGHTCOUCH_MINIHTTP_BUFFERED_H_ */
