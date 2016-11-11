/*
 * buffered.h
 *
 *  Created on: 10. 11. 2016
 *      Author: ondra
 */

#ifndef LIGHTCOUCH_MINIHTTP_BUFFERED_H_
#define LIGHTCOUCH_MINIHTTP_BUFFERED_H_


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

	void operator()(int b) {
		if (b == -1) {
			close();
		} else {
			buffer[bufferUsed++] = b;
			if (bufferUsed == buffSize) flush();
		}
	}

	void operator()(const unsigned char *data, std::size_t sz) {
		if (sz == 0) {
			close();
		} else if (sz < (buffSize - bufferUsed)) {
			std::memcpy(buffer+bufferUsed,data,sz);
			bufferUsed+=sz;
		} else if (sz < buffSize) {
			flush();
			std::memcpy(buffer,data,sz);
			bufferUsed+=sz	;
		} else {
			flush();
			outFn(data,sz);
		}
	}


	void close() {
		flush();
		outFn(0,0);
	}

	void flush() {
		if (bufferUsed) {
			outFn(buffer,bufferUsed);
			bufferUsed = 0;
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

	BufferedRead(const InFn &fn):inFn(fn),pos(0),bufferSize(0) {}

	int operator()() {
		return readNext();
	}

	const unsigned char *operator()(std::size_t procesed, std::size_t *ready) {
		if (pos < bufferSize) {
			inFn(pos,0);
			pos = bufferSize = 0;
		}
		return inFn(procesed,ready);
	}


protected:

	InFn inFn;

	unsigned char *curBuffer;
	std::size_t pos;
	std::size_t bufferSize;

	int readNext() {
		if (pos < bufferSize) {
			return curBuffer[pos++];
		} else {
			curBuffer = inFn(pos,&bufferSize);
			if (curBuffer) {
				pos = 0;
				return curBuffer[pos++];
			} else {
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


	BufferedReadWLimit(const InFn &fn):Super(fn) {}

	int operator()() {
		if (limit) {
			limit--;
			return Super::operator ()();
		} else {
			return -1;
		}
	}

	const unsigned char *operator()(std::size_t processed, std::size_t *ready) {
		limit -= std::min(limit,processed);
		if (limit) {
			const unsigned char *b = Super::operator()(processed,ready);
			if (ready && *ready > limit) *ready = limit;
			return b;
		} else {
			const unsigned char *b = Super::operator()(processed,0);
			return ready?0:b;
		}
	}
};




#endif /* LIGHTCOUCH_MINIHTTP_BUFFERED_H_ */
