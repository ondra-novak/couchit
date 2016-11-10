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
template<typename OutFn, std::size_t buffSize = 65536>
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




#endif /* LIGHTCOUCH_MINIHTTP_BUFFERED_H_ */
