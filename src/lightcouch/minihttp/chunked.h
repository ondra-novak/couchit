/*
 * chunked.h
 *
 *  Created on: Nov 10, 2016
 *      Author: ondra
 */

#ifndef LIGHTCOUCH_MINIHTTP_CHUNKED_H_
#define LIGHTCOUCH_MINIHTTP_CHUNKED_H_

namespace LightCouch {

///
/**
 * @tparam OutFn a function which accepts two arguments. A pointer and size to buffer to write to the output
 *
 * @code
 * void OutFn(const void *buffer, std::size_t size)
 * @endcode
 */
template<typename OutFn, std::size_t chunkSize = 65536>
class ChunkedWrite {
public:

	ChunkedWrite(const OutFn &fn):outFn(fn),chunkUsed(0) {}

	void operator()(int b) {
		if (b == -1) {
			close();
		} else {
			chunk[chunkUsed++] = b;
			if (chunkUsed == chunkSize) flush();
		}
	}

	void close() {
		flush();
		sendChunk();
	}

	void flush() {
		if (chunkUsed) {
			sendChunk();
			chunkUsed = 0;
		}
	}



protected:
	OutFn outFn;
	unsigned char chunk[chunkSize];
	std::size_t chunkUsed;

	void sendChunk() {
		char printbuff[20];
		snprintf(printbuff,19,"%lX\r\n", chunkUsed);
		std::size_t sz = printbuff[19] = 0;
		outFn(printbuff,sz);
		outFn(chunk,chinkUsed);
		outFn(printBuff+sz-2,2);
	}
};

///
/**
 * @tparam InFn
 *
 * @code
 * const unsigned char *InFn(std::size_t bytesRead, std::size_t *bytesReady = 0)
 * @endcode
 *
 * Function accepts bytesRead which carries count of bytes read from the previous buffer. To initiate
 * reading, specify zero. Second argument contains pointer to a variable which receives
 * count of bytes read in the buffer. If this argument is 0, no reading is performed. Function
 * returns pointer to buffer. If bytesReady is equal to 0, the function can return 0. If bytesReady is
 * not null and the function returns null, it has to be interpreted as the end of the stream
 *
 *
 *
 * Usage:
 *  - initial reading: buffer = InFn(0,&sz);
 *  - continuation of reading: buffer = InFn(rdsz,&sz)
 *  - end of reading: InFn(rdsz,0)
 */
template<typename InFn>
class ChunkedRead {
public:

	ChunkedRead(const InFn &fn):inFn(fn),curChunk(0),pos(0),bufferSize(0),chunkError(false) {}

	int operator()() {

		if (curChunk) {
			curChunk--;
			return readNext();
		} else {
			curChunk = readNext();
			if (curChunk == 0) return -1;
			else {
				curChunk--;
				return readNext();
			}
		}

	}

	///Returns true, if error in chunk has encoutered
	/**
	 * @retval true the reading has been terminated due error in chunk
	 * @retval false the reading has been terminated because the final chunk has been extracted
	 */
	bool invalidChunk() const {
		return chunkError;
	}

protected:

	InFn inFn;

	std::size_t curChunk;
	unsigned char *curBuffer;
	std::size_t pos;
	std::size_t bufferSize;
	bool chunkError;

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

	std::size_t readChunkHeader() {

		std::size_t acc = 0;
		int c = readNext();
		if (c == '\r') c = readNext();
		if (c == '\n') c = readNext();
		while (c != '\r') {
			acc<<=4;
			if (c>='0' && c<='9') acc += (c-48);
			else if (c>='A' && c<='F') acc+=(c-'A'+10);
			else if (c>='a' && c<='f') acc+=(c-'a'+10);
			else {
				chunkError = true;
				return 0;
			}
		}
		c = readNext();
		if (c != '\n') {
			chunkError = true;
			return 0;
		}
		return acc;
	}

};


}


#endif /* LIGHTCOUCH_MINIHTTP_CHUNKED_H_ */
