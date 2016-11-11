/*
 * chunked.h
 *
 *  Created on: Nov 10, 2016
 *      Author: ondra
 */

#ifndef LIGHTCOUCH_MINIHTTP_CHUNKED_H_
#define LIGHTCOUCH_MINIHTTP_CHUNKED_H_

#include "buffered.h"

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

	void operator()(const unsigned char *data, std::size_t sz) {
		if (sz == 0) {
			close();
		} else if (sz < (chunkSize- chunkUsed)) {
			std::memcpy(chunk+chunkUsed,data,sz);
			chunkUsed+=sz;
		} else if (sz < chunkSize) {
			flush();
			std::memcpy(chunk,data,sz);
			chunkUsed+=sz	;
		} else {
			flush();
			sendChunk(data,sz);
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
		sendChunk(chunk,chunkUsed);
	}
	void sendChunk(unsigned char *data, std::size_t datasz) {
		char printbuff[20];
		snprintf(printbuff,19,"%lX\r\n", datasz);
		std::size_t sz = printbuff[19] = 0;
		outFn(printbuff,sz);
		outFn(data,datasz);
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

	ChunkedRead(const InFn &fn):rd(fn),curChunk(0),chunkError(false) {}

	int operator()() {

		if (curChunk) {
			curChunk--;
			return rd();
		} else {
			curChunk = readChunkHeader();
			if (curChunk == 0) return -1;
			else {
				curChunk--;
				return rd();
			}
		}

	}

	const unsigned char *operator()(std::size_t processed, std::size_t *ready) {

		const unsigned char *buff;
		if (ready) {
			*ready = 0;
			if (processed >= curChunk) {
				rd(curChunk,0);
				curChunk = readChunkHeader();
				if (curChunk == 0) {
					buff = 0;
				} else {
					buff = rd(0,ready);
				}
			} else {
				buff = rd(processed,ready);
				curChunk-=processed;
			}
			if (*ready > curChunk) *ready = curChunk;
			return buff;
		} else {
			if (processed == 0) {
				return rd(0,0);
			}
			if (processed >= curChunk) {
				buff = rd(curChunk,0);
				curChunk = 0;
			} else {
				buff = rd(processed,0);
				curChunk -= processed;
			}
			return buff;
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

	BufferedRead<InFn> rd;

	std::size_t curChunk;
	bool chunkError;

	std::size_t readChunkHeader() {

		std::size_t acc = 0;
		int c = rd();
		if (c == '\r') c = rd();
		if (c == '\n') c = rd();
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
		c = rd();
		if (c != '\n') {
			chunkError = true;
			return 0;
		}
		return acc;
	}

};


}


#endif /* LIGHTCOUCH_MINIHTTP_CHUNKED_H_ */
