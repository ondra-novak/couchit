/*
 * chunked.h
 *
 *  Created on: Nov 10, 2016
 *      Author: ondra
 */

#ifndef LIGHTCOUCH_MINIHTTP_CHUNKED_H_
#define LIGHTCOUCH_MINIHTTP_CHUNKED_H_

#include "buffered.h"

namespace couchit {

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

	ChunkedWrite(const OutFn &fn):outFn(fn),chunkUsed(0),noError(true) {}
	~ChunkedWrite() {
		flush();
	}

	bool operator()(int c) {
		if (c == -1) return operator()(0,0,0);
		else {
			unsigned char z = (char)c;
			return operator()(&z,1,0);
		}
	}

	bool operator()(const unsigned char *data, std::size_t sz, std::size_t *wrt) {
		if (sz == 0) {
			close();
		} else if (sz <= (chunkSize- chunkUsed)) {
			std::memcpy(chunk+chunkUsed,data,sz);
			chunkUsed+=sz;
		} else if (sz <= chunkSize) {
			if (!flush()) return false;
			std::memcpy(chunk,data,sz);
			chunkUsed+=sz	;
		} else {
			return flush() && sendChunk(data,sz);
		}
		if (wrt) *wrt = sz;
		return noError;
	}

	bool close() {
		return flush() && sendChunk(chunk,0);
	}

	bool flush() {
		if (chunkUsed) {
			std::size_t end = chunkUsed;
			chunkUsed = 0;
			return sendChunk(chunk,end);
		} else {
			return true;
		}
	}

	bool isError() const {
		return !noError;
	}


protected:
	OutFn outFn;
	unsigned char chunk[chunkSize];
	std::size_t chunkUsed;
	bool noError;



	char *writeHex(char *buff, std::size_t num) {
		if (num) {
			char *c = writeHex(buff, num>>4);
			std::size_t rem = num & 0xF;
			*c = rem<10?rem+'0':rem+'A'-10;
			return c+1;
		} else {
			return buff;
		}
	}

	std::size_t writeChunkSize(char *buff, std::size_t num) {
		char *end = buff+1;
		if (num == 0) {
			buff[0] = '0';
		} else {
			end = writeHex(buff,num);
		}
		end[0] = '\r';
		end[1] = '\n';
		return end-buff+2;
	}

	bool sendChunk(const unsigned char *data, std::size_t datasz) {
		char printbuff[50];
		std::size_t sz = writeChunkSize(printbuff,datasz);
		noError =  outFn(reinterpret_cast<unsigned char *>(printbuff),sz,0)
				&& outFn(data,datasz,0)
				&& outFn(reinterpret_cast<unsigned char *>(printbuff)+sz-2,2,0);
		return noError;

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
	enum State {
		skipWhite,
		readNum,
		readCR,
		readLF,
		readFinish
	};

public:

	ChunkedRead(const InFn &fn):inFn(fn),curChunk(0),state(skipWhite),eof(false),chunkError(false) {}

	const unsigned char *operator()(std::size_t processed, std::size_t *ready) {

		const unsigned char *buff;
		//we need to prepare next data?
		if (ready) {
			//set zero first
			*ready = 0;
			//eof reported
			if (eof) {
				//set fake buffer ptr (*ready is set to 0 = eof)
				buff = reinterpret_cast<const unsigned char *>(ready);

			//read whole chunk
			} else if (processed >= curChunk) {
				//accept the remaining bytes
				inFn(curChunk,0);
				//read chunk header
				readChunkHeader();
				//we have empty chunk
				if (curChunk == 0) {
					//set fake buffer ptr (*ready is set to 0 = eof)
					buff = reinterpret_cast<const unsigned char *>(ready);
					//set eof
					eof = true;
				} else {
					//otherwise prepare output
					buff = inFn(0,ready);
				}
			} else {
				//chunk is not finished
				//prepare next data
				buff = inFn(processed,ready);
				//decrease remaining bytes
				curChunk-=processed;
			}
			//limit ready bytes by chunk size
			if (*ready > curChunk) *ready = curChunk;
			//return buffer
			return buff;
		} else {
			//we dont need to prepare data
			//first test, whether processed is equal to chunk
			if (processed >= curChunk && !eof) {
				//accept chunk
				const unsigned char *p = inFn(curChunk,0);
				//finish cur-chunk
				curChunk = 0;

				if (p != 0) {
					readChunkHeaderImpl();
					if (state == readFinish) {
						state = skipWhite;
						if (curChunk == 0) {
							eof = true;
						}
						return inFn(0,0);
					}
				}
				return nullptr;
			} else {
				curChunk-=processed;
				//report processed bytes
				return inFn(processed,0);
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
	std::size_t curChunk,accChunk;
	State state;
	bool eof;
	bool chunkError;

	bool setChunkError() {
		chunkError = true;
		state = readFinish;
		curChunk = 0;
		return true;
	}

	bool readChunkHeaderImpl() {
		//we need to read chunk header with respect to currently available characters

		//pointer to buffer
		const unsigned char *buff;

		do {
			//ready bytes
			std::size_t rd;
			//processed bytes (first initialized to 0
			std::size_t processed = 0;
			//read bytes (may block if processed == previous rd)
			buff = inFn(processed,&rd);
			//if null returned, then unexpected eof
			if (buff == 0 || rd == 0) return setChunkError();

			do {
				//depend on state
				switch(state) {
				//we are skipping whites
				case skipWhite:
					//until all bytes processed
					while (processed < rd) {
						//read byte
						unsigned char b = buff[processed];
						//if hex digit
						if (isxdigit(b)) {
							//change state
							state = readNum;
							accChunk = 0;
							//break;
							break;
						//if it is not space and not hex
						} else if (!isspace(buff[processed])) {
							return setChunkError();
						} else {
							//process next byte
							++processed;
						}
					}
					break;
				case readNum:
					//until all bytes processed
					while (processed < rd) {
						//read byte
						unsigned char b = buff[processed];
						//convert hex digit to number
						if (b >= '0' && b <='9') {
							accChunk = (accChunk << 4) + (b - '0');
						} else if (b >= 'A' && b <='F') {
							accChunk = (accChunk << 4) + (b - 'A' + 10);
						} else if (b >= 'a' && b <='f') {
							accChunk = (accChunk << 4) + (b - 'a' + 10);
						} else {
							//nonhex character - advance to next state
							state = readCR;
							break;
						}
						//count processed characters
						++processed;
					}
					break;
				case readCR:
					//only /r is expected
					if (buff[processed] != '\r') return setChunkError();
					//advance next state
					state = readLF;
					//processed
					++processed;
					break;
				case readLF:
					//only /n is expected
					if (buff[processed] != '\n') return setChunkError();
					state = readFinish;
					//processed
					++processed;

					break;
				case readFinish:
					curChunk = accChunk;
					inFn(processed,0);
					return true;
				}

			}
			//still left bytes to process? continue
			while (processed < rd);
		buff = inFn(processed,0);
		}
		while (buff != 0);

		return false;
	}

	bool readChunkHeaderNonBlock() {
		if (inFn(0,0)) {
			readChunkHeaderImpl();
			if (state == readFinish) {
				state = skipWhite;
				return true;
			}
		}
		return false;

	}

	void readChunkHeader() {
		while (state != readFinish) {
			readChunkHeaderImpl();
		}
		state = skipWhite;
	}

};


}


#endif /* LIGHTCOUCH_MINIHTTP_CHUNKED_H_ */
