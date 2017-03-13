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
			*c = (char)(rem<10?rem+'0':rem+'A'-10);
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

	ChunkedRead(const InFn &fn):inFn(fn),curChunk(0),accChunk(0),state(skipWhite),eof(false),eofReported(false),chunkError(false) {}

	json::BinaryView operator()(std::size_t processed) {
		using json::BinaryView;
		//when processed == 0, stronger part folllows
		if (processed == 0) {
			//if reading while eof,
			if (eof || chunkError) {
				//eof already reported, then throw exception
				if (eofReported) throw std::runtime_error("Reading beyoind the chunk");
				//mark that we now reporting the eof
				eofReported = true;
				//return empty buffer
				return BinaryView(nullptr, 0);
			}
			//if still any data in chunk
			if (curChunk) {
				//read next bytes from the input
				BinaryView b = inFn(0);
				//trim up to given chunk
				return b.substr(0, curChunk);
			}
			//no more chunk data
			else {
				//prepare next chunk
				return prepareNext(0);
			}
		}
		//when processed <> 0 weaker part follows
		else {
			//if eof or error, return empty buffer - no report is recorded
			if (eof || chunkError)
			//
				return BinaryView(nullptr, 0);
			//processed whole chunk?
			if (curChunk <= processed) {
				//adjust processed
				processed = curChunk;
				//finish chunk
				curChunk = 0;
				//prepare next chunk
				return prepareNext(processed);
			}
			else {
				//decrease remaining chunk
				curChunk -= processed;
				//read reast of buffer
				BinaryView b = inFn(processed);
				//trim up to chunk
				return b.substr(0, curChunk);
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
	bool eofReported;
	bool chunkError;

	bool setChunkError() {
		chunkError = true;
		state = readFinish;
		curChunk = 0;
		return true;
	}

	json::BinaryView prepareNext(std::size_t processed) {
		BinaryView b = inFn(processed);
		do {
			std::size_t sz = parseChunkHdr(b);
			if (sz)
				b = inFn(sz);
			if (eof)
				return BinaryView(nullptr, 0);
			if (processed == 0 && b.empty()) {
				b = inFn(0);
				if (b.empty()) eof = true;
			}
		} while (curChunk == 0 && !b.empty());
		return b.substr(0, curChunk);

	}

	std::size_t parseChunkHdr(const json::BinaryView &data) {
		for (std::size_t i = 0; i < data.length; ) {
			unsigned char c = data[i];
			switch (state) {
			case skipWhite: if (!isspace(c)) state = readNum;
							else i++;
							break;
			case readNum:	if (isdigit(c)) { accChunk = (accChunk << 4) + (c - '0'); i++; }
							else if (c >= 'A' && c <= 'F') { accChunk = (accChunk << 4) + (c - 'A' + 10); i++; }
							else if (c >= 'a' && c <= 'f') { accChunk = (accChunk << 4) + (c - 'a' + 10); i++; }
							else state = readCR;
							break;
			case readCR:   if (c != '\r') {
				setChunkError();
			}
						   else {
							   i++;
							   state = readLF;
						   }
						   break;
			case readLF:   if (c != '\n') {
				setChunkError();
			}
						   else {
							   i++;
							   state = readFinish;
						   }
						   break;
			case readFinish:
				if (chunkError) return 0;
				std::swap(curChunk, accChunk);
				if (curChunk == 0) eof = true;
				state = skipWhite;
				return i;
			}
		}
		return data.length;
	}

};


}


#endif /* LIGHTCOUCH_MINIHTTP_CHUNKED_H_ */
