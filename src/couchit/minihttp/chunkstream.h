#pragma once

#include "abstractio.h"

namespace couchit {

class ChunkedInputStream: public AbstractInputStream {
public:
	ChunkedInputStream(InputStream input):input(input) {}

	void closeInput() {
		input->closeInput();
	}

protected:

	virtual json::BinaryView doRead(bool nonblock = false) {
		if (toCommit)  {
			input->commit(toCommit);
			curChunk -= toCommit;
			toCommit = 0;
		}
		if (curChunk) {
			auto x = input->read(nonblock).substr(0,curChunk);
			toCommit = x.length;
			return x;
		} else if (eof) {
			return AbstractInputStream::eofConst;
		} else {
			if (openChunk(nonblock) == false) {
				eof = true;
				return AbstractInputStream::eofConst;
			}
			if (curChunk) {
				auto x = input->read(nonblock).substr(0,curChunk);
				toCommit = x.length;
				return x;
			}
		}
	}

	virtual bool doWaitRead(int milisecs) {
		return input->waitRead(milisecs);
	}

	bool openChunk(bool nonblock) {
		do {
			auto buff = input->read(nonblock);
			if (AbstractInputStream::isEof(buff)) {
				return false;
			}

			std::size_t  pos = 0;
			while (chunkHdrSz < sizeof(chunkHdr) && pos < buff.length) {
				unsigned char c = buff[pos++];
				if (isspace(c) && chunkHdrSz == 0)
					continue;

				chunkHdr[chunkHdrSz++] = c;
				if (chunkHdrSz > 2 && c == '\n') break;
			}

			input->commit(pos);

			StrViewA strHdr (chunkHdr, chunkHdrSz);
			if (strHdr.length>2 && strHdr.substr(strHdr.length-2) == "\r\n") {
				std::size_t a = ::strtoul(strHdr.data,nullptr,16);
				curChunk = a;
				chunkHdrSz = 0;
				return curChunk > 0;
			}
			if (strHdr.length == sizeof(chunkHdr))
				throw std::runtime_error("Chunk header is too long (50+ bytes)");
		} while (!nonblock);

		curChunk = 0;
		return true;
	}

	InputStream input;
	char chunkHdr[50];
	int chunkHdrSz = 0;
	unsigned int curChunk = 0;
	unsigned int toCommit = 0;
	bool eof = false;
};


template <std::size_t chunkSize=65536>
class ChunkedOutputStream: public AbstractOutputStream {
public:

	ChunkedOutputStream(OutputStream stream):stream(stream) {}

	virtual void closeOutput() {
		commit(0,true);
		sendChunk(json::BinaryView(0,0));
	}

protected:
	virtual Buffer createBuffer() {
		return Buffer(chunkBuffer, chunkSize);
	}

	unsigned char *writeHex(unsigned char *buff, std::size_t num) {
		if (num) {
			unsigned char *c = writeHex(buff, num>>4);
			std::size_t rem = num & 0xF;
			*c = (char)(rem<10?rem+'0':rem+'A'-10);
			return c+1;
		} else {
			return buff;
		}
	}

	std::size_t writeChunkSize(unsigned char *buff, std::size_t num) {
		unsigned char *end = buff+1;
		if (num == 0) {
			buff[0] = '0';
		} else {
			end = writeHex(buff,num);
		}
		end[0] = '\r';
		end[1] = '\n';
		return end-buff+2;
	}


	void sendChunk(const json::BinaryView &data) {
		unsigned char printbuff[50];
		std::size_t sz = writeChunkSize(printbuff,data.length);
		BinaryView chksz(printbuff, sz);
		BinaryView enter = chksz.substr(chksz.length-2);
		stream(chksz);
		stream(data);
		stream(enter);
	}


	virtual json::BinaryView doWrite(const json::BinaryView &data, bool nonblock) {
		if (data.empty()) return data;
		sendChunk(data);
		return json::BinaryView(0,0);

	}
	virtual bool doWaitWrite(int milisecs) {
		stream->waitWrite(milisecs);
	}






protected:
	unsigned char chunkBuffer[chunkSize];

	OutputStream stream;

};

}
