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

	json::BinaryView readChunkPart(bool nonblock) {
		auto x = input->read(nonblock);
		auto rest = x.substr(std::min<std::size_t>(curChunk,x.length()));
		x = x.substr(0,x.length()-rest.length());
		input->putBack(rest);
		curChunk-=x.length();
		return x;
	}

	virtual json::BinaryView doRead(bool nonblock = false) {
		if (curChunk) return readChunkPart(nonblock);

		if (eof) {
			return AbstractInputStream::eofConst;
		}

		if (openChunk(nonblock) == false) {
			eof = true;
			return AbstractInputStream::eofConst;
		}
		if (curChunk) return readChunkPart(nonblock);
		return json::BinaryView();
#if 0

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
			return json::BinaryView();
		}
#endif
	}

	virtual bool doWaitRead(int milisecs) {
		return input->waitRead(milisecs);
	}

	bool parseChunkLine(std::size_t &put_back_count) {
		auto pos = chunkLine.find("\r\n");
		decltype(pos) beg = 0;
		if (pos == 0) {
			beg = pos+2;
			pos = chunkLine.find("\r\n",beg);
		}

		if (pos == chunkLine.npos) return false;
		put_back_count = chunkLine.length() - pos - 2;

		std::size_t n = 0;
		for (auto x = beg; x < pos; x++) {
			n = n * 16;
			char c = chunkLine[x];
			if (isdigit(c)) n = n + (c - '0');
			else if (c >= 'A' && c <= 'F') n = n + (c-'A'+10);
			else if (c >= 'a' && c <= 'f') n = n + (c-'a'+10);
			else throw std::runtime_error("Invalid chunk header");
		}
		curChunk = n;
		return true;

	}

	bool openChunk(bool nonblock) {
		do {
			auto buff = input->read(nonblock);
			if (AbstractInputStream::isEof(buff)) {
				return false;
			}

			chunkLine.append(reinterpret_cast<const char *>(buff.data()), buff.length());
			std::size_t put_back_count;
			if (parseChunkLine(put_back_count)) {
				input->putBack(buff.substr(buff.length()-put_back_count));
				chunkLine.clear();
				return curChunk > 0;
			}
			if (chunkLine.length() > 50)
				throw std::runtime_error("Chunk header is too long (50+ bytes)");
		}
		while (!nonblock);
		return true;
	}

	InputStream input;
	unsigned int curChunk = 0;
	std::string chunkLine;
	bool eof = false;
};


template <std::size_t chunkSize=65536>
class ChunkedOutputStream: public AbstractOutputStream {
public:

	ChunkedOutputStream(OutputStream stream):stream(stream) {}

	virtual void closeOutput() override {
		commit(0,true);
		sendChunk(json::BinaryView(0,0));
		flush();
	}

	virtual void flush() override {
		AbstractOutputStream::flush();
		stream->flush();
	}


protected:
	virtual Buffer createBuffer() override {
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
		std::size_t sz = writeChunkSize(printbuff,data.length());
		BinaryView chksz(printbuff, sz);
		BinaryView enter = chksz.substr(chksz.length()-2);
		stream(chksz);
		stream(data);
		stream(enter);
	}


	virtual json::BinaryView doWrite(const json::BinaryView &data, bool ) override {
		if (data.empty()) return data;
		sendChunk(data);
		return json::BinaryView(0,0);

	}
	virtual bool doWaitWrite(int milisecs) override {
		return stream->waitWrite(milisecs);
	}






protected:
	unsigned char chunkBuffer[chunkSize];

	OutputStream stream;

};

}
