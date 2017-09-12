#pragma once
#include "abstractio.h"

namespace couchit {


class StringInputStream: public AbstractInputStream {
public:

	StringInputStream(json::BinaryView string):string(string),eof(false) {}


	virtual void closeInput() {}


protected:
	json::BinaryView string;
	bool eof;

	json::BinaryView doRead(bool ) {

		if (eof) return eofConst;
		eof = true;
		return string;
	}

	virtual bool doWaitRead(int ) {
		return true;
	}

};


template<typename Fn>
class ProducerInputStream: public AbstractInputStream {
	typedef decltype((*(Fn *)(nullptr))()) RetVal;
public:

	ProducerInputStream(const Fn &producer):producer(producer),eof(false) {}
	ProducerInputStream(const Fn &producer, const RetVal &initialVal):producer(producer),eof(false),storage(initialVal) {}

	virtual void closeInput() {}

protected:
	Fn producer;
	bool eof;
	RetVal storage;

	json::BinaryView doRead(bool ) {

		if (eof) return eofConst;
		storage = producer();
		json::BinaryView buff(storage);
		if (!buff.empty()) return buff;
		eof = true;
		return eofConst;
	}

	virtual bool doWaitRead(int ) {
		return true;
	}

};

template<typename Fn, std::size_t bufferSize = 1024>
class ConsumentOutputStream: public AbstractOutputStream {
public:

	ConsumentOutputStream(const Fn &consument): consument(consument),outputClosed(false) {}
	~ConsumentOutputStream() {
		commit(0,true);
	}

	virtual void closeOutput() {
		outputClosed = true;
		consument(AbstractInputStream::eofConst);
	}

protected:
	Fn consument;
	bool outputClosed;
	unsigned char buffer[bufferSize];

	virtual Buffer createBuffer() {
		return Buffer(buffer, bufferSize);
	}
	virtual json::BinaryView doWrite(const json::BinaryView &data, bool nonblock) {
		if (data.empty() || outputClosed) return data;
		consument(data);
		return json::BinaryView(0,0);
	}
	virtual bool doWaitWrite(int milisecs) {
		return true;
	}

};


}
