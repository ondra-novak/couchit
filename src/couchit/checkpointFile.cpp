/*
 * checkpointFile.cpp
 *
 *  Created on: 14. 12. 2017
 *      Author: ondra
 */

#include <cstdio>
#include <cerrno>
#include <fstream>
#include <atomic>
#include <thread>
#include "checkpointFile.h"

#include "query.h"
#include <imtjson/binjson.tcc>
#include <shared/dispatcher.h>

namespace couchit {

using namespace json;

class SyncCheckpointFile: public AbstractCheckpoint {
public:

	SyncCheckpointFile(const std::string &fname):fname(fname) {}

	virtual Value load() const override;
	virtual void store (const Value &res) override;


protected:
	std::string fname;


};


class AsyncCheckpointQueue: public ondra_shared::Dispatcher {
public:

	void operator<<(const Msg &msg) {
		ondra_shared::Dispatcher::operator <<(msg);
		bool b = false;
		if (running.compare_exchange_strong(b, true)) {
			if (thr.joinable()) thr.join();
			thr = std::thread([this] {
				this->run();
				running = false;
			});
		}
	}

	~AsyncCheckpointQueue() {
		if (running) this->quit();
		if (thr.joinable()) thr.join();
	}

	static AsyncCheckpointQueue &getInstance();
protected:
	std::atomic<bool> running;
	std::thread thr;

};

AsyncCheckpointQueue &AsyncCheckpointQueue::getInstance() {
	static AsyncCheckpointQueue q;
	return q;
}

class AsyncCheckpointFile: public SyncCheckpointFile {
public:
	using SyncCheckpointFile::SyncCheckpointFile;

	virtual void store(const Value &res) override{
		AsyncCheckpointQueue &q = AsyncCheckpointQueue::getInstance();
		q << [me =RefCntPtr<AsyncCheckpointFile>(this), val = Value(res) ] {
			me->SyncCheckpointFile::store(val);
		};
	}
};

PCheckpoint checkpointFile(StrViewA fname) {
	return new SyncCheckpointFile(fname);
}

PCheckpoint asyncCheckpointFile(StrViewA fname) {
	return new AsyncCheckpointFile(fname);
}

Value SyncCheckpointFile::load() const {


	std::ifstream in(fname, std::ios::in|std::ios::binary);
	if (!in) return json::undefined;

	Value data = Value::parseBinary([&]{
		return in.get();
	}, base64);



	return data;
}

void SyncCheckpointFile::store(const Value & data) {

	std::string newfname = fname+".part";
	{
		std::ofstream out(newfname, std::ios::binary|std::ios::out|std::ios::trunc);
		if (!out) {
			int err = errno;
			throw CheckpointIOException(String({"Failed to open checkpoint file for writting: ",newfname}), err);
		}

		data.serializeBinary([&](char c){
			out.put(c);
		},json::compressKeys);

		if (!out){
			int err = errno;
			throw CheckpointIOException(String({"Failed to write to the checkpoint file: ",newfname}),err);
		}
	}
	std::rename(newfname.c_str(), fname.c_str());
}


} /* namespace couchit */

