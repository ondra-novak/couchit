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
#include "shared/dispatcher.h"

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

	Value version = data["version"];
	if (version.defined()) {
		if (version.getUInt() == 2) {
			Value compdata = data["compdata"];
			Value objtable = data["objects"];
			Array decompdata;
			decompdata.reserve(compdata.size());
			Array tmp;
			tmp.reserve(10);
			unsigned int columns = data["columns"].getUInt();


			for (Value rw : compdata) {
				if (rw.isNull()) tmp.push_back(rw);
				std::size_t dataid = rw.getUInt();
				tmp.push_back(objtable[dataid]);
				if (tmp.size() == columns) {
					decompdata.push_back(tmp);
					tmp.clear();
				}
			}
			return data.replace("data",decompdata);
		} else {
			return Value();
		}
	} else {
		return data;
	}

}

void SyncCheckpointFile::store(const Value & data) {

	std::string newfname = fname+".part";
	{
		std::ofstream out(newfname, std::ios::binary|std::ios::out|std::ios::trunc);
		if (!out) {
			int err = errno;
			throw CheckpointIOException(String({"Failed to open checkpoint file for writting: ",newfname}), err);
		}

		Array compdata;
		Value rows = data["data"];
		std::unordered_map<Value, Value> objmap;
		Array objtable;
		unsigned int columns = 4;

		for (Value rw: rows) {
			for (unsigned int i = 0; i < columns; i++) {
				Value c = rw[i];
				Value idx;
				auto x = objmap.find(c);
				if (x == objmap.end()) {
					idx = objtable.size();
					objmap.insert(std::make_pair(c, idx));
					objtable.push_back(c);
				} else {
					idx = x->second;
				}
				compdata.push_back(idx);
			}
		}

		Object newdata(data);
		newdata.unset("data");
		newdata.set("compdata", compdata);
		newdata.set("objects", objtable);
		newdata.set("version",2);
		newdata.set("columns",columns);
		Value ndata = newdata;

		ndata.serializeBinary([&](char c){
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
