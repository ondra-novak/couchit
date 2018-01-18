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
#include <unordered_set>

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


class AsyncCheckpointQueue {
public:

	typedef std::function<void()> Msg;


	void runTask(const std::string &taskName, const Msg &task) {
		std::unique_lock<std::mutex> _(mx);
		strMap[taskName] = task;
		if (!thr.joinable()) {
			thr = std::thread([this]{
				this->run();
			});
		}
		condvar.notify_all();
	}

	void run() {
		std::unique_lock<std::mutex> _(mx);
		std::string prevName;
		for(;;) {
			condvar.wait(_, [&]{
				return finish || !strMap.empty();
			});
			if (finish) return;
			auto itr = strMap.upper_bound(prevName);
			if (itr == strMap.end()) itr = strMap.begin();
			Msg msg = itr->second;
			prevName = itr->first;
			strMap.erase(itr);
			_.unlock();
			try {
				msg();
			} catch (...) {

			};
			_.lock();
		}

	}

	~AsyncCheckpointQueue() {
		std::unique_lock<std::mutex> _(mx);
		if (thr.joinable()) {
			finish = true;
			condvar.notify_all();
			_.unlock();
			thr.join();
		}
	}


	static AsyncCheckpointQueue &getInstance();

protected:
	std::map<std::string, Msg> strMap;
	std::mutex mx;
	std::condition_variable condvar;
	std::thread thr;
	bool finish = false;


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
		q.runTask(fname, [me =RefCntPtr<AsyncCheckpointFile>(this), val = Value(res) ] {
			me->SyncCheckpointFile::store(val);
		});
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

	if (data["version"]!=3)
		return json::undefined;

	std::unordered_set<Value> objects;
	Value rows = data["rows"];
	Array newrows;
	newrows.reserve(rows.size());
	for (Value rw : rows) {
		Object o(rw);
		for (Value x: rw) {
			auto itr = objects.find(x);
			if (itr != objects.end()) {
				o.set(x.getKey(), *itr);
			} else {
				objects.insert(x.stripKey());
			}
		}
		newrows.push_back(o);
	}
	return data.replace("rows",newrows);
}

void SyncCheckpointFile::store(const Value & data) {

	std::string newfname = fname+".part";
	{
		std::ofstream out(newfname, std::ios::binary|std::ios::out|std::ios::trunc);
		if (!out) {
			int err = errno;
			throw CheckpointIOException(String({"Failed to open checkpoint file for writting: ",newfname}), err);
		}

		data.replace("version",3).serializeBinary([&](char c){
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

