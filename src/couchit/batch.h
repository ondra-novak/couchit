/*
 * batch.h
 *
 *  Created on: 10. 10. 2020
 *      Author: ondra
 */

#ifndef SRC_COUCHIT_SRC_COUCHIT_BATCH_H_
#define SRC_COUCHIT_SRC_COUCHIT_BATCH_H_

#include <atomic>
#include <thread>
#include <functional>
#include <variant>
#include <imtjson/value.h>
#include <shared/msgqueue.h>


namespace couchit {
class Document;
class CouchDB;

///Performs batch writting using background thread
/** it collects writes and send them as batch to the database
 * Writes are collected when thread is busy to communicate with the database
 *
 * Every write request can contain callback, which is called when write is succeed
 *
 */
class BatchWrite {
public:
	///Initialize instance and start background thread
	BatchWrite(CouchDB &db);
	///Destroy instance and stop thread
	/** Note that destructor ensures, that all queued requests are finalized */
	virtual ~BatchWrite();

	///A callback function
	/**@param bool contains true if operation suceed, or false, if there were an error
	 * @param Value carries revision of newly stored document, in case of succes, otherwise it contains objects describing the error.
	 */
	using Callback = std::function<void(bool, json::Value)>;
	///Puts document to the database
	/**
	 * @param doc document to put. There is no callback, so program will not be notified, when operation fails
	 */
	void put(const json::Value &doc);
	///Puts document to the database
	/**
	 * @param doc document to put
	 * @param cb callback function
	 */
	void put(const json::Value &doc, Callback &&cb);
	///Replicate existing document
	void replicate(const json::Value &doc);

	///Called on exception
	/** You must rethrow and catch exception to process. */
	virtual void onException() noexcept;

	using ReadCallback = std::function<void(Document &doc)>;

	void get(const json::String docId, ReadCallback &&cb);

protected:
	enum Mode { //number specifies priority
		batch_put = 3,
		batch_replicate = 2,
		thread_exit = 1,
		batch_get = 0,
	};

	class Msg {
	public:
		json::Value doc;
		std::variant<Callback, ReadCallback> cb;
		Mode mode;
		Msg(const json::Value &doc, Callback &&cb, bool replication);
		Msg(const json::String &docid, ReadCallback &&cb);
		Msg():mode(thread_exit) {}

		bool operator<(const Msg &msg) const {
			return static_cast<int>(mode) < static_cast<int>(msg.mode);
		}

	};

	class priority_queue: public std::priority_queue<Msg> {
	public:
		using std::priority_queue<Msg>::priority_queue;
		auto front() const {return top();}
	};

	class Queue : public ondra_shared::MsgQueue<Msg, priority_queue > {
	public:
		std::recursive_mutex &getLock() {return this->lock;}
	};

	enum class RunState {
		not_started,
		running,
		exited
	};

	CouchDB &db;
	RunState run_state;
	std::thread thr;
	Queue queue;
	void worker();
	void init_worker();


};



}


#endif /* SRC_COUCHIT_SRC_COUCHIT_BATCH_H_ */
