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
#include <imtjson/value.h>
#include <shared/msgqueue.h>


namespace couchit {

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

protected:
	struct Msg {
		json::Value doc;
		Callback cb;
		bool replication;
		Msg(const json::Value &doc, Callback &&cb, bool replication);
	};

	class Queue : public ondra_shared::MsgQueue<Msg> {
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



};



}


#endif /* SRC_COUCHIT_SRC_COUCHIT_BATCH_H_ */
