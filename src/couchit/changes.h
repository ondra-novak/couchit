/*
 * changeDoc.h
 *
 *  Created on: 21. 3. 2016
 *      Author: ondra
 */

#ifndef LIBS_LIGHTCOUCH_SRC_LIGHTCOUCH_CHANGEDDOC_H_
#define LIBS_LIGHTCOUCH_SRC_LIGHTCOUCH_CHANGEDDOC_H_

#include <couchit/changeevent.h>
#include  <memory>
#include  <mutex>


#include "minihttp/cancelFunction.h"

#include "view.h"
#include "couchDB.h"
#include "changeObserver.h"
#include "changeevent.h"

namespace std {
	class thread;
}



namespace couchit {



class CouchDB;
class Filter;



///Collection that contains all changed documents (or references) received from ChangesSink
class Changes: public json::Value  {
public:
	///Initialize the collection
	/**
	 * @param jsonResult result of ChangesSink::exec(). The ChangesSink::exec() function returns just ConstValue
	 * json, but you can easily convert it to Changes collection.
	 *
	 * @code
	 * Changes coll(sink.exec());
	 * @endcode
	 */
	Changes(Value jsonResult);

	Changes();

	///Receive next change
	/**
	 * @return next change in collection. It is returned as JSON object. You can convert it
	 * to the ChangedDoc object to easy access to all its fields
	 *
	 * Function also moves iteration to next change
	 */
	Value getNext();
	///Peek next change
	/** @return next change in collection. It is returned as JSON object. You can convert it
	 * to the ChangedDoc object to easy access to all its fields
	 *
	 * Function doesn't move to the next change
	 */
	Value peek() const;
	///Determines whether any change follows
	/**
	 * @retval true still a change is available
	 * @retval false no more changes in the collection
	 */
	bool hasItems() const;

	///Rewinds iterator, starting from the first changed document
	void rewind();

	///Retrieve whole result as array of rows;
	Value getAllChanges() const {return *this;}

	std::size_t length() const {return size();}

	std::size_t getRemain() const {return size() - pos;}

protected:

	std::size_t pos;
	std::size_t sz;
};

class ChangesFeedHandler {
public:
	virtual bool operator()(Value v) = 0;
};

///Receives changes from the couch database
class ChangesFeed {
public:
	///Construct the sink
	/**
	 * @param couchdb reference to the couchdb instance
	 *
	 * @note you can also create sink using CouchDB::createChangesSink().
	 *
	 * @note Each CouchDB instance is able to process one request at time. Note that while
	 * the sink is waiting for events, the referenced object is blocked and it is unable to process
	 * other requests.
	 */
	ChangesFeed(CouchDB &couchdb);


	///specifies sequence of last seen change.
	/**
	 * @param seqNumber last seen change. Listener will generate changes starting by this seq number.
	 *
	 * @return reference to this (chaining).
	 *
	 */
	ChangesFeed& fromSeq(const Value &seqNumber);

	///Alias to the function fromSeq()
	ChangesFeed& since(const Value &seqNumber) {return fromSeq(seqNumber);}

	///Specifies timeout how long will listener wait for changes
	/**
	 *
	 * @param timeout According to CouchDB documentation, you can specify value between 0 and 60000.
	 *                Specifying 0 causes that listener can return an empty result.
	 *                You can also specify ((std::size_t)-1) to enable infinite wait. In this case,
	 *                heartbeat is used to keep connection up (using heartbeed is transparent for
	 *                the API)
	 * @return reference to this (chaining)
	 *
	 * @note default timeout is 0. You have to set timeout to perform longpool reading
	 */
	ChangesFeed& setTimeout(std::size_t timeout);

	///Sets filter
	/**
	 * @param filter filter reference.
	 * @return reference to this (chaining)
	 *
	 * @note function also resets any arguments of the filter
	 */
	ChangesFeed& setFilter(const Filter &filter);
	///Removes any filter
	/** @return reference to this (chaining) */
	ChangesFeed& unsetFilter();

	///Sets flags defined for Filter object, but without setting the filter
	/**
	 * Function allows to use Filter's flags without setting filter. Note that
	 * this function removes current filter. This works similar as using filter with empty url.
	 *
	 * @param flags flags
	 * @return reference to this (chaining)
	 */
	ChangesFeed &setFilterFlags(std::size_t flags);
	///Define argument of the filter
	/**
	 * @param key name of key
	 * @param value value of key. Only JSON allowed values are acceptable here
	 * @return
	 */
	template<typename T>
	ChangesFeed& arg(StrViewA key, T value);
	///Limit output for max count result
	/**
	 * @param count count of results to be in output. Specify ((std::size_t)-1) to remove limit
	 * @return
	 */
	ChangesFeed& limit(std::size_t count);


	///Applies for continuous streams with timeout set to infinity
	/**This allows to restart stream after specified count of milliseconds
	 *
	 * This can handle various bug in couchdb engine, when stream is not fully recovered when
	 * one of the shards are down for a while. This can cause, that data from the recovered shard
	 * are not streamed. With this feature, stream is restarted after some time allowing to
	 * reconnect recovered shard.
	 *
	 *
	 * @param ms number of milliseconds of interval of restarting of the stream. Default is 30s
	 */
	ChangesFeed& restartAfter(std::size_t ms);

	///Executes the operation - reads all changes
	/**
	 *
	 * @return Changes collection which can be iterated for all documents that are subject of change.
	 *  If there were no change or when timeout ellapsed, return is empty collection.
	 *
	 * @exception CanceledException In reaction to calling the function cancelWait(), this
	 * function can return CanceledException. This can also happen, when timeout is 0 and
	 * cancelWait() is called before the function exec(). Throwing exception also causes
	 *  that cancel state is reset, so any next call of exec() will not throw the exception
	 *
	 *
	 *
	 */
	Changes exec();


	void continuous(ChangesFeedHandler &fn);

	///Cancels any waiting or future waiting
	/** This function can be called from another thread. It causes, that
	 *  waiting operation will be canceled. Function cancels current or
	 *  first future waiting. If waiting is canceled, the function exec()
	 *  throws the exception CanceledException.
	 *
	 *  @note There can be small delay between calling this function and throwing the exception from
	 *  the exec().
	 */
	void cancelWait();

	///Process all results using specified function
	/**
	 * Starts to monitor changes calling the function for every change that happen.
	 *
	 * Monitoring stops when there is no change for specified timeout or by function
	 * cancelWait() depend on which happened first.
	 *
	 * @param fn function called with every change
	 */
	template<typename Fn>
	void operator>> (const Fn &fn) {
		class FnHndl: public ChangesFeedHandler {
		public:
			Fn fn;
			FnHndl(const Fn &fn):fn(fn) {}
			virtual bool operator()(Value v) {
				return fn(v);
			}
		};
		FnHndl h(fn);
		continuous(h);
	}


	Value getLastSeq() const {return seqNumber;}

	ChangesFeed &includeDocs(bool v=true) {
		forceIncludeDocs = v;
		return *this;
	}
	ChangesFeed &reversedOrder(bool v=true) {
		forceReversed = v;
		return *this;
	}

	bool wasCanceled() const {
		return state.wasCanceledState;
	}

	///specifies list of docs to monitor for changes
	/**
	 * @param docIds string or array of strings.The string contains document id
	 *  to monitor for changes
	 * @return this feed
	 *
	 * @note functions setFilter and forDocs are mutualy exclusive. If both
	 * are set, filter has priority.
	 */
	ChangesFeed &forDocs(Value docIds) {
		docFilter = docIds;
		return *this;
	}


	///Sets IO-Timeout
	/**defines timeout waiting for any data from the database. Default value is 2 minutes. However it can
	 * be useful to specify longer timeout especially when filtering is used and it is expected that processing
	 * the filter takes a long time to response
	 *
	 * @param iotm
	 * @return
	 */
	ChangesFeed &setIOTimeout(std::size_t iotm) {
		iotimeout = iotm;
		return *this;
	}


	CouchDB &getDB() const {return couchdb;}
protected:

	struct State {
		CouchDB::Connection *curConn = nullptr;
		bool canceled = false;
		bool wasCanceledState = false;
		mutable std::recursive_mutex initLock;

		State() {}
		State(const State &) {}
		State(State &&) {}

		void cancelEpilog();
		void errorEpilog();
		void finishEpilog();
		void cancelWait();

	};



	CouchDB &couchdb;
	Value seqNumber;
	std::size_t outlimit = (std::size_t)-1;
	std::size_t timeout = (std::size_t)-1;
	std::size_t restart_after = 30000;
	std::size_t iotimeout = 120000;
	Filter filter = Filter("");
	Value docFilter;
	Value selectorFilter;
	Object filterArgs;
	bool forceIncludeDocs = false;
	bool forceReversed = false;
	bool filterInUse = false;


	State state;


	friend class CouchDB;


};




template<typename T>
inline ChangesFeed& couchit::ChangesFeed::arg(StrViewA key, T value) {

	filterArgs.set(key, value);
	return *this;

}

class IChangeEventObserver;


class ChangesDistributor {
public:

	typedef std::function<void(IChangeEventObserver *)>  Deleter;
	typedef std::unique_ptr<IChangeEventObserver, Deleter>PObserver;

	typedef const IChangeEventObserver *RegistrationID;
	constexpr static RegistrationID noreg = nullptr;

	explicit ChangesDistributor(CouchDB &db, bool include_docs = true);
	ChangesDistributor(CouchDB &db, CouchDB::ChangeFeedState &&feedState);


	///Add observer as reference
	/** Using refernece means, that caller keeps ownership. Once the distributor is destroyed, the observer
	 * is removed but not destroyed
	 * @param observer reference to observer
	 * @return registration identification for the function remove()
	 */
	RegistrationID add(IChangeEventObserver &observer);
	///Add observer using unique pointer with custom deleter
	/**
	 * @param observer unique pointer to observer. The function moves the observer with the custom deleter
	 * and eventually calls deleter once the distributor is destroyed
	 * @return registration identification for the function remove()
	 */
	RegistrationID add(PObserver &&observer);
	///Add observer using uniqoe pointer with standard deletion
	/**
	* @param observer unique pointer to observer. The function moves the observer to the distributor
	* and eventually destroyes observer with the distributor
	* @return registration identification for the function remove()
	*/
	RegistrationID add(std::unique_ptr<IChangeEventObserver> &&observer);

	///Removes the observer identified by its registration id
	/**
	 * @param reg_id observer's registration id
	 * @note The observer is destroyed unless it is regstered as reference
	 */
	void remove(RegistrationID reg_id);

	///Retrieves the initial updateSeq calculated using all observers.
	/**
	 * @return updateSeq.
	 *
	 * @note return value is primarily usefull for the function since() because it can eventually return
	 * "0" which means to start over, or "now" which means start from here.
	 */
	Value getInitialUpdateSeq() const;

	///Synchronizes distributor with current database state
	/** Function must be called before the service is installed. The function
	 * blocks execution until the distribution is synchronized.
	 *
	 */
	void sync();

	///Runs the distributor in the current thread
	/** To leave this function, one must call cancelWait() */
	void run();


	///Runs as service thread (deprecated)
	void runService();
	///Runs as service thread
	/**
	 * @param onError function called when exception happen.
	 * Function must return true to retry, or false to exit thread
	 */
	void runService(std::function<bool()> onError);

	///stops the service thread
	void stopService();

	virtual ~ChangesDistributor();

	template<typename Fn>
	RegistrationID addFn(Fn &&fn) {
		return add(std::unique_ptr<IChangeEventObserver>(new ChangeObserverFromFn<Fn>(std::forward<Fn>(fn))));
	}

	virtual void onException();

	CouchDB &getDB() const {return db;}

protected:

	CouchDB &db;
	CouchDB::ChangeFeedState feedState;
	std::vector<PObserver> observers;
	std::unique_ptr<std::thread> thr;
	bool exit = false;
	mutable std::recursive_mutex lock;

	class Distributor;


};
}



#endif /* LIBS_LIGHTCOUCH_SRC_LIGHTCOUCH_CHANGEDDOC_H_ */
