/*
 * changeDoc.h
 *
 *  Created on: 21. 3. 2016
 *      Author: ondra
 */

#ifndef LIBS_LIGHTCOUCH_SRC_LIGHTCOUCH_CHANGEDDOC_H_
#define LIBS_LIGHTCOUCH_SRC_LIGHTCOUCH_CHANGEDDOC_H_

#include <lightspeed/base/containers/constStr.h>
#include <lightspeed/base/containers/optional.h>


#include "view.h"


namespace LightCouch {

using namespace LightSpeed;


class CouchDB;
class Filter;

///Contains information about changed document
class ChangedDoc: public Value {

public:
	///sequence number
	const Value seqId;
	///document id
	const StringRef id;
	///list of revisions changed
	const Value revisions;
	///true, if document has been deleted
	const bool deleted;
	///document, if requested, or null if not available
	const Value doc;
	///Constructor.
	/**
	 * @param allData json record containing information about changed document. You can use
	 * result of Changes::getNext() or Changes::peek()
	 */
	ChangedDoc(const Value &allData);
};



///Collection that contains all changed documents (or references) received from ChangesSink
class Changes: public IteratorBase<Value, Changes> {
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

	///Receive next change
	/**
	 * @return next change in collection. It is returned as JSON object. You can convert it
	 * to the ChangedDoc object to easy access to all its fields
	 *
	 * Function also moves iteration to next change
	 */
	const Value &getNext();
	///Peek next change
	/** @return next change in collection. It is returned as JSON object. You can convert it
	 * to the ChangedDoc object to easy access to all its fields
	 *
	 * Function doesn't move to the next change
	 */
	const Value &peek() const;
	///Determines whether any change follows
	/**
	 * @retval true still a change is available
	 * @retval false no more changes in the collection
	 */
	bool hasItems() const;

	///Rewinds iterator, starting from the first changed document
	void rewind();

	///Retrieve whole result as array of rows;
	Value getAllChanges() const {return rows;}

	natural length() const {return rows.size();}

	natural getRemain() const {return rows.size() - pos;}

protected:

	Value rows;
	natural pos;
	natural sz;
	mutable Value vout;
};


///Receives changes from the couch database
class ChangesSink {
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
	ChangesSink(CouchDB &couchdb);


	///specifies sequence of last seen change.
	/**
	 * @param seqNumber last seen change. Listener will generate changes starting by this seq number.
	 *
	 * @return reference to this (chaining).
	 *
	 */
	ChangesSink& fromSeq(const Value &seqNumber);

	///Specifies timeout how long will listener wait for changes
	/**
	 *
	 * @param timeout According to CouchDB documentation, you can specify value between 0 and 60000.
	 *                Specifying 0 causes that listener can return an empty result.
	 *                You can also specify naturalNull to enable infinite wait. In this case,
	 *                heartbeat is used to keep connection up (using heartbeed is transparent for
	 *                the API)
	 * @return reference to this (chaining)
	 *
	 * @note default timeout is 0. You have to set timeout to perform longpool reading
	 */
	ChangesSink& setTimeout(natural timeout);

	///Sets filter
	/**
	 * @param filter filter reference.
	 * @return reference to this (chaining)
	 *
	 * @note function also resets any arguments of the filter
	 */
	ChangesSink& setFilter(const Filter &filter);
	///Removes any filter
	/** @return reference to this (chaining) */
	ChangesSink& unsetFilter();

	///Sets flags defined for Filter object, but without setting the filter
	/**
	 * Function allows to use Filter's flags without setting filter. Note that
	 * this function removes current filter. This works similar as using filter with empty url.
	 *
	 * @param flags flags
	 * @return reference to this (chaining)
	 */
	ChangesSink &setFilterFlags(natural flags);
	///Define argument of the filter
	/**
	 * @param key name of key
	 * @param value value of key. Only JSON allowed values are acceptable here
	 * @return
	 */
	template<typename T>
	ChangesSink& arg(StringRef key, T value);
	///Limit output for max count result
	/**
	 * @param count count of results to be in output. Specify naturalNull to remove limit
	 * @return
	 */
	ChangesSink& limit(natural count);


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
	 * You can easy chain sink with run operator to a function which receives all results one by one.
	 * This function will run until empty result received. In case that
	 * the timeout is set to naturalNull, the function will never return.
	 *
	 * The only way how to stop this cycle is to call cancelWait() which causes the throwing the CanceledException
	 * out of the function
	 *
	 * @param fn
	 */
	template<typename Fn>
	void operator>> (const Fn &fn);



protected:

	CouchDB &couchdb;
	Value seqNumber;
	natural outlimit;
	natural timeout;
	Optional<Filter> filter;
	Object filterArgs;

	atomic cancelState;

	friend class CouchDB;

};


template<typename T>
inline ChangesSink& LightCouch::ChangesSink::arg(StringRef key, T value) {

	filterArgs.set(key, value);
	return *this;

}

template<typename Fn>
inline void LightCouch::ChangesSink::operator >>(const Fn& fn) {
	bool repeat;
	do {
		repeat = false;
		Changes chs = exec();
		while (chs.hasItems()) {
			ChangedDoc chdoc = chs.getNext();
			fn(chdoc);
			repeat= true;
		}
	} while (repeat);
}



}
;


#endif /* LIBS_LIGHTCOUCH_SRC_LIGHTCOUCH_CHANGEDDOC_H_ */
