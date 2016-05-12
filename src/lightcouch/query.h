/*
 * query.h
 *
 *  Created on: 12. 3. 2016
 *      Author: ondra
 */

#ifndef LIBS_LIGHTCOUCH_SRC_LIGHTCOUCH_QUERY_H_
#define LIBS_LIGHTCOUCH_SRC_LIGHTCOUCH_QUERY_H_
#include <lightspeed/base/text/textOut.h>
#include "lightspeed/base/containers/constStr.h"
#include "lightspeed/base/containers/autoArray.h"
#include "lightspeed/base/memory/smallAlloc.h"
#include "lightspeed/base/containers/string.h"
#include <lightspeed/utils/json/json.h>

#include "view.h"

#include "object.h"


namespace LightCouch {

class CouchDB;
class View;

using namespace LightSpeed;



/// Query
/**
 * Select multiple keys
 *
 * @code
 * 	 Query q(...);
 * 	 q.select("aaa")(10);
 * 	 q.select("bbb")(13);
 * 	 q.exec();
 * @endcode
 *
 * Select range of subkeys
 *
 * @code
 *   q.select("aaa").all()  //select all documents starting with key "aaa"
 *   q.exec();
 * @endcode
 *
 * Select range of keys with prefix
 *
 * @code
 *
 *   q.select("aaa").prefix(); //select all documents with key that starts with "aaa"
 *   q.exec();
 *
 * @endcode
 *
 * Select a range
 *
 * @code
 *
 *   q.from("aaa")(10).to("aaa")(40);
 *   q.exec();
 */

class Query {
public:



	Query(CouchDB &db, const View &view);
	Query(const Query &other);
	virtual ~Query();

	///changes JSON factory
	/**
	 * @param json
	 */
	void setJsonFactory(JSON::PFactory json);


	///Resets query object state. Deletes prepared query
	Query &reset();


	///select one or more keys defined as json value
	/** You can call this function multiple times to specify (and return)
	 * multiple keys and values
	 *
	 * @param key
	 * @return reference to object query to create chain
	 *
	 * @note function removes prepared range, because select and range
	 * is mutually exclusive
	 */
	Query &selectKey(JCValue key);
	///define range search
	Query &fromKey(JCValue key);
	///define range search
	Query &toKey(JCValue key);

	template<typename T>
	Query &select(const T &key);

	template<typename T>
	Query &from(const T &key);

	template<typename T>
	Query &to(const T &key);


	enum MetaValue {
		///use instead value to declare, that any key can be there
		/** Metavalue any is allowed at last column. It also switches the Query
		 * into ranged query
		 */
		any,
		///use instead value to declare, that previous string is prefix
		/** Metavalue wildcard is allowed at last column. It also switches the
		 * Query into ranged query. Previous key should be string or isArray metavalue
		 */
		wildcard,
		///put this value after single value key condition to enforce putting field to the array
		/**
		 * @code
		 * q.select("aaa").exec(db);
		 * @endcode
		 * Above code will try to select string key "aaa"
		 *
		 * @code
		 * q.select("aaa")(Query::isArray).exec(db);
		 * @endcode
		 * Above code will try to select array key ["aaa"]
		 *
		 * This can be applied only for single value key. If multiple values are given, this
		 * flag is ignored.
		 *
		 * This metavalue can be combined with wildcard, but it is allowed before wildcard
		 * because the wildcard metavalue must be at the end.
		 *
		 * @code
		 * q.select("aaa")(Query::isArray)(Query::wildcard).exec(db);
		 * @endcode
		 *
		 * Above key will search for key ["aaa%"] (% is meant to be wildcard)
		 *
		 *
		 */

		isArray
	};

	Query &operator ()(ConstStrA key);
	Query &operator ()(natural key);
	Query &operator ()(integer key);
	Query &operator ()(int key);
	Query &operator ()(double key);
	Query &operator ()(JCValue key);
	Query &operator ()(bool key);
	Query &operator ()(const char *key);
	Query &operator ()(MetaValue metakey);

	JCValue exec(CouchDB &db);
	JCValue exec();

	///Apply reduce on the result
	/**
	 * @param level count of columns to left. Other columns will be reduced. Setting
	 * level to naturalNull cause disabling this function. Function overwrites
	 * setting specified by the view definition - just locally for this query only
	 */
	Query &group(natural level);


	///Maximum count of results
	Query &limit(natural limit);

	///Similar to MySQL defines offset and limit
	Query &limit(natural offset, natural limit);

	///Defines start position using docId and limit
	/** This function is valid for ranged query, when start doc is specified by
	 * its key. However, if there are multiple documents with the same key, you
	 * need to specify exact starting position by its docId. You can also specify
	 * count of document in the result
	 * @param docId docId must match to starting key
	 * @param limit count of results
	 * @return
	 */
	Query &limit(ConstStrA docId, natural limit);

	///Update view after query executes
	Query &updateAfter();

	///Do not update the view now
	Query &stale();

	///Enables returning results in reverse order.
	/**
	 * Reverses the order of the results. However, if you order is already reversed
	 * by the view definition, it makes reverse of reversed order, so result it original order.
	 * Calling this function more then once causes that order is always reversed thus every
	 * second call results to the original order
	 *
	 * in contrast to couchdb view API definition, you must not reverse startkey and endkey
	 * if descending is defined. This is done by default by the function exec(). That because
	 * object often creates ranged search silently
	 * @return
	 */
	Query &reverseOrder();

	template<typename T>
	Query &arg(ConstStrA key, T value);


	class Row {
	public:
		///contains key
		const JCValue key;
		///contains value
		const JCValue value;
		///contains document - will be nil, if documents are not requested in the query
		const JCValue doc;
		///contains source document ID
		const ConstStrA id;

		Row(const JCValue &jrow);
	};

	class Result: public IteratorBase<JCValue, Result> {
	public:
		Result(JCValue jsonResult);

		const JCValue &getNext();
		const JCValue &peek() const;
		bool hasItems() const;

		natural getTotal() const;
		natural getOffset() const;
		natural length() const;
		natural getRemain() const;
		void rewind();
	protected:

		JCValue rows;
		JSON::ConstIterator rowIter;
		mutable JCValue out;
		natural total;
		natural offset;
	};

	CouchDB &getDatabase() {return db;}
	const CouchDB &getDatabase() const {return db;}


	const JBuilder json;

protected:

	CouchDB &db;
	View viewDefinition;

	AutoArray<JSON::Value,SmallAlloc<9> > curKeySet;
	JSON::Value startkey, endkey, keys;
	enum Mode {
		mdKeys,
		mdStart,
		mdEnd,
	};

	enum StaleMode {
		smUpdate,
		smUpdateAfter,
		smStale
	};

	Mode mode;
	StaleMode staleMode;
	natural groupLevel;
	natural offset;
	natural maxlimit;
	bool descent;
	StringA offset_doc;
	bool forceArray;



	typedef AutoArrayStream<char> UrlLine;
	UrlLine urlline;
	typedef TextOut<UrlLine &, SmallAlloc<256> > UrlFormatter;


	JSON::Value args;

	JSON::Value buildKey(ConstStringT<JSON::Value> values);



	void finishCurrent();
	JSON::Value initArgs();
	void appendCustomArg(UrlFormatter &fmt, ConstStrA key, ConstStrA value);
	void appendCustomArg(UrlFormatter &fmt, ConstStrA key, const JSON::INode * value );

};

template<typename T>
inline Query& LightCouch::Query::select(const T& key) {
	finishCurrent();
	mode = mdKeys;
	return this->operator ()(key);
}

template<typename T>
inline Query& LightCouch::Query::from(const T& key) {
	finishCurrent();
	mode = mdStart;
	return this->operator ()(key);
}

template<typename T>
inline Query& LightCouch::Query::to(const T& key) {
	finishCurrent();
	mode = mdEnd;
	return this->operator ()(key);
}

template<typename T>
inline Query& LightCouch::Query::arg(ConstStrA key, T value) {
	initArgs()->add(key, factory(value));
	return *this;
}


}


#endif /* LIBS_LIGHTCOUCH_SRC_LIGHTCOUCH_QUERY_H_ */

