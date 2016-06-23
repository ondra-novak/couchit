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
 * @endcode
 */

class QueryBase {
public:



	QueryBase(const Json &json, natural viewFlags);
	QueryBase(const QueryBase &other);
	virtual ~QueryBase();



	///Resets query object state. Deletes prepared query
	QueryBase &reset();


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
	QueryBase &selectKey(ConstValue key);
	///define range search
	QueryBase &fromKey(ConstValue key);
	///define range search
	QueryBase &toKey(ConstValue key);

	template<typename T>
	QueryBase &select(const T &key);

	template<typename T>
	QueryBase &from(const T &key);

	template<typename T>
	QueryBase &to(const T &key);


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

	QueryBase &operator ()(ConstStrA key);
	QueryBase &operator ()(natural key);
	QueryBase &operator ()(integer key);
	QueryBase &operator ()(int key);
	QueryBase &operator ()(double key);
	QueryBase &operator ()(ConstValue key);
	QueryBase &operator ()(bool key);
	QueryBase &operator ()(const char *key);
	QueryBase &operator ()(MetaValue metakey);


	///Apply reduce on the result
	/**
	 * @param level count of columns to left. Other columns will be reduced. Setting
	 * level to naturalNull cause disabling this function. Function overwrites
	 * setting specified by the view definition - just locally for this query only
	 */
	QueryBase &group(natural level);


	///Maximum count of results
	QueryBase &limit(natural limit);

	///Similar to MySQL defines offset and limit
	QueryBase &limit(natural offset, natural limit);

	///Defines start position using docId and limit
	/** This function is valid for ranged query, when start doc is specified by
	 * its key. However, if there are multiple documents with the same key, you
	 * need to specify exact starting position by its docId. You can also specify
	 * count of document in the result
	 * @param docId docId must match to starting key
	 * @param limit count of results
	 * @return
	 */
	QueryBase &limit(ConstStrA docId, natural limit);

	///Update view after query executes
	QueryBase &updateAfter();

	///Do not update the view now
	QueryBase &stale();

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
	QueryBase &reverseOrder();

	template<typename T>
	QueryBase &arg(ConstStrA key, T value);

	virtual ConstValue exec() = 0;


	class Row {
	public:
		///contains key
		const ConstValue key;
		///contains value
		const ConstValue value;
		///contains document - will be nil, if documents are not requested in the query
		const ConstValue doc;
		///contains source document ID
		const ConstStrA id;

		Row(const ConstValue &jrow);
	};

	enum MergeType {
		///merge two results keep both sides, if equal, merge values, right has priority
		mergeUnion,
		///merge two results keep both sides, if equal, duplicated records created
		mergeUnionAll,
		///merge two results, keep only equal, merge values, right has priority
		mergeEqual,
		///merge two results, keep only equal, duplicate records
		mergeEqualAll,
		///merge two results, keep not equal only
		mergeNotEqual,
		///merge two results, remove all keys from left, if they have result at right
		mergeRemoveFromLeft,
		///merge two results, remove all keys from right, if they have result at left
		mergeRemoveFromRight

	};

	class Result: public IteratorBase<ConstValue, Result> {
	public:
		Result(ConstValue jsonResult);

		const ConstValue &getNext();
		const ConstValue &peek() const;
		bool hasItems() const;

		natural getTotal() const;
		natural getOffset() const;
		natural length() const;
		natural getRemain() const;
		void rewind();

		///Join to the result
		/**
		 * Function collects foreign keys and executes query with the
		 * keys. After query is executed, new record are stored with
		 * original records.
		 *
		 * @param foreignKey path to the foreign key. If key doesn't exist,
		 *  function skips it (in this case, nether member will be created).
		 *
		 * @param resultName name of member field where reuslts will be stored.
		 * Note that there can be always multiple results per single key.
		 * Function always creates an array, which will contain results even
		 * if there is only one result per record.
		 *
		 * @param q query to execute. Function will feed query with
		 * keys and then executes it. Function is able to remove duplicate
		 * keys, because they should return same values
		 * @return Function returns new result.
		 *
		 * @note If you processed some record using the getNext(), function
		 * will skip these records. If you need to ensure, that records will be
		 * processed from the start, call rewind() before. After function returns,
		 * the function hasItems() will return false
		 */
		Result join(const Path foreignKey, ConstStrA resultName, Query &q);

		///Merges two results
		/** Processe two results and nerges them key by key
		 *
		 * Merge uses only keys. It expects, that they are ordered. However
		 * there is small difference between LightCouch orderibg and CouchDB's
		 * ordering (LightCouch always order members of the objects alphabetically)
		 *
		 *
		 * @param mergeType type of merge
		 * @param other other result
		 * @return merged result
		 */
		Result &merge(MergeType mergeType, Result &other);

	protected:

		ConstValue rows;
		JSON::ConstIterator rowIter;
		mutable ConstValue out;
		natural total;
		natural offset;
	};



	const Json json;

protected:


	AutoArray<JSON::ConstValue,SmallAlloc<9> > curKeySet;
	JSON::ConstValue startkey, endkey;
	JSON::Container keys;
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

	natural viewFlags;

	JSON::ConstValue buildKey(ConstStringT<JSON::ConstValue> values);
	JSON::Container buildRangeKey(ConstStringT<JSON::ConstValue> values);



	void finishCurrent();
	JSON::Value initArgs();
	void appendCustomArg(UrlFormatter &fmt, ConstStrA key, ConstStrA value);
	void appendCustomArg(UrlFormatter &fmt, ConstStrA key, const JSON::INode * value );

};

template<typename T>
inline QueryBase& LightCouch::QueryBase::select(const T& key) {
	finishCurrent();
	mode = mdKeys;
	return this->operator ()(key);
}

template<typename T>
inline QueryBase& LightCouch::QueryBase::from(const T& key) {
	finishCurrent();
	mode = mdStart;
	return this->operator ()(key);
}

template<typename T>
inline QueryBase& LightCouch::QueryBase::to(const T& key) {
	finishCurrent();
	mode = mdEnd;
	return this->operator ()(key);
}

template<typename T>
inline QueryBase& LightCouch::QueryBase::arg(ConstStrA key, T value) {
	initArgs()->add(key, factory(value));
	return *this;
}

class Query: public QueryBase {
public:
	Query(CouchDB &db, const View &view);
	Query(const Query &other);
	virtual ~Query();

	ConstValue exec(CouchDB &db);
	ConstValue exec();

protected:
	CouchDB &db;
	View viewDefinition;

	CouchDB &getDatabase() {return db;}
	const CouchDB &getDatabase() const {return db;}

};


}


#endif /* LIBS_LIGHTCOUCH_SRC_LIGHTCOUCH_QUERY_H_ */

