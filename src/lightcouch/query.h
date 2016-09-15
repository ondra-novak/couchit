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
class Result;

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
	/**
	 * @note You @b have to call reset() everytime you need to build new query after exec(). This is not done automatically.
	 *
	 * @return chaining reference
	 */
	virtual QueryBase &reset();


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

	///Execute query and return the result
	/**
	 * @return result of query. You can pass the result to the Result object.
	 *
	 * @note if you need to reuse the query object, you need to call reset(), otherwise old setup can influent the next query
	 *
	 */
	virtual Result exec() const = 0;




	const Json json;

protected:


	mutable AutoArray<JSON::ConstValue,SmallAlloc<9> > curKeySet;
	mutable JSON::ConstValue startkey, endkey;
	mutable JSON::Container keys;
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
	mutable UrlLine urlline;
	typedef TextOut<UrlLine &, SmallAlloc<256> > UrlFormatter;


	JSON::Value args;

	natural viewFlags;

	JSON::ConstValue buildKey(ConstStringT<JSON::ConstValue> values) const;
	JSON::Container buildRangeKey(ConstStringT<JSON::ConstValue> values) const;



	void finishCurrent() const;;
	JSON::Value initArgs();
	static void appendCustomArg(UrlFormatter &fmt, ConstStrA key, ConstStrA value) ;
	void appendCustomArg(UrlFormatter &fmt, ConstStrA key, const JSON::INode * value ) const;

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

	virtual Result exec() const override;

protected:
	CouchDB &db;
	View viewDefinition;

	CouchDB &getDatabase() {return db;}
	const CouchDB &getDatabase() const {return db;}

};



class Row: public ConstValue {
public:
	///contains key
	const ConstValue key;
	///contains value
	const ConstValue value;
	///contains document - will be nil, if documents are not requested in the query
	const ConstValue doc;
	///contains source document ID
	const ConstValue id;
	///contains error information for this row
	const ConstValue error;

	Row(const ConstValue &jrow);

	///Returns 'true' if row exists (it is not error)
	bool exists() const {return error != null;}

};

enum MergeType {
	///combine rows from both results.
	mergeUnion,
	///make intersection - keep only rows with matching keys in both sides
	mergeIntersection,
	///make symetricall difference - keep only rows which doesn't match to each other
	mergeSymDiff,
	///remove rows from left results matching the keys from right result
	mergeMinus,
};


class Result: public ConstValue, public IteratorBase<ConstValue, Result> {
public:
	Result(const Json &json, ConstValue jsonResult);

	const ConstValue &getNext();
	const ConstValue &peek() const;
	bool hasItems() const;

	natural getTotal() const;
	natural getOffset() const;
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
	 * @param q query to execute. Function will feed query with
	 * keys and then executes it. Function is able to remove duplicate
	 * keys, because they should return same values
	 * @return Function returns new result.
	 *
	 * @param resultName name of member field where reuslts will be stored.
	 * Note that there can be always multiple results per single key.
	 * Function always creates an array, which will contain results even
	 * if there is only one result per record. Argument is ignored, if values
	 * aren't objects. When values are arrays, result is appended at the
	 * end of the array. If values are scalar, function converts them to arrays.
	 * (so there will be ['scalarValue',[joined result]]). This also happen,
	 * when resultName is empty for objects (so there will be [{result1},[joined result]])
	 *
	 *
	 * @note If you processed some record using the getNext(), function
	 * will skip these records. If you need to ensure, that records will be
	 * processed from the start, call rewind() before. After function returns,
	 * the function hasItems() will return false
	 */
	Result join(const JSON::Path foreignKey, QueryBase &q, ConstStrA resultName = ConstStrA());


	///Sorts result
	/** Function orders rows by compare function
	 *
	 * @param compareRowsFunction accepts two rows and returns -1, 0, or 1
	 * @param descending set true to reverse ordering
	 * @return
	 *
	 * The compareRowsFunction has following prototype
	 * @code
	 * int compareRowsFunction(const ConstValue &left, const ConstValue &right);
	 * @endcode
	 *
	 * Function should return
	 *
	 *  - -1 if left is less than right
	 *  - +1 if less is greater than right
	 *  - 0 if they are equal
	 *
	 * If descending is set to true, result will be reversed
	 */
	template<typename Fn>
	Result sort(Fn compareRowsFunction, bool descending = false) const;


	///Aggregate groups of rows
	/**
	 * @param compareRowsFunction function which compares rows. It accepts 2xRow and returns -1,0, or 1
	 * @param reduceFn function which reduces rows. It accepts ConstStringT<ConstValue> and returns ConstValue
	 * @param descending set true to reverse ordering
	 * @return new result
	 *
	 * The compareRowsFunction has following prototype
	 * @code
	 * int compareRowsFunction(const ConstValue &left, const ConstValue &right);
	 * @endcode
	 *
	 * Function should return
	 *
	 *  - -1 if left is less than right
	 *  - +1 if less is greater than right
	 *  - 0 if they are equal
	 *
	 * If descending is set to true, result will be reversed
	 *
	 * The reduceFn has following prototype
	 * @code
	 * ConstValue reduceFn(const ConstStringT<ConstValue> &rows)
	 * @endcode
	 *
	 * Function should return reduced row. Function should follow format of the row to allow the row
	 * process using Row object. It should have at least "key" and "value"
	 */
	template<typename CmpFn, typename ReduceFn>
	Result group(CmpFn compareRowsFunction, ReduceFn reduceFn, bool descending = false) const;



	///Merges two results into one
	/**
	 *
	 * @param other other result
	 * @param mergeFn function which accepts two rows and returns one of them.
	 * @return merged result
	 *
	 * The mergeFn has following prototype
	 * @code
	 * ConstValue mergeFn(const ConstValue &left,const ConstValue &right);
	 * @endcode
	 *
	 * mergeFn can return
	 *   - left row: left row will be put to the result and left result will advance. Note that you need
	 *    to return directly the argument left (references are compared)
	 *   - right row: right row will be put to the result and right result will advance. Note that you need
	 *    to return directly the argument right (references are compared)
	 *   - anything except null: row will be put to the result and both (left and right) results will advance
	 *   - null: nothing will be put to the result and both (left and right) results will advance
	 *
	 * If the argument left is null, there is no more items in the left result.
	 * If the argument right is null, there is no more items in the right result.
	 * Function will never called with both arguments set to null
	 *
	 * @note function expect, that results are already ordered. It doesn't perform ordering. You
	 * have to perform it before using sort()
	 */
	template<typename MergeFn>
	Result merge(const Result &other, const MergeFn &mergeFn) const;

protected:

	Json json;
	natural rdpos;
	natural total;
	natural offset;
	mutable ConstValue out;
};


}
#endif /* LIBS_LIGHTCOUCH_SRC_LIGHTCOUCH_QUERY_H_ */

