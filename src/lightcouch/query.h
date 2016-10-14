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

#include "view.h"



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



	QueryBase(natural viewFlags);
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
	QueryBase &selectKey(Value key);
	///define range search
	QueryBase &fromKey(Value key);
	///define range search
	QueryBase &toKey(Value key);

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

	QueryBase &operator ()(const StringRef & key);
	QueryBase &operator ()(natural key);
	QueryBase &operator ()(integer key);
	QueryBase &operator ()(int key);
	QueryBase &operator ()(double key);
	QueryBase &operator ()(Value key);
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
	QueryBase &limit(const StringRef & docId, natural limit);

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
	QueryBase &arg(const StringRef & key, T value);

	///Execute query and return the result
	/**
	 * @return result of query. You can pass the result to the Result object.
	 *
	 * @note if you need to reuse the query object, you need to call reset(), otherwise old setup can influent the next query
	 *
	 */
	virtual Result exec() const = 0;





protected:


	mutable AutoArray<Value,SmallAlloc<9> > curKeySet;
	mutable Value startkey, endkey;
	mutable Value keys;
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


	Object args;

	natural viewFlags;

	Value buildKey(ConstStringT<Value> values) const;
	Value buildRangeKey(ConstStringT<Value> values) const;



	void finishCurrent() const;;
	static void appendCustomArg(UrlFormatter &fmt, const StringRef & key, const StringRef & value) ;
	void appendCustomArg(UrlFormatter &fmt, const StringRef & key, const Value value ) const;

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
inline QueryBase& LightCouch::QueryBase::arg(const StringRef & key, T value) {
	args.set(key, value);
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



class Row: public Value {
public:
	///contains key
	const Value key;
	///contains value
	const Value value;
	///contains document - will be nil, if documents are not requested in the query
	const Value doc;
	///contains source document ID
	const Value id;
	///contains error information for this row
	const Value error;

	Row(const Value &jrow);

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


class Result: public Value, public IteratorBase<Value, Result> {
public:
	Result(Value jsonResult);

	const Value &getNext();
	const Value &peek() const;
	bool hasItems() const;

	natural getTotal() const;
	natural getOffset() const;
	natural getRemain() const;
	void rewind();

	///Join operation will use only first row of the dependent result (skipping additional rows returned)
	/** Due to nature of CouchDB's view, the first row is also minimal row in order of CouchDB's collation
	 *
	 * @note this option is default if not specified any other option
	 *
	 * */
	static const natural joinFirstRow = 0;
	///Join operation will use last row of the dependent result (previous rows skipped)
	/** Due to nature of CouchDB's view, the first row is also minimal row in order of CouchDB's collation
	 *
	 * @note this option supresses joinFirstRow
	 *
	 * */
	static const natural joinLastRow = 1;
	///Join operation will put all rows to the result
	/** Note that this flag switches to use arrays. Instead direct value, there will be always array (even
	 * if there is one result)
	 *
	 * @note this option suppresses joinFirstRow and joinLastRow
	 */
	static const natural joinAllRows = 2;
	///Join operation will include rows which missing in dependant result
	/** These such rows will be copied directly from the source result without modification. So the
	 * target field is undefined there.
	 *
	 * @note can be combined with ether joinFirstRow or joinLastRow or joinAllRows
	 */
	static const natural joinMissingRows= 4;


	///Join other view with result
	/**
	 * CouchDB inherently doesn't support joining views. You need to use this function
	 * to extend the result by values from the other view. Function performs extra
	 * lookup to CouchDB and distributes values to the row through bind condition.
	 *
	 * @param q query object containing a view to query. Function calls reset() on the query to ensure
	 * that query is empty and ready to use. You can use Query or LocalView::Query object as any other
	 * object inherited from QueryBase
	 *
	 * @param name Name of the result. Each row contains at least two fields: "key" and "value" and results
	 * generated by map function also contains "id". This value specifies name of the field,
	 * where "value" part of joined result will put.
	 *
	 * @param flags Combination of flags: joinFirstRow, joinLastRow, joinAllRows, joinMissingRows
	 *
	 * @param bindFn defines bind function. See below

	 * @return combined result
	 *
	 * Bind function has following prototype
	 * @code
	 * Value bindFn(const Value &row)
	 * @endcode
	 *
	 * The BindFn is called for every row in the current result. It should return foreign key which is
	 * be used to query other view. It is not required that value must be from the row. It can be
	 * also calculated somehow or generated by mixing row data with some external data.
	 *
	 * The function can return null to skip the row. Otherwise returned result is used directly as key to the
	 * query.
	 *
	 */
	template<typename BindFn>
	Result join(QueryBase &q, const StringRef & name, natural flags, BindFn bindFn);

	///Sorts result
	/** Function orders rows by compare function
	 *
	 * @param compareRowsFunction accepts two rows and returns -1, 0, or 1
	 * @param descending set true to reverse ordering
	 * @return
	 *
	 * The compareRowsFunction has following prototype
	 * @code
	 * int compareRowsFunction(const Value &left, const Value &right);
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
	 * @param reduceFn function which reduces rows. It accepts ConstStringT<Value> and returns Value
	 * @param descending set true to reverse ordering
	 * @return new result
	 *
	 * The compareRowsFunction has following prototype
	 * @code
	 * int compareRowsFunction(const Value &left, const Value &right);
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
	 * Value reduceFn(const ConstStringT<Value> &rows)
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
	 * Value mergeFn(const Value &left,const Value &right);
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
	Result merge(const Result &other, MergeFn mergeFn) const;

protected:

	natural rdpos;
	natural total;
	natural offset;
	mutable Value out;
};


}


#endif /* LIBS_LIGHTCOUCH_SRC_LIGHTCOUCH_QUERY_H_ */

