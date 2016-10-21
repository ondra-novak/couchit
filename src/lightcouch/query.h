#pragma once

#include "iqueryable.h"

namespace LightCouch {


class Query {
public:

	Query(const View &view, const IQueryableObject &qau);


	template<typename ... Args>

	///Select single key
	/**
	 * @param v single key to select. You cannot call this function by multiple
	 * times to achieve multiple keys search. Each call deletes previous key
	 */
	Query &key(const Value &v);
	///Select in list of keys
	/**
	 * @param v The argument MUST be array
	 */
	Query &keys(const Value &v);
	///Define range search lower bound side of the range
	/**
	 * @param v single key defining lower bound (from < to)
	 */
	Query &from(const Value &v);
	///Define range search upper bound side of the range
	/**
	 * @param v single key defining upper bound (from < to)
	 */
	Query &to(const Value &v);
	///Define range search lower bound side of the range
	/**
	 * @param v single key defining lower bound (from < to)
	 * @param docId specifies starting document by its id in case that lowest key
	 *  returns multiple documents. T
	 */
	Query &from(const Value &v, const String &docId);
	///Define range search upper bound side of the range
	/**
	 * @param v single key defining upper bound (from < to)
	 * @param docId specifies ending document by its id in case that highest key
	 *  returns multiple documents. T
	 */
	Query &to(const Value &v, const String &docId);
	///Define range search upper bound side of the range
	/**
	 * @param v single key defining upper bound (from < to). Upper bound
	 * is excluded
	 * @note function works well if ordering is not reserved. Otherwise
	 * it will include lower bound
	 */
	Query &to_exclusive(const Value &v);
	///Defines ranged search as prefix
	/** The function returns all values starting with specified key. It
	 * is usefull for multicollumn keys.
	 * @param v Must be multicollumn key, so you have to put it into an array.
	 *  You need to put there less columns then query returns as keys
	 */
	Query &prefixKey(const Value &v);
	///Defines ranged search as string prefix
	/**
	 * @param v the argument must be single string or an array, where the
	 * latest item is string. The function use the string as prefix and
	 * will search for all keys, that are staring by specified prefix at given
	 * position
	 */
	Query &prefixString(const Value &v);
	///Change offset (default is 0)
	Query &offset(natural offset);
	///Change limit (default is unlimited)
	Query &limit(natural limit);
	///Append argument to postprocessing
	Query &arg(const StringRef &argname, const Value *value);
	///Reverse order
	/** The view can be declared with already reversed order. In this
	 * case the function reverses already reversed order which results to
	 * original order
	 */
	Query &reversedOrder();
	///Disables reduce
	Query &noreduce();
	///enable (force) reduce (returns single row)
	Query &reduceAll();
	///reduce per key (duplicated keys will be reduced)
	Query &group();
	///reduce per if duplicated keys up to count of columns (levels)
	Query &groupLevel(natural level);
	///Disables default ordering
	/** Result may be unordered */
	Query &nosort();

	Value exec();



protected:
	QueryRequest request;
	const IQueryableObject &qao;


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


	///Exclusive join
	/** From original result removes all rows which exists on dependant result, regardless on how many are there.
	 * This flags automatically turn on joinMissingRows
	 */
	static const natural joinExclude = 3 | joinMissingRows;


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
	Result join(Query &q, const StringRef & name, natural flags, BindFn bindFn);

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


	using Value::begin;
	using Value::end;

protected:

	natural rdpos;
	natural total;
	natural offset;
	mutable Value out;
};




}
