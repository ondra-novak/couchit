#pragma once

#include "iqueryable.h"

namespace LightCouch {


class Query {
public:

	Query(const View &view, IQueryableObject &qau);


	static const natural docIdFromGetKey = 1;
	static const natural excludeEnd = 2;

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

	///Query for specified range
	/**
	 * @param from define lower bound of the range. You should always specify
	 *  the lower bound regardless on, how the View is ordered
	 * @param to define upper bound of the range. You should always specify
	 *  the lower bound regardless on, how the View is ordered
	 * @param flags combination of following flags
	 *
	 *  * @b docIdFromGetKey Function calls getKey on both bounds to retrieve
	 *  document-id which more precisely specifies lower and upper bound
	 *  in situation, when there are multiple values per key. If you want
	 *  to set these document-ids, use Value::setKey. To un-set the document-id, use
	 *  empty string instead document-id to disable this feature for particular key.
	 *  * @b excludeEnd exclude ending key from the result.
	 *
	 * @return
	 */
	Query &range(const Value &from, const Value &to, natural flags = 0);

	Query &range(const Value &from, const StrViewA &fromDoc, const Value &to, const StrViewA &toDoc, bool exclusiveEnd = false);


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
	Query &arg(const StrViewA &argname, const 	Value &value);
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

	Query &reset();

	///Disable caching for this query
	/** If the query is so unique or if the source is changing often, you
	 * can disable the cache, so the result will not waste a space in the cache.
	 */
	Query &nocache();

	Value exec();



	static const Value minKey;
	static const Value maxKey;
	static const String maxString;
	static const String minString;

protected:
	QueryRequest request;
	IQueryableObject &qao;


};

class Result: public Value {
public:

	Result(const Value &result);

	natural getTotal() const {return total;}
	natural getOffset() const {return offset;}

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

	template<typename BindFn>
	Value join(Query &q, const StrViewA & name, natural flags, BindFn bindFn);

	class Iterator: public json::ValueIterator {
	public:
		Iterator(const json::ValueIterator &src):json::ValueIterator(src) {}
		Iterator(json::ValueIterator &&src):json::ValueIterator(std::move(src)) {}
		Value operator *() const {return Value(json::ValueIterator::operator *());}
	};
	Iterator begin() const {return Value::begin();}
	Iterator end() const {return Value::end();}

	bool hasItems() const {return pos < cnt;}
	Value getNext() {return operator[](pos++);}
	Value peek() const {return operator[](pos);}
	void rewind() {pos = 0;}

protected:

	natural total;
	natural offset;
	natural pos;
	natural cnt;

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



}

