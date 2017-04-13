#pragma once

#include "iqueryable.h"
#include <unordered_map>

namespace couchit {


template<typename A, typename B, typename C> class JoinedQuery;




class Query {
public:

	Query(const View &view, IQueryableObject &qau);


	static const std::size_t docIdFromGetKey = 1;
	static const std::size_t excludeEnd = 2;

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
	Query &range(const Value &from, const Value &to, std::size_t flags = 0);

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
	Query &offset(std::size_t offset);
	///Change limit (default is unlimited)
	Query &limit(std::size_t limit);
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
	Query &groupLevel(std::size_t level);
	///Disables default ordering
	/** Result may be unordered */
	Query &nosort();

	Query &reset();

	///Disable caching for this query
	/** If the query is so unique or if the source is changing often, you
	 * can disable the cache, so the result will not waste a space in the cache.
	 */
	Query &nocache();

	Value exec() const;

	Value exec(const QueryRequest &request) const;


	///Join two queries into one
	/**
	 * @param rightSide other query
	 * @param bindFn function Value(Row) - function need to extract foreign key from the row
	 * @param agregFn function Value(Array) - argument contains array of rows returned for given key, the function must aggregate the rows into single value (or row)
	 * @param mergeFn function Row(Row,Value) - arguments contains original Row and aggregated result
	 *                                        for matching foreign key.
	 *                                        The function need to merge row with the matching
	 *                                        value and return value which is put to the result
	 *                                        as final row. Function can return "undefined"
	 *                                        to remove row from the result
	 * @return query
	 */
	template<typename BindFn, typename AgregFn, typename MergeFn>
	JoinedQuery<BindFn, AgregFn, MergeFn> join(const Query &rightSide,
			const BindFn &bindFn,
			const AgregFn &agregFn,
			const MergeFn &mergeFn);


	static const Value minKey;
	static const Value maxKey;
	static const String maxString;
	static const String minString;

protected:
	QueryRequest request;
	IQueryableObject &qao;

	template<typename A, typename B, typename C> friend class JoinedQuery;

};

class Result: public Value {
public:

	Result(const Value &result);
	Result(const Value &resultArray, const Value &total, const Value &offset);

	std::size_t getTotal() const {return total;}
	std::size_t getOffset() const {return offset;}

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

	std::size_t total;
	std::size_t offset;
	std::size_t pos;
	std::size_t cnt;

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

	Row(const PValue &jrow);
	Row(const Value &jrow);

	///Returns 'true' if row exists (it is not error)
	bool exists() const {return error != null;}


};

namespace _details {
	template<typename T, std::size_t n> class NStore {
	public:
		T data[n];
		const std::size_t count = n;
		typedef T Type;

		NStore(const std::initializer_list<T> &list) {
			std::size_t pos = 0;
			for(auto &&x : list) {
				if (pos >=n) break;
				data[pos++] = x;
			}
		}
		NStore() {}

		operator const T &() const {return data[0];}
		operator T &() {return data[0];}
	};

	template<typename> class DetectFkType {
	public:
		typedef NStore<Value, 1> Type;
		static Type conv(const Value &v) {return Type({v});}
	};

	template<std::size_t n> class DetectFkType<NStore<Value,n> > {
	public:
		typedef NStore<Value,n> Type;
		static Type conv(const Type &v) {return v;}
	};
}

///If you need to return multiple foreign keys from the BindFn of the JoinedQuery
template<std::size_t n> using MultiFKey = _details::NStore<Value, n>;



///Query object which joins two view into single result
/**
 * Join operation involves two requests to the database, however, the
 * class also support queries above local views, so one or both requests can be
 * processed locally.
 *
 * The class need three functions
 *
 * @tparam BindFn Bind funcion Value(Value), it receives row from the results of the
 *   first query and it should extract a foreign key. Function can return undefined value
 *   to skip the row in the request to the other view.
 *   (Alternatively: Function can return StringView<Value> if there are multiple
 *   foreign keys for given row. Note that because only view is returned,
 *   the function need to store it somewhere else. Furtunately the returned value is immediately
 *   processed and it is no longer needed when the function is called again)
 *
 *
 * @tparam AgrFn Aggregate function Value(Array &). Function receives array of results
 *    matching a single foreign key. Function should agregate the result and return a signle
 *    value which is considered as result for the given foreign key. Each item
 *    of the Array (as argument of the function) is row from the other query.
 *
 * @tparam MergeFn Merge function. The function receives two arguments, first argument
 *   is row from the first query, second argument is matching row from the other query (as it was
 *   generated by AgrFn). The Function need to return final row, which is
 *   included to the result. If there is no result for the other query, second argument is undefined.
 *   The function may return undefined to remove whole row from the result.
 *
 *
 */
template<typename BindFn, typename AgrFn, typename MergeFn>
class JoinedQuery: public Query {
public:
	JoinedQuery(const Query &left, const Query &right, const BindFn &bindFn, const AgrFn &agrFn, const MergeFn &mergeFn)
		:Query(left.request.view,qobj),lq(left),rq(right),qobj(*this,bindFn,agrFn,mergeFn) {}
	JoinedQuery(const JoinedQuery &other)
		:Query(other.lq.request.view,qobj),lq(other.lq),rq(other.rq),qobj(other.qobj) {}

protected:



	class QObj: public IQueryableObject {
	public:
		QObj(JoinedQuery &owner, const BindFn &bindFn, const AgrFn &agrFn, const MergeFn &mergeFn)
				:owner(owner),bindFn(bindFn),agrFn(agrFn),mergeFn(mergeFn) {}
		virtual Value executeQuery(const QueryRequest &r);

		typedef typename _details::DetectFkType<typename std::result_of<BindFn(Value)>::type>::Type KeyType;
		typedef KeyType ResultType;
		typedef std::pair<std::size_t, int> IndexType;

		JoinedQuery &owner;
		BindFn bindFn;
		AgrFn agrFn;
		MergeFn mergeFn;





		typedef std::unordered_multimap<Value, IndexType> KeyAtIndexMap;
		KeyAtIndexMap keyAtIndexMap;
		typedef std::vector<ResultType> ResultMap;
		ResultMap resultMap;

		void addFk(const Value &v, std::size_t index);
		void addFk(const KeyType &v, std::size_t index);

	};

	Query lq;
	Query rq;
	QObj qobj;
};




template<typename BindFn, typename AgregFn, typename MergeFn>
inline JoinedQuery<BindFn, AgregFn, MergeFn> couchit::Query::join(
		const Query& rightSide, const BindFn& bindFn, const AgregFn& agregFn,
		const MergeFn& mergeFn) {
	return JoinedQuery<BindFn, AgregFn, MergeFn>(*this,rightSide,bindFn,agregFn,mergeFn);
}

}
