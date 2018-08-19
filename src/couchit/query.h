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

	///Updates view before the query is executed
	/** Similar to View::update */
	Query &update();

	///Disable updating the view
	/** Similar to View::stale */
	Query &stale();

	///Includes documents into the query.
	/** Similar to View::includeDocs */
	Query &includeDocs();

	///Includes conflict informations to the result (enables includeDocs)
	Query &conflicts();

	///Set POST data for the list as additional arguments
	Query &setPostData(Value postData);

	///Requests for valid update_seq
	/** The query is always asks for update_seq, however, there are bugs in serveral
	 * versions of the couchdb. Sometimes an invalid update_seq can be stored
	 * with the view and then your code can fail if it relly thightly to that number.
	 *
	 * By setting this flag, the query ensures that update_seq is valid. If not,
	 * it performs some other actions to receive last known update_seq. This can
	 * have a negative performance impact on the query, so the option is disabled by
	 * default.
	 *
	 */
	Query &needUpdateSeq();
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
	Result(const Value &resultArray, std::size_t total, std::size_t offset, const Value &updateSeq = json::undefined);

	std::size_t getTotal() const {return total;}
	std::size_t getOffset() const {return offset;}

	bool hasItems() const {return pos < cnt;}
	Value getNext() {return operator[](pos++);}
	Value peek() const {return operator[](pos);}
	void rewind() {pos = 0;}

	///Returns update_seq if available
	Value getUpdateSeq() const {return updateSeq;}

	///Updates all rows in the view
	/**
	 * The function processes the whole result and calls updateFn for each document in it. Then, documents are updated.
	 *
	 * @param db database where the documents are put
	 * @param updateFn function, which accepts Value. It have to return updated value of the document.
	 *
	 * @return count of affected documents
	 *
	 */
	template<typename UpdateFn>
	unsigned int update(CouchDB &db, UpdateFn &&updateFn);

	///Updates all rows in the view
	/**
	 * The function processes the whole result and calls updateFn for each document in it. Then, documents are updated.
	 *
	 * @param db database where the documents are put
	 * @param updateFn the function, which accepts Value. It have to return updated value of the document.
	 * @param commitFn the function, which is called after the document is stored to the database. The argument
	 * can contain either updated document or an error, which happened during commit. To distinguish between
	 * success and error, you can use getCommitError(). If the result is null, the document has been stored
	 * @return count if affected documents
	 *
	 */
	template<typename UpdateFn, typename CommitFn>
	unsigned int update(CouchDB &db, UpdateFn &&updateFn, CommitFn &&commitFn);


	///Retrieves error status in commit function
	/**
	 * @param commitRes commit result.
	 * @retval null no error happened, commitRes contains updated document
	 * @retval object error happened, returns error object (commitRes is useless)
	 */
	static Value getCommitError(Value commitRes);

protected:

	std::size_t total;
	std::size_t offset;
	std::size_t pos;
	std::size_t cnt;
	Value updateSeq;

};



class Row: public Value {
public:
	///contains key
	Value key;
	///contains value
	Value value;
	///contains document - will be nil, if documents are not requested in the query
	Value doc;
	///contains source document ID
	Value id;
	///contains error information for this row
	Value error;

	Row(const PValue &jrow);
	Row(const Value &jrow);
	Row() {}

	///Returns 'true' if row exists (it is not error)
	bool exists() const {return error != null;}


};

namespace _details {
	template<typename T, std::size_t n> class NStore {
	public:
		T data[n];
		static const std::size_t count = n;
		typedef T Type;

		NStore(const std::initializer_list<T> &list) {
			std::size_t pos = 0;
			for(auto &&x : list) {
				if (pos >=n) break;
				data[pos++] = x;
			}
		}
		NStore() {}

		const T *begin() const {return data;}
		const T *end() const {return data+n;}


		const T &operator [](std::size_t idx) const {return data[idx];}
		T &operator [](std::size_t idx) {return data[idx];}
	};


	template<typename T> class NStore<T,1>: public T {
	public:
		static const std::size_t count = 1;
		typedef T Type;

		NStore(const std::initializer_list<T> &list):T(list[0]) {}
		NStore() {}

		const T *begin() const {return this;}
		const T *end() const {return this+1;}


		const T &operator [](std::size_t) const {return *this;}
		T &operator [](std::size_t) {return *this;}
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

	Value bulkUpload(CouchDB &db, Value data);
	Query allDocs(CouchDB &db);
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
 *   to skip the row in the request to the other view. Function can also return MultiFKey
 *   type, which defines multiple keys. However in this case, the type MultiFKey
 *   is used to carry result for MergeFn (instead Value)
 *
 * @tparam AgrFn Aggregate function Value(Array &). Function receives array of results
 *    matching a single foreign key. Function should agregate the result and return a signle
 *    value which is considered as result for the given foreign key. Each item
 *    of the Array (as argument of the function) is row from the other query. You can cast each
 *    item to Row to extract attributes of the row.
 *
 * @tparam MergeFn Merge function. The function receives two arguments, first argument
 *   is row from the first query, second argument is matching row from the other query (as it was
 *   generated by AgrFn). The Function need to return final row, which is
 *   included to the result. If there is no result for the other query, second argument is undefined.
 *   The function may return undefined to remove whole row from the result.
 *
 *   Note that if the BindFn returned MultiFKey, the MergeFn also receives MultiFKey as second argument.
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


template<typename UpdateFn>
inline unsigned int Result::update(CouchDB& db, UpdateFn&& fn) {
	return update(db, std::forward<UpdateFn>(fn), [](const Value &){});
}


template<typename UpdateFn, typename CommitFn>
inline unsigned int couchit::Result::update(CouchDB& db, UpdateFn&& updateFn, CommitFn&& commitFn) {

	Array chs;

	for (Value rw: *this) {
		Value doc = rw["doc"];
		if (doc.type() == json::object) {
			Value newDoc = updateFn(doc);
			if (!newDoc.isCopyOf(doc)) {
				chs.push_back(newDoc);
			}
		}

	}

	unsigned int cnt = 0;

	if (!chs.empty()) {
		Value upload(chs);
		Value res = _details::bulkUpload(db, upload);
		chs.clear();
		for (std::size_t i = 0, sz = res.size(); i < sz; ++i) {

			Value r = res[i];
			if (r["ok"].getBool()) {
				Value nd = upload[i].replace("_rev", r["rev"]);
				commitFn(nd);
				cnt++;
			} else if (r["error"].getString() == "conflict") {
				chs.push_back(r["id"]);
			} else {
				commitFn(r);
			}

		}

		if (!chs.empty()) {
			Query q = _details::allDocs(db);
			q.keys(chs);
			Result res = q.exec();
			cnt += res.update(db,updateFn, commitFn);
		}
	}

	return cnt;
}

}
