/*
 * iqueryable.h
 *
 *  Created on: 20. 10. 2016
 *      Author: ondra
 */

#ifndef LIGHTCOUCH_IQUERYABLE_H_
#define LIGHTCOUCH_IQUERYABLE_H_
#include "view.h"
#include "json.h"

namespace LightCouch {

enum QueryMode {

	///Query for all items
	/**No keys are used, all results are returned */
	qmAllItems,
	/// Query for list of keys
	/** List of keys is supplied. Ask for each key */
	qmKeyList,
	/// Ranged search
	/**Two keys are defined, first key is lower bound and second key is upper bound
	*
	 * @note always keep first key < second key regadless on ordering (in
	 * contrast to CouchDB API)
	 */
	qmKeyRange,
	/// Prefixed search
	/**One key is defined, the key is used as prefix to generate ranged search
	 *
	 * The key must be an array. If non-array is set, it will be converted
	 * to one-item array
	 */
	qmKeyPrefix,
	///String prefix search
	/**One key is defined, it could be string or array where last item is string,
	 * otherwise, the query is konverted to qmKeyPrefix
	 *
	 * The string is used as prefix where all key starting with this prefix
	 * are returned
	 */
	qmStringPrefix
};


enum ReduceMode {
	///Use default reduce mode defined by the view
	rmDefault,
	///disable reduce
	rmNoReduce,
	///force reduce (View must define reduce function)
	rmReduce,
	///group results by key (group=exact)
	rmGroup,
	///specify group-level
	rmGroupLevel
};

struct QueryRequest {

	///Define view which will be used (for non-db queries, only flags can be used)
	View view;
	///List of keys
	Array keys;
	///Type of search
	QueryMode mode;
	///offset in result
	std::size_t offset;
	///count of results
	std::size_t limit;
	///arguments passed to the postprocessing function (or server's list function)
	Object ppargs;
	///Specifies mode for reduce
	ReduceMode reduceMode;
	///for reduce mode rmGroupLevel specifies the level (otherwise ignored)
	std::size_t groupLevel;
	///set true to reversed order
	bool reversedOrder;
	///set true to skip sorting the result
	/** This option was introduced for CouchDB 2.0. Setting to true
	 * causes, that results don't need to be sorted. This
	 * can boost performance especially when you plan to change ordering
	 */
	bool nosort;
	///set true to exclude end key
	bool exclude_end;

	///Retrive startDoc and endDoc using getKey() of ranged keys
	bool docIdFromGetKey;
	///disable caching
	bool nocache;

	QueryRequest(const View &view)
		:view(view)
		,mode(qmAllItems)
		,offset(0)
		,limit(((std::size_t)-1))
		,reduceMode(rmDefault)
		,groupLevel(0)
		,reversedOrder(false)
		,nosort(false)
		,exclude_end(false)
		,nocache(false) {}
	void reset() {
		mode = qmAllItems;
		offset = 0;
		limit = ((std::size_t)-1);
		reduceMode = rmDefault;
		groupLevel = 0;
		reversedOrder = false;
		nosort = false;
		exclude_end = false;
		nocache = false;
		keys.clear();
		ppargs.clear();
	}
};


class IQueryableObject {
public:
	///Executes query on queryable object
	/** @param r request to query
	 * @return {"rows":[ ... rows...],...}
	 *
	 * The function may return additional informations about returned rows. The result
	 * can be passed to the object Result
	 */
	virtual Value executeQuery(const QueryRequest &r) = 0;
	virtual ~IQueryableObject() {}

};

}




#endif /* LIGHTCOUCH_IQUERYABLE_H_ */
