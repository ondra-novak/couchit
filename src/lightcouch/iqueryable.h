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
	const Array &keys;
	///Type of search
	QueryMode mode;
	///offset in result
	natural offset;
	///count of results
	natural limit;
	///specifies starting document-id if lower-bound key is mapped to multiple docs
	StringRef startDoc;
	///specifies starting document-id if upper-bound key is mapped to multiple docs
	StringRef endDoc;
	///arguments passed to the postprocessing function (or server's list function)
	const Object &ppargs;
	///Specifies mode for reduce
	ReduceMode reduceMode;
	///for reduce mode rmGroupLevel specifies the level (otherwise ignored)
	natural groupLevel;
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
};


class IQueryableObject {
public:
	///Executes query on queryable object
	/** @param r request to query
	 * @return {"docs":[ ... rows...],...}
	 *
	 * The function may return additional informations about returned rows. The result
	 * can be passed to the object Result
	 */
	virtual Value executeQuery(const QueryRequest &r) = 0;
	virtual ~IQueryableObject() {}

};

}




#endif /* LIGHTCOUCH_IQUERYABLE_H_ */
