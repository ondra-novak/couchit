/*
 * view.h
 *
 *  Created on: 2. 4. 2016
 *      Author: ondra
 */

#ifndef LIBS_LIGHTCOUCH_SRC_LIGHTCOUCH_VIEW_H_
#define LIBS_LIGHTCOUCH_SRC_LIGHTCOUCH_VIEW_H_
#include <functional>
#include "json.h"

namespace couchit {

class CouchDB;

///Uses to configure the query
/**
 * Declaration of the view doesn't need anything from CouchDB. This is just only
 * declaration of an existing view. You can configure path to the view and
 * features of the view. This class can be used also for lists, in situation
 * when the list returns JSON, especially when JSON is formatted as view. In
 * this case, the list can be considered as postprocessing on the results.
 */
class View {
public:

	///always include documents to the view's results
	/** If view refers to the list, then documents are processed by the list, not returned */
	static const std::size_t includeDocs=0x01;
	///Perform reduce (default value is not to perform reduce)
	static const std::size_t reduce=0x02;
	///Controls whether the endkey is excluded in the result. Default is false
	static const std::size_t exludeEnd=0x04;
	///Disable caching for this view
	static const std::size_t noCache=0x04;
	///do not update the view
	static const std::size_t stale=0x10;
	///synchronous update. Updates the view before the results are returned
	/**Querying with this flag can block the thread for long time until the
	 * view is updated.
	 *
	 * This flag enforces CouchDB::updateView during the query
	 */
	static const std::size_t update=0x20;
	///include attachments to the result (before list is processed)
	static const std::size_t attachments = 0x80;
	///include attachment encoding info
	static const std::size_t attEncodingInfo = 0x100;
	///include conflicts, requires includeDocs
	static const std::size_t conflicts = 0x200;
	///reverse ordering
	/**reverses order, so result will be returned in reversed order. Note that
	 * if order is reserver in the View and also in the Query, original order is
	 * returned
	 */
	static const std::size_t reverseOrder = 0x400;


	///specify group level
	/** Use this flag and multiply it by selected group level. You have to turn on reduce
	 *
	 * For multi-key queries, group with nonzero value is interpreted as group=true.
	 * In this case, query will reduce rows with the whole same key (exact). This is
	 * the behaviour of couchDb
	 *
	 *  */
	static const std::size_t     groupLevel=0x01000000;
	///Masks groupLevel, maximum 255 levels
	static const std::size_t groupLevelMask=0xFF000000;

	///Function called to postprocess to view
	/**
	 * @param CouchDb* pointer to an active instance of CouchDB (currently used for query)
	 * @param ConstValue arguments of the query
	 * @param ConstValue result from map-reduce-list executed by the query
	 * @return Modified result
	 *
	 * Function have to update result and return it as return value.
	 */
	typedef std::function<Value(CouchDB *, Value, Value)> Postprocessing;

	///Declare the view
	/** Declare single view or list by the path only */
    View(String viewPath);
    ///Declare more specific view
    /**
     * @param viewPath path to the view relative to the database's root. It can be also path to the list
     * @param flags various flags
     * @param args additional arguments preconfigured for this view
     */
	View(String viewPath, std::size_t flags, Value args = Value());

	///Declare more specific view
	/**
	 * @param viewPath path to the view relative to the database's root. It can be also path to the list
	 * @param flags various view flags
	 * @param ppfunction function called to post-process result. Note, results from post-processing are
	 * not cached. If you need to cache results, use Lists on the server side instead
	 * @param args additional arguments passed to list-function on the couchDb. It must be Object
	 */
	View(String viewPath, std::size_t flags, const Postprocessing &ppfunction, Value args = Value() );

	String viewPath;
	std::size_t flags;
	Value args;
	Postprocessing postprocess;
};

///Define filtering for changes feed
/** Filter is declared as extension of the view. However, not all flags can be used with
 * the filter. Unsupported flags are ignored. Filters also doesn't support reduce
 * function
 */
class Filter: public View {
public:
	///Declaration of the filter
	/**
	 * @param filter specify filter name
	 * @param flags some flags to define additional behaviour
	 * @param args optional arguments passed to the filter
	 */
	Filter(String filter, std::size_t flags, Value args = Value()  );
	///Declare filter using the view
	/**
	 * @param view view that will be used to filter results
	 * @param allConflicts include all conflicted documents
	 *
	 * @note postprocess function is not used for filtering
	 */
	Filter(const View &view, bool allConflicts = false);


	///Declare filter without flags (convert string name of the filter to the filter definition)
	Filter(String filter);
	///return all current revisions, including deleted
	static const std::size_t allRevs = 0x10000;


};

} /* namespace couchit */

#endif /* LIBS_LIGHTCOUCH_SRC_LIGHTCOUCH_VIEW_H_ */
