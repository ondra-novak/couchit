/*
 * view.h
 *
 *  Created on: 2. 4. 2016
 *      Author: ondra
 */

#ifndef LIBS_LIGHTCOUCH_SRC_LIGHTCOUCH_VIEW_H_
#define LIBS_LIGHTCOUCH_SRC_LIGHTCOUCH_VIEW_H_
#include <functional>
#include <lightspeed/base/types.h>
#include <lightspeed/base/containers/string.h>
#include <lightspeed/utils/json.h>
#include "object.h"

namespace LightCouch {

using namespace LightSpeed;
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
	static const natural includeDocs=0x01;
	///Perform reduce (default value is not to perform reduce)
	static const natural reduce=0x02;
	///Controls whether the endkey is excluded in the result. Default is false
	static const natural exludeEnd=0x04;
	///Enforce GET method for various situations, where POST could be used
	/** this helps to caching and E-Tags, but may create large URI */
	static const natural forceGETMethod=0x08;
	///never update the view
	static const natural stale=0x10;
	///update view after values are returned
	static const natural updateAfter=0x20;
	///include update seq
	static const natural updateSeq = 0x40;
	///include attachments to the result (before list is processed)
	static const natural attachments = 0x80;
	///include attachment encoding info
	static const natural attEncodingInfo = 0x100;
	///include conflicts, requires includeDocs
	static const natural conflicts = 0x200;
	///reverse ordering
	/**reverses order, so result will be returned in reversed order. Note that
	 * if order is reserver in the View and also in the Query, original order is
	 * returned
	 */
	static const natural reverseOrder = 0x400;

	///specify group level
	/** Use this flag and multiply it by selected group level. You have to turn on reduce
	 *
	 * For multi-key queries, group with nonzero value is interpreted as group=true.
	 * In this case, query will reduce rows with the whole same key (exact). This is
	 * the behaviour of couchDb
	 *
	 *  */
	static const natural     groupLevel=0x01000000;
	///Masks groupLevel, maximum 255 levels
	static const natural groupLevelMask=0xFF000000;

	///Defines list arguments
	/** these arguments will be always applied for the lits */
	struct ListArg {
		///key
		StringA key;
		///value
		StringA value;
	};

	typedef ConstStringT<ListArg> ListArgs;
	///Function called to postprocess to view
	/**
	 * @param CouchDb* pointer to an active instance of CouchDB (currently used for query)
	 * @param ConstValue arguments of the query
	 * @param ConstValue result from map-reduce-list executed by the query
	 * @return Modified result
	 *
	 * Function have to update result and return it as return value. It have allowed to
	 * perform other
	 */
	typedef std::function<ConstValue(CouchDB *, ConstValue, ConstValue)> Postprocessing;

	///Declare the view
	/** Declare single view or list by the path only */
    View(StringA viewPath);
    ///Declare more specific view
    /**
     * @param viewPath path to the view relative to the database's root. It can be also path to the list
     * @param flags various flags
     * @param args additional arguments preconfigured for this view
     */
	View(StringA viewPath, natural flags, ListArgs args = ListArgs());

	///Declare more specific view
	/**
	 * @param viewPath path to the view relative to the database's root. It can be also path to the list
	 * @param flags various view flags
	 * @param ppfunction function called to post-process result. Note, results from post-processing are
	 * not cached. If you need to cache results, use Lists on the server side instead
	 * @param args additional arguments preconfigured for this view
	 */
	View(StringA viewPath, natural flags, const Postprocessing &ppfunction, ListArgs args = ListArgs() );

	View addArg(ConstStringT<ListArg> args) const;

	View setArgs(ConstStringT<ListArg> args) const;

	View setFlags(natural flags) const;

	View setPath(StringA path) const;


	const StringA viewPath;
	const natural flags;
	const StringCore<ListArg> args;
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
	Filter(StringA filter, natural flags, ConstStringT<ListArg> args = ConstStringT<ListArg>() );
	///Declare filter using the view
	/**
	 * @param view view that will be used to filter results
	 * @param allConflicts include all conflicted documents
	 *
	 * @note postprocess function is not used for filtering
	 */
	Filter(const View &view, bool allConflicts = false);


	///Declare filter without flags (convert string name of the filter to the filter definition)
	Filter(StringA filter);
	///return all current revisions, including deleted
	static const natural allRevs = 0x10000;

	Filter addArg(ConstStringT<ListArg> args) const;

	Filter setArgs(ConstStringT<ListArg> args) const;

	Filter setFlags(natural flags) const;

	Filter setPath(StringA path) const;

};

} /* namespace LightCouch */

#endif /* LIBS_LIGHTCOUCH_SRC_LIGHTCOUCH_VIEW_H_ */
