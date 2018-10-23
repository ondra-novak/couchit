
#ifndef SRC_COUCHIT_SRC_COUCHIT_CONFLICTRESOLVER_H_
#define SRC_COUCHIT_SRC_COUCHIT_CONFLICTRESOLVER_H_

#include "document.h"
#include <imtjson/value.h>
#include <imtjson/path.h>
#include <functional>
#include "shared/countdown.h"

#pragma once

namespace couchit {

class CouchDB;

using namespace json;

class ConflictResolver {
public:



	///Merge three-way
	/**
	 * Function is able to merge three objects into one. It can be also subobjects. Function can
	 * be called recursivelly
	 *
	 * @param baseRev base revision, most common revision for both branches/
	 * @param curVer current revision
	 * @param conflictVer conflicted revision
	 * @param skipReserved set true (default) to skip couchdb's reserved keys (starting with underscore)
	 * @return merged object
	 */
	virtual Value merge3way(Value baseRev, Value curVer, Value conflictVer, bool recursive) const;

	///Merge two-way
	/** Function is called when merge three-way is not possible. You can override default behaviour
	 *
	 * Three-way merge can be impossible after database compaction, when the historical version
	 * is not available
	 *
	 * @param curVer current revision
	 * @param conflictVer conflicted revision
	 * @param skipReserved
	 * @param skipReserved set true (default) to skip couchdb's reserved keys (starting with underscore)
	 * @return merged object
	 *
	 * @note current implementation simply discard all conflicts leaving curVer untouched
	 *
	 */
	virtual Value merge2way(Value curVer, Value conflictVer, bool recursive) const;

	virtual Value makeDiff(Value curRev, Value oldRev, bool recursive) const;
	virtual Value mergeDiffs(Value baseRev, Value diff1, Value diff2, bool recursive, const Path &path = Path::root) const;
	virtual Value applyDiff(Value curRev, Value diff, bool recursive) const;

	virtual Value resolveConflict(const Path &path,
			const Value &baseRev, const Value &rev1, const Value &rev2) const;

	///creates document containing resolved conflicts of conflicted document;
	/**
	 * @param couch
	 * @param id
	 * @param doc document object which receives merged document
	 *
	 * @retval true conflicts resolved
	 * @retval false no conflicts found
	 */
	virtual bool resolveAllConflicts(CouchDB &couch, String id, Document &doc);



	///Starts conflict resolver
	/**
	 * @param db database object.
	 *
	 * If the resolver is already running, function does nothing.
	 *
	 * To stop resolver, call stopResolver()
	 *
	 * The destructor also automatically stops the resolver.
	 *
	 * Note that controlling the resolver thread is not MT safe
	 *
	 */
	void runResolver(CouchDB &db);
	void stopResolver();
	virtual void onResolverError() {throw;}
	virtual ~ConflictResolver();
protected:
	typedef std::function<void()> Action;
	Action stopResolverFn;
	ondra_shared::Countdown finishWait;

};

} /* namespace couchit */

#endif /* SRC_COUCHIT_SRC_COUCHIT_CONFLICTRESOLVER_H_ */
