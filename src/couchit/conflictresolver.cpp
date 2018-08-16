/*
 * conflictresolver.cpp
 *
 *  Created on: 16. 3. 2018
 *      Author: ondra
 */

#include <shared/logOutput.h>
#include <future>
#include <thread>
#include <unordered_map>
#include "changes.h"
#include "couchDB.h"
#include "conflictresolver.h"
#include "shared/mtcounter.h"

using ondra_shared::logDebug;

#define logDebug(...)

namespace couchit {

using ondra_shared::MTCounter;
using ondra_shared::logDebug;

Value ConflictResolver::makeDiff(Value curRev, Value oldRev, Flags flags) const {

	bool skipReserved = (flags & ConflictResolver::skipReserved) != 0;
	bool recursive = (flags & ConflictResolver::recursive) != 0;
	if (curRev.type() == json::object && oldRev.type() == json::object) {

		std::vector<Value> diffData;
		diffData.reserve(std::max(curRev.size(), oldRev.size()));
		auto crit = curRev.begin();
		auto orit = oldRev.begin();
		auto cre = curRev.end();
		auto ore = oldRev.end();
		while (crit != cre && orit != ore) {
			Value crv = *crit;
			Value orv = *orit;
			StrViewA orvk = orv.getKey();
			StrViewA crvk = crv.getKey();
			int cmpname = crvk.compare(orvk);
			if (cmpname < 0)  {
				if (!skipReserved || crvk.substr(0,1) !="_") {
					diffData.push_back(crv);
				}
				++crit;
			} else if (cmpname > 0) {
				if (!skipReserved || orvk.substr(0,1) !="_") {
					diffData.push_back(Value(orvk, json::undefined));
				}
				++orit;
			} else {
				if (!skipReserved || crvk.substr(0,1) !="_") {
					if (recursive) {
						Value d = Value(orvk, makeDiff(crv,orv,false));
						if (d.defined()) diffData.push_back(d);
					} else if (crv != orv) {
						diffData.push_back(crv);
					}
				}
				++crit;
				++orit;
			}
		}
		while (crit != cre) {
			Value crv = *crit;
			StrViewA crvk = crv.getKey();
			if (!skipReserved || crvk.substr(0,1) !="_") {
				diffData.push_back(crv);
			}
			++crit;
		}
		while (orit != ore) {
			Value orv = *orit;
			StrViewA orvk = orv.getKey();
			if (!skipReserved || orvk.substr(0,1) !="_") {
				diffData.push_back(Value(orvk, json::undefined));
			}
			++crit;
		}
		if (diffData.empty()) return json::undefined;
		else return Value(json::object, diffData,false);
	} else {
		if (curRev != oldRev) return curRev;
		else return json::undefined;
	}


}


Value ConflictResolver::mergeDiffs(bool recursive, Value baseRev, Value diff1, Value diff2, const Path &path) const {
	if (diff1.type() == json::object && diff2.type() == json::object) {

		std::vector<Value> diffData;
		diffData.reserve(diff1.size()+diff2.size());
		auto d1it = diff1.begin();
		auto d2it = diff2.begin();
		auto d1e = diff1.end();
		auto d2e = diff2.end();
		while (d1it != d1e && d2it != d2e) {
			Value d1v = *d1it;
			Value d2v = *d2it;
			StrViewA d1vk = d1v.getKey();
			StrViewA d2vk= d2v.getKey();
			int cmpname = d1vk.compare(d2vk);
			if (cmpname < 0)  {
				diffData.push_back(d1v);
				++d1it;
			} else if (cmpname > 0) {
				diffData.push_back(d2v);
				++d2it;
			} else {
				if (recursive) {
					diffData.push_back(Value(d1vk,
							mergeDiffs(recursive, baseRev[d1vk],d1v,d2v,Path(path, d1vk))));
				} else if (d1v != d2v) {
					diffData.push_back(resolveConflict(Path(path, d1vk), baseRev, d1v, d2v));
				}

				++d1it;
				++d2it;
			}
		}
		while (d1it != d1e) {
			Value d1v = *d1it;
			diffData.push_back(d1v);
			++d1it;
		}
		while (d2it != d2e) {
			Value d2v = *d2it;
			diffData.push_back(d2v);
			++d2it;
		}
		return Value(json::object, diffData,false);

	} else {
		if (diff1 != diff2) {
			return resolveConflict(path, baseRev, diff1, diff2);
		} else {
			return diff1;
		}
	}

}

Value ConflictResolver::applyDiff(Value curRev, Value diff) const {
	if (curRev.type() == json::object && diff.type() == json::object) {

		std::vector<Value> diffData;
		diffData.reserve(curRev.size()+diff.size());
		auto crit = curRev.begin();
		auto dfit = diff.begin();
		auto cre = curRev.end();
		auto dfe = diff.end();
		while (crit != cre && dfit != dfe) {
			Value crv = *crit;
			Value dfv = *dfit;
			StrViewA crvk = crv.getKey();
			StrViewA dfvk= dfv.getKey();
			int cmpname = crvk.compare(dfvk);
			if (cmpname < 0)  {
				diffData.push_back(crv);
				++crit;
			} else if (cmpname > 0) {
				diffData.push_back(dfv);
				++dfit;
			} else {
				diffData.push_back(Value(crvk,applyDiff(crv, dfv)));
				++crit;
				++dfit;
			}
		}
		while (crit != cre) {
			Value crv = *crit;
			diffData.push_back(crv);
			++crit;
		}
		while (dfit != dfe) {
			Value dfv = *dfit;
			diffData.push_back(dfv);
			++dfit;
		}
		return Value(json::object, diffData,true);

	} else {
		return diff;
	}

}


Value ConflictResolver::merge3way(Value baseRev, Value curVer, Value conflictVer, Flags flags) const {

	logDebug("base $1", baseRev.stringify());
	logDebug("curVer $1", curVer.stringify());
	logDebug("conlifct $1", conflictVer.stringify());
	Value diff1 = makeDiff(curVer, baseRev, flags);
	logDebug("diff1 $1", diff1.stringify());
	Value diff2 = makeDiff(conflictVer, baseRev,flags);
	logDebug("diff2 $1", diff2.stringify());
	Value diffMerged = mergeDiffs((flags & recursive) != 0, baseRev, diff1, diff2);
	logDebug("diffMerged $1", diffMerged.stringify());
	Value result = applyDiff(baseRev, diffMerged);
	logDebug("result $1", result.stringify());
	return result;

}

Value ConflictResolver::merge2way(Value curVer, Value , Flags) const {
	return curVer;
}

Value ConflictResolver::resolveConflict(const Path& ,
		const Value& , const Value& rev1, const Value& ) const {
	return rev1;
}

static Value findCommonRev(Value curVer, Value conflictVer) {

	//Load revisions field;
	Value curVerRevs = curVer["_revisions"];
	Value cnflVerRevs = conflictVer["_revisions"];
	//Receive revision position of each document
	std::size_t curVerHistSize = curVerRevs["start"].getUInt();
	std::size_t cnflVerHistSize = cnflVerRevs["start"].getUInt();
	//the current version should have longer or equal history length then conflicted version
	//if this is not achieved, then switch the argumenys
	if (curVerHistSize < cnflVerHistSize)
		return findCommonRev(conflictVer, curVer);
	//skip newest revisions of current document which are newer relative to conflicted document
	std::size_t curPtr = curVerHistSize-cnflVerHistSize;
	//explore whole history of conflicted document
	std::size_t cnflPtr = 0;
	//receive list of revisions
	Value curList = curVerRevs["ids"];
	Value cnflList = cnflVerRevs["ids"];
	//receive size of these lists
	std::size_t curListLen = curList.size();
	std::size_t cnflListLen = cnflList.size();
	while (curPtr < curListLen && cnflPtr < cnflListLen) {
		//compare side by side to find common revision
		if (curList[curPtr] == cnflList[cnflPtr]) {
			//reconstruct revision ID
			//we count revision number and append revision hash
			Revision rev(curVerHistSize-curPtr, String(curList[curPtr]));
			//generate string
			return Value(rev.toString());
		}
		++curPtr;
		++cnflPtr;
	}
	return Value();

}

bool ConflictResolver::resolveAllConflicts(CouchDB& couch, String id,Document& doc, Array &conflictsArr) {
	Value curVer = couch.get(id,CouchDB::flgRevisions|CouchDB::flgConflicts|CouchDB::flgNullIfMissing);
	if (curVer.isNull()) return false;

	Value conflicts = curVer["_conflicts"];
	if (conflicts.empty()) return false;

	std::unordered_map<Value, Value> commonRevCache;

	Value conflictDocs = couch.getRevisions(id,conflicts,CouchDB::flgRevisions|CouchDB::flgAttachments);
	Array revToDownload;
	for (Value cdoc : conflictDocs) {
		 if (cdoc.getKey() == "ok") {
			 Value commonRev = findCommonRev(curVer, cdoc);
			 if (commonRev.defined()) {
				 if (commonRevCache.find(commonRev) == commonRevCache.end()) {
					 revToDownload.push_back(commonRev);
					 commonRevCache.insert(std::make_pair(commonRev, Value()));
				 }
			 }
		 }
	}
	Value commonRevs = couch.getRevisions(id, revToDownload, CouchDB::flgRevisions);
	for (Value x: commonRevs) {
		if (x.getKey() == "ok"_) {
				commonRevCache[x["_rev"]] =  x;
		}
	}
	Value newVer = curVer;
	for (Value cdoc : conflictDocs) {
		 if (cdoc.getKey() == "ok") {
			 Value commonRev = findCommonRev(curVer, cdoc);
			 if (commonRev.defined()) {
				 auto iter = commonRevCache.find(commonRev);
				 if (iter != commonRevCache.end()) {
					 Value commonDoc = iter->second;
					 newVer = merge3way(commonDoc, newVer, cdoc);
					 newVer = merge3way_attachments(commonDoc,newVer, cdoc);
				 } else {
					 newVer = merge2way(newVer, cdoc);
					 newVer = merge2way_attachments(newVer, cdoc);

				 }
			 } else{
				 newVer = merge2way(newVer, cdoc);
				 newVer = merge2way_attachments(newVer, cdoc);
			 }
		 }
	}
	doc.clear();
	doc.setBaseObject(newVer);
	doc.setRev(curVer["_rev"]);
	doc.set("_revisions", curVer["_revisions"]);
	conflictsArr.setBaseObject(conflicts);
	logDebug("merged doc $1", Value(doc).stringify());
	return true;



}

Value ConflictResolver::merge3way_attachments(Value baseRev, Value curVer, Value conflictVer) const {

	return curVer.replace("_attachments",
			merge3way(baseRev["_attachments"], curVer["_attachments"], conflictVer["_attachments"], 0));


}

Value ConflictResolver::merge2way_attachments(Value curVer, Value conflictVer) const {
	return curVer.replace("_attachments",
			merge3way(json::object, curVer["_attachments"], conflictVer["_attachments"], 0));
}


static StrViewA conflictDesignDoc(
R"json(
{
   "_id": "_design/couchit_conflicts",
   "language": "javascript",
   "filters": {
       "conflicts": "function(doc) {\n return doc._conflicts;\n}"
   }
}
)json");

void ConflictResolver::runResolver(CouchDB& db) {

	if (stopResolverFn) return;

	db.putDesignDocument(conflictDesignDoc.data, conflictDesignDoc.length);

	MTCounter initwait(1);

	std::thread thr([&] {
		CouchDB &xdb = db;
		ChangesFeed chfeed = xdb.createChangesFeed();
		chfeed.setFilter(Filter("couchit_conflicts/conflicts"));
		Document state (xdb.getLocal("conflict_resolver",CouchDB::flgCreateNew));
		Value since = state["since"];
		if (since.defined()) chfeed.since(since);


		finishWait.setCounter(1);

		while (true) {
			try {

				chfeed.setTimeout(0);
				chfeed.setIOTimeout(xdb.getConfig().syncQueryTimeout);
				stopResolverFn = [&]{
					stopResolverFn = nullptr;
					chfeed.cancelWait();
					finishWait.wait();
				};


				auto processFn = [&](const ChangedDoc &d){
					if (!d.deleted) {
						Document doc;
						Array conflicts;
						if (resolveAllConflicts(xdb,d.id,doc, conflicts)) {
							db.pruneConflicts(doc, conflicts);
						}
					}
					state.set("since",d.seqId);
					xdb.put(state);
					return true;
				};


				for (ChangedDoc d: chfeed.exec()) {
					processFn(d);
				}

				state.set("since",chfeed.getLastSeq());
				xdb.put(state);

				initwait.dec();

				chfeed.setTimeout((std::size_t)-1);
				chfeed >> processFn;
				finishWait.dec();
				return;
			} catch (...) {
				onResolverError();
				std::this_thread::sleep_for(std::chrono::seconds(10));
			}
		}
	});
	thr.detach();
	initwait.wait();

}

void ConflictResolver::stopResolver() {
	if (stopResolverFn) stopResolverFn();
}

ConflictResolver::~ConflictResolver() {
	stopResolver();
}

} /* namespace couchit */
