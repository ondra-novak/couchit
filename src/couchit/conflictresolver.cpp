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

Value ConflictResolver::makeDiff(Value curRev, Value oldRev, bool recursive) const {

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
				diffData.push_back(crv);
				++crit;
			} else if (cmpname > 0) {
				diffData.push_back(Value(orvk, json::undefined));
				++orit;
			} else {
				if (recursive) {
					Value d = Value(orvk, makeDiff(crv,orv,false));
					if (d.defined()) diffData.push_back(d);
				} else if (crv != orv) {
					diffData.push_back(crv);
				}
				++crit;
				++orit;
			}
		}
		while (crit != cre) {
			Value crv = *crit;
			diffData.push_back(crv);
			++crit;
		}
		while (orit != ore) {
			Value orv = *orit;
			StrViewA orvk = orv.getKey();
			diffData.push_back(Value(orvk, json::undefined));
			++crit;
		}
		if (diffData.empty()) return json::undefined;
		else return Value(json::object, diffData,false);
	} else {
		if (curRev != oldRev) return curRev;
		else return json::undefined;
	}


}


Value ConflictResolver::mergeDiffs(Value baseRev, Value diff1, Value diff2, bool recursive, const Path &path) const {
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
							mergeDiffs(baseRev[d1vk],d1v,d2v,recursive,Path(path, d1vk))));
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

Value ConflictResolver::applyDiff(Value curRev, Value diff, bool recursive) const {
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
				diffData.push_back(Value(crvk,recursive?applyDiff(crv, dfv,recursive):dfv));
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

static Value removeSystemProps(Value v) {
	Object o(v);
	o.unset("_rev");
	o.unset("_conflicts");
	o.unset("_revisions");
	return o;
}


Value ConflictResolver::merge3way(Value baseRev, Value curVer, Value conflictVer, bool recursive) const {

	logDebug("base $1", baseRev.stringify());
	logDebug("curVer $1", curVer.stringify());
	logDebug("conlifct $1", conflictVer.stringify());
	Value diff1 = removeSystemProps(makeDiff(curVer, baseRev, recursive));
	logDebug("diff1 $1", diff1.stringify());
	Value diff2 = removeSystemProps(makeDiff(conflictVer, baseRev,recursive));
	logDebug("diff2 $1", diff2.stringify());
	Value diffMerged = mergeDiffs( baseRev, diff1, diff2, recursive);
	logDebug("diffMerged $1", diffMerged.stringify());
	Value result = applyDiff(baseRev, diffMerged, recursive);
	logDebug("result $1", result.stringify());
	return result;

}

Value ConflictResolver::merge2way(Value curVer, Value , bool) const {
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

struct AttachmentInfo {
	String name;
	String revision;
};

template<typename T>
void find_attachments_for_download(T &cont, Value curVer, Value newVer, Value rev) {
	Value curAtt = curVer["_attachments"];
	Value newAtt = newVer["_attachments"];
	for (Value v : newAtt) {
		StrViewA name = v.getKey();
		Value c = curAtt[name];
		if (!c.defined() || c["digest"] != v["digest"]) {
			AttachmentInfo nfo;
			logDebug("Registered attachment for download: rev=$1 $2",rev.getString(), name);
			nfo.name = name;
			nfo.revision = String(rev);
			cont.push_back(nfo);
		}
	}
}

bool ConflictResolver::resolveAllConflicts(CouchDB& couch, String id,Document& doc) {
	Value curVer = couch.get(id,CouchDB::flgRevisions|CouchDB::flgConflicts|CouchDB::flgNullIfMissing);
	if (curVer.isNull()) return false;

	Value conflicts = curVer["_conflicts"];
	if (conflicts.empty()) return false;

	std::unordered_map<Value, Value> commonRevCache;

	Value conflictDocs = couch.getRevisions(id,conflicts,CouchDB::flgRevisions);
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

	std::vector<AttachmentInfo> att_need_download;

	Value newVer = curVer;
	for (Value cdoc : conflictDocs) {
		 if (cdoc.getKey() == "ok") {
			 Value commonRev = findCommonRev(curVer, cdoc);
			 if (commonRev.defined()) {
				 auto iter = commonRevCache.find(commonRev);
				 if (iter != commonRevCache.end()) {
					 Value commonDoc = iter->second;
					 newVer = merge3way(commonDoc, newVer, cdoc, true);
				 } else {
					 newVer = merge2way(newVer, cdoc, true);

				 }
			 } else{
				 newVer = merge2way(newVer, cdoc, true);
			 }
			 find_attachments_for_download(att_need_download, curVer, newVer, cdoc["_rev"]);
		 }
	}
	doc.clear();
	doc.setBaseObject(newVer);
	doc.setRev(curVer["_rev"]);
	doc.set("_revisions", curVer["_revisions"]);

	for (auto &&item : att_need_download) {
		logDebug("Inlining attachment: $1",item.name);
		Download dwn = couch.getAttachment(doc.getID(),item.name,"",item.revision);
		auto data = dwn.load();
		doc.inlineAttachment(item.name,AttachmentDataRef(BinaryView(data.data(), data.size()),dwn.contentType));
	}
	doc.set("_conflicts", conflicts);
	logDebug("merged doc $1", Value(doc).stringify());
	return true;



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

		stopResolverFn = [&]{
			stopResolverFn = nullptr;
			chfeed.cancelWait();
			finishWait.wait();
		};


		while (stopResolverFn!=nullptr) {
			try {

				chfeed.setTimeout(0);
				chfeed.setIOTimeout(xdb.getConfig().syncQueryTimeout);


				auto processFn = [&](const ChangeEvent &d){
					if (!d.deleted) {
						Document doc;
						if (resolveAllConflicts(xdb,d.id,doc)) {
							db.pruneConflicts(doc);
						}
					}
					state.set("since",d.seqId);
					xdb.put(state);
					return true;
				};


				for (ChangeEvent d: chfeed.exec()) {
					processFn(d);
				}

				state.set("since",chfeed.getLastSeq());
				xdb.put(state);

				initwait.dec();

				chfeed.setTimeout((std::size_t)-1);
				chfeed >> processFn;
				break;
			} catch (...) {
				onResolverError();
				std::this_thread::sleep_for(std::chrono::seconds(10));
			}
		}
		finishWait.dec();
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
