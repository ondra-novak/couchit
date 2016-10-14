/*
 * confilctResolver.cpp
 *
 *  Created on: May 22, 2016
 *      Author: ondra
 */

#include "conflictResolver.h"
#include <lightspeed/base/containers/autoArray.tcc>
#include <lightspeed/base/text/textOut.tcc>
#include "couchDB.h"
#include "document.h"
#include <lightspeed/base/containers/map.tcc>
#include <lightspeed/utils/urlencode.h>

#include "revision.h"

#include "validator.h"

using LightSpeed::JSON::serialize;
namespace LightCouch {



ConflictResolver::ConflictResolver(CouchDB& db, bool attachments):db(db),attachments(attachments) {
}

static StringA getBaseRev(ConstStrA revStr, natural start, ConstValue ids) {
	Revision crev(revStr);

	natural revid = crev.getRevId()-1;
	natural pos = (start - revid);
	if (pos < ids.length()) {
		return Revision(revid, ids[pos].getStringA()).toString();
	} else {
		return StringA();
	}
}

Document ConflictResolver::resolve(const ConstStrA& docId) {

	//buffer to format url-requests
	AutoArrayStream<char, SmallAlloc<256> > buffer;
	//url formatter
	TextOut<AutoArrayStream<char, SmallAlloc<256> >  &, SmallAlloc<256> > fmt(buffer);

	Document headRev = db.retrieveDocument(docId, CouchDB::flgRevisions|CouchDB::flgConflicts|(attachments?CouchDB::flgAttachments:0));

	//Get list of revisions
	ConstValue revisions = headRev["_revisions"];
	//parse start revision
	natural start = revisions["start"].getUInt();
	//retrieve array of ids
	ConstValue ids = revisions["ids"];
	//retrieve conflicts
	ConstValue conflicts = headRev["_conflicts"];


	//if no conflicts found,
	if (conflicts == null || conflicts.empty()) return headRev;
	Map<ConstStrA, ConstValue> openedRevs;

	Container open_revs = db.json.array(conflicts);


	conflicts->enumEntries(JSON::IEntryEnum::lambda([&](const ConstValue &v, ConstStrA , natural ){
		ConstStrA revStr= v.getStringA();
		StringA baseRevStr = getBaseRev(revStr,start,ids);
		if (!baseRevStr.empty()) {
			if (openedRevs.find(baseRevStr) == 0) {
				openedRevs(baseRevStr) = null;
				open_revs.add(db.json(baseRevStr));
			}
		}
		return false;
	}));

	buffer.clear();
	{
		FilterRead<ConstStrA::Iterator, UrlEncoder> docIdEnc(docId.getFwIter());
		fmt("%1?open_revs=") << &docIdEnc;
		FilterWrite<AutoArrayStream<char, SmallAlloc<256> > &, UrlEncoder> wrt(buffer);
		JSON::serialize(open_revs,wrt,true);
		if (attachments) {
			fmt("&attachments=true");
		}
	}
	ConstValue revlist = db.requestGET(buffer.getArray(),null,CouchDB::flgDisableCache);
	revlist->enumEntries(JSON::IEntryEnum::lambda([&](const ConstValue &v, ConstStrA , natural ){
		ConstValue obj = v["ok"];
		if (obj != null) {
			ConstStrA rev = obj["_rev"].getStringA();
			openedRevs(rev) = obj;
		}
		return false;
	}));

	conflicts->enumEntries(JSON::IEntryEnum::lambda([&](const ConstValue &v, ConstStrA , natural ){
		ConstStrA revStr= v.getStringA();
		const ConstValue *r = openedRevs.find(revStr);
		if (r) {
			StringA baseRevStr = getBaseRev(revStr,start,ids);
			if (!baseRevStr.empty()) {
				const ConstValue *b = openedRevs.find(baseRevStr);
				if (b) {
					Value merged = merge3w(headRev, *r,*b);
					if (merged != null) headRev.setContent(merged);

				} else {
					Value merged = merge2w(headRev,*r);
					if (merged != null) headRev.setContent(merged);
				}
			} else {
				Value merged = merge2w(headRev,*r);
				if (merged != null) headRev.setContent(merged);
			}
		}
		return false;
	}));
	ConstValue v = headRev["_attachments"];
	if (v != null && v.empty()) headRev.unset("_attachments");
	return headRev;

}

Value ConflictResolver::merge3w(const ConstValue& topdoc,const ConstValue& conflict, const ConstValue& base) {
	Document doc(base);
	ConstValue mainDiff = makeDiffObject(doc,Path::root,base,topdoc);
	ConstValue conflictDiff = makeDiffObject(doc,Path::root,base,conflict);
	ConstValue mergedDiffs = mergeDiffs(doc,Path::root,mainDiff,conflictDiff);
	Value mergedDoc = patchObject(doc,Path::root,base,mergedDiffs);
	Pointer<Validator> v = db.getValidator();
	if (v != null) {
		Validator::Result res = v->validateDoc(mergedDoc);
		if (!res) {
			return merge2w(topdoc,conflict);
		}
	}
	return mergedDoc;
}

Value ConflictResolver::merge2w(const ConstValue& ,const ConstValue& ) {
	return nil;
}

Value ConflictResolver::mergeValue(Document& doc,const Path& path, const ConstValue& oldValue, const ConstValue& newValue) {
	if (oldValue->getType() == newValue->getType() && oldValue->isObject())
		return patchObject(doc,path,oldValue,newValue);
	else
		return newValue->copy(db.json.factory);

}

ConstValue ConflictResolver::diffValue(Document& doc, const Path& path, const ConstValue& oldValue, const ConstValue& newValue) {
	//different types, use new value
	if (oldValue->getType() != newValue->getType()) return newValue;
	//make diff
	if (oldValue->isObject()) return makeDiffObject(doc, path, oldValue,newValue);
	if (oldValue->operator == (*newValue)) return nil;
	return newValue;
}

Value ConflictResolver::mergeObject(Document& doc, const Path& path,const ConstValue& oldValue,
		const ConstValue& newValue,const ConstValue& baseValue) {

	ConstValue diff = makeDiffObject(doc,path, newValue, baseValue);
	return patchObject(doc,path,oldValue, diff);
}

template<typename Fn>
void mergeTwoObjects(const ConstValue &left, const ConstValue &right,const Fn &resFn) {
	JSON::ConstIterator l = left->getFwConstIter();
	JSON::ConstIterator r = right->getFwConstIter();

	while (l.hasItems() && r.hasItems()) {
		const JSON::ConstKeyValue &lkv = l.peek();
		const JSON::ConstKeyValue &rkv = r.peek();
		ConstStrA lk = lkv.getStringKey();
		ConstStrA rk = rkv.getStringKey();

		if (lk < rk) {
			resFn(lk, lkv, null);
			l.skip();
		} else if (lk > rk) {
			resFn(rk, null, rkv);
			r.skip();
		} else {
			resFn(lk,lkv,rkv);
			l.skip();
			r.skip();
		}
	}
	while (l.hasItems()) {
		const JSON::ConstKeyValue &lkv = l.getNext();
		resFn(lkv.getStringKey(),lkv, null);
	}
	while (r.hasItems()) {
		const JSON::ConstKeyValue &rkv = r.getNext();
		resFn(rkv.getStringKey(),null, rkv);
	}
}


JSON::ConstValue ConflictResolver::makeDiffObject(Document& doc, const Path& path, const ConstValue& oldValue, const ConstValue& newValue) {


		if (deletedItem == null) deletedItem = db.json("deleted");
		Container diff = db.json.object();

		mergeTwoObjects(oldValue,newValue,[&](const ConstStrA keyName, const ConstValue &oldv, const ConstValue &newv) {
			if (path.isRoot() && keyName != "_attachments" && keyName.head(1) == ConstStrA("_")) return;
			if (newv == null) {
				if (path.isRoot() && keyName == "_attachments") {
					ConstValue fakeObj = db.json.object();
					ConstValue x = diffValue(doc,Path(path,keyName),oldv,fakeObj);
					if (x != null)
						diff.set(keyName, x);
				} else {
					diff.set(keyName, deletedItem);
				}
			} else if (oldv == null) {
				diff.set(keyName,newv);
			} else {
				ConstValue x= diffValue(doc,Path(path,keyName),oldv,newv);
				if (x != nil)
					diff.set(keyName, x);
			}
		});
	if (diff.empty()) return null;
		diff.set("_diff",deletedItem);
		return diff;

}

Value ConflictResolver::patchObject(Document& doc, const Path& path, const ConstValue& oldValue, const ConstValue& newValue) {
	if (newValue == null) {
		return oldValue->copy(db.json.factory,1);
	}
	if (isObjectDiff(newValue)) {
		Value res = db.json.object();

		mergeTwoObjects(oldValue,newValue,[&](const ConstStrA keyName, const ConstValue &oldv, const ConstValue &newv) {
			if (oldv == null) {
				if (newv != deletedItem && !isObjectDiff(newv))
					res.set(keyName, newv->copy(db.json.factory));
			} else if (newv == null) {
				res.set(keyName, oldv->copy(db.json.factory));
			} else {
				if (newv != deletedItem) {
					if (newv->isObject()) {
						res.set(keyName, patchObject(doc,Path(path,keyName),oldv,newv));
					} else {
						res.set(keyName, newv->copy(db.json.factory));
					}
				}
			}
		});



		return res;
	} else {
		return newValue->copy(db.json.factory,1);
	}

}

ConstValue ConflictResolver::resolveConflict(Document& doc, const Path& path ,const ConstValue& leftValue, const ConstValue& rightValue) {
	if (path.getParent().isRoot() && path.getKey() == "_attachments" ) {
		if (leftValue == deletedItem)
			return rightValue;
		else if (rightValue == deletedItem)
			return leftValue;
		else {
			return mergeDiffs(doc,path,leftValue,rightValue);
		}
	} else {
		if (rightValue == deletedItem)
			return rightValue;
		else
			return leftValue;
	}
}

Container ConflictResolver::mergeDiffs(Document& doc, const Path& path, const ConstValue& leftValue, const ConstValue& rightValue) {
	Container res = db.json.object();

	if (leftValue == null) {
		if (rightValue == null) return null;
		res.load(rightValue);
	} else if (rightValue == null) {
		res.load(leftValue);
	} else {
		mergeTwoObjects(leftValue,rightValue,[&](const ConstStrA keyName, const ConstValue &lkv, const ConstValue &rkv) {

			if (lkv == null) {
				//add it to result
				res.set(keyName, rkv);
			//left key goes first
			} else if (rkv == null) {
				//add it to result
				res.set(keyName, lkv);
			//both are equal
			} else {
				//we found conflict
				//create path
				Path p(path,keyName);
				//if they both are objects - we can try to resolve conflict now
				if (rkv->isObject() && lkv->isObject()) {
					//determine, which is diff (they don't need to be diff, especialy when both doesn't exist in base revision)
					bool drkv = isObjectDiff(rkv);
					bool dlkv = isObjectDiff(lkv);
					//right is diff
					if (drkv) {
						//left is diff
						if (dlkv) {
							//resolve it by merging diffs
							res.set(keyName,mergeDiffs(doc,p,lkv,rkv));
						} else {
							//if left is not diff, patch right diff to the left object
							//however, this is conflict, so we must report this issue
							//merged object is put left, because default implementation will use it
							res.set(keyName,resolveConflict(doc,p,patchObject(doc,p,lkv,rkv),lkv));
						}
						//if right is not diff
					} else if (dlkv) {
						//patch right object and ask to resolution
						res.set(keyName,resolveConflict(doc,p,patchObject(doc,p,rkv,lkv),rkv));
					} else {
						//if boths objects are not diffs and are not same
						if (*rkv != *lkv) {
							//ask to resolve conflict
							res.set(keyName,resolveConflict(doc,p,lkv,rkv));
						}
					}
				//for other types
				} else {
					//are they same
					if (*rkv != *lkv) {
						//if not, ask for resolution
						res.set(keyName,resolveConflict(doc,p,lkv,rkv));
					} else {
						res.set(keyName,rkv);
					}
				}
			}
		});
	}
	return res;
}

bool ConflictResolver::isObjectDiff(const ConstValue &v) {
	ConstValue isdiff = v["_diff"];
	return isdiff != null && isdiff == deletedItem;
}


} /* namespace LightCouch */
