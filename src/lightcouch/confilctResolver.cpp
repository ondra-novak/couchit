/*
 * confilctResolver.cpp
 *
 *  Created on: May 22, 2016
 *      Author: ondra
 */

#include <lightspeed/base/containers/stack.tcc>
#include <lightspeed/base/containers/autoArray.tcc>
#include <lightspeed/base/containers/priorityQueue.tcc>
#include "couchDB.h"
#include "document.h"
#include "confilctResolver.h"

#include "lightspeed/base/containers/set.h"
namespace LightCouch {

ConfilctResolver::ConfilctResolver(CouchDB& db):db(db) {
}

Document ConfilctResolver::resolve(const ConstStrA& docId) {

	TextFormatBuff<char, SmallAlloc<256> > fmt;
	fmt("%1?revs=true&conflicts=true") << docId;
	Document headRev = db.jsonGET(fmt.write(),null);
	ConstValue revisions = headRev["_revisions"];
	ConstValue conflicts = headRev["_conflicts"];
	if (conflicts == null) return headRev;




}

Value ConfilctResolver::merge3w(const ConstValue& topdoc,const ConstValue& conflict, const ConstValue& base) {
	Document doc(topdoc);
	return mergeObject(doc,Path(""), topdoc, conflict, base);
}

Value ConfilctResolver::merge2w(const ConstValue& ,const ConstValue& ) {
	return nil;
}

Value ConfilctResolver::mergeValue(Document& doc,const Path& path, const ConstValue& oldValue, const ConstValue& newValue) {
	if (oldValue->getType() == newValue->getType() && oldValue->isObject())
		return patchObject(doc,path,oldValue,newValue);
	else
		return newValue->copy(db.json.factory);

}

ConstValue ConfilctResolver::diffValue(Document& doc, const Path& path, const ConstValue& oldValue, const ConstValue& newValue) {
	//different types, use new value
	if (oldValue->getType() != newValue->getType()) return newValue;
	//make diff
	if (oldValue->isObject()) return makeDiffObject(doc, path, oldValue,newValue);
	if (oldValue->operator == (*newValue)) return nil;
	return newValue;
}

Value ConfilctResolver::mergeObject(Document& doc, const Path& path,const ConstValue& oldValue,
		const ConstValue& newValue,const ConstValue& baseValue) {

	ConstValue diff = makeDiffObject(doc,path, newValue, baseValue);
	return patchObject(doc,path,oldValue, diff);
}




JSON::ConstValue ConfilctResolver::makeDiffObject(Document& doc, const Path& path, const ConstValue& oldValue, const ConstValue& newValue) {


		if (deletedItem == null) deletedItem = db.json("deleted");
		Container diff = db.json.object();

		JSON::ConstIterator o = oldValue->getFwConstIter();
		JSON::ConstIterator n = newValue->getFwConstIter();

		while (n.hasItems() && o.hasItems()) {
			const JSON::ConstKeyValue &nkv = n.peek();
			const JSON::ConstKeyValue &okv = o.peek();
			ConstStrA nk = nkv.getStringKey();
			ConstStrA ok = okv.getStringKey();
			if (nk.head(1) == ConstStrA("_")) {
				n.skip();
			} else if (ok.head(1) == ConstStrA("_")) {
				o.skip();
			} else if (nk < ok) {
				diff.set(nk, nkv);
				n.skip();
			} else if (nk > ok) {
				diff.set(ok, deletedItem);
				o.skip();
			} else {
				ConstValue x= diffValue(doc,Path(path,nk),okv,nkv);
				if (x != nil)
					diff.set(nk, x);
				n.skip();
				o.skip();
			}
		}
		while (n.hasItems()) {
			const JSON::ConstKeyValue &nkv = n.getNext();
			diff.set(nkv.getStringKey(), nkv);
		}
		while (o.hasItems()) {
			const JSON::ConstKeyValue &okv = o.getNext();
			diff.set(okv.getStringKey(), deletedItem);
		}

		if (diff.empty()) return null;
		diff.set("_diff",db.json(true));
		return diff;

}

Value ConfilctResolver::patchObject(Document& doc, const Path& path, const ConstValue& oldValue, const ConstValue& newValue) {
	if (isObjectDiff(newValue)) {
		Value res = db.json.object();

		JSON::ConstIterator o = oldValue->getFwConstIter();
		JSON::ConstIterator n = newValue->getFwConstIter();

		while (n.hasItems() && o.hasItems()) {
			const JSON::ConstKeyValue &nkv = n.peek();
			const JSON::ConstKeyValue &okv = o.peek();



			ConstStrA nk = nkv.getStringKey();
			ConstStrA ok = okv.getStringKey();
			if (nk.head(1) == ConstStrA("_")) {
				n.skip();
			} else if (ok.head(1) == ConstStrA("_")) {
				o.skip();
			} else if (nk < ok) {
				if (nkv != deletedItem && !isObjectDiff(nkv)) {
						res.set(nk, nkv->copy(db.json.factory));
				}
				n.skip();
			} else if (nk > ok) {
				res.set(ok, okv->copy(db.json.factory));
				o.skip();
			} else {
				if (nkv != deletedItem) {
					if (nkv->isObject())
						res.set(nk,patchObject(doc,Path(path,nk),okv,nkv));
					else {
						res.set(nk, nkv->copy(db.json.factory));
					}
				}
				n.skip();
				o.skip();
			}
		}
		while (n.hasItems()) {
			const JSON::ConstKeyValue &nkv = n.getNext();
			if (nkv != deletedItem && !isObjectDiff(nkv)) {
				res.set(nkv.getStringKey(), nkv->copy(db.json.factory));
			}
		}
		while ( o.hasItems()) {
			const JSON::ConstKeyValue &okv = o.getNext();
			res.set(okv.getStringKey(), okv->copy(db.json.factory));
		}
		return res;
	} else {
		return newValue->copy(db.json.factory);
	}


}

ConstValue ConfilctResolver::resolveConflict(Document& doc, const Path& path,const ConstValue& leftValue, const ConstValue& rightValue) {
	if (rightValue == deletedItem) return rightValue;
	else return leftValue;
}

Container ConfilctResolver::mergeDiffs(Document& doc, const Path& path, const ConstValue& leftValue, const ConstValue& rightValue) {
	Container res = db.json.object();

	//retrieve iterators
	JSON::ConstIterator l = leftValue->getFwConstIter();
	JSON::ConstIterator r = rightValue->getFwConstIter();

	//while both have vales (they are ordered)
	while (r.hasItems() && l.hasItems()) {
		//pick right key-value
		const JSON::ConstKeyValue &rkv = r.peek();
		//pick left key-value
		const JSON::ConstKeyValue &lkv = l.peek();

		//pick right key
		ConstStrA rk = rkv.getStringKey();
		//pick left key
		ConstStrA lk = lkv.getStringKey();
		//right key goes first
		if (rk < lk) {
			//add it to result
			res.set(rk, rkv);
			//skip to next
			r.skip();
		//left key goes first
		} else if (rk > lk) {
			//add it to result
			res.set(lk, lkv);
			//skip to next
			l.skip();
		//both are equal
		} else {
			//we found conflict
			//create path
			Path p(path,rk);
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
						res.set(rk,mergeDiffs(doc,p,lkv,rkv));
					} else {
						//if left is not diff, patch right diff to the left object
						//however, this is conflict, so we must report this issue
						//merged object is put left, because default implementation will use it
						res.set(rk,resolveConflict(doc,p,patchObject(doc,p,lkv,rkv),lkv));
					}
					//if right is not diff
				} else if (dlkv) {
					//patch right object and ask to resolution
					res.set(rk,resolveConflict(doc,p,patchObject(doc,p,rkv,lkv),rkv));
				} else {
					//if boths objects are not diffs and are not same
					if (*rkv != *lkv) {
						//ask to resolve conflict
						res.set(rk,resolveConflict(doc,p,lkv,rkv));
					}
				}
			//for other types
			} else {
				//are they same
				if (*rkv != *lkv) {
					//if not, ask for resolution
					res.set(rk,resolveConflict(doc,p,lkv,rkv));
				}
			}
			//skip both sides
			r.skip();
			l.skip();
			//continue here
		}
	}
	//finish right values
	while (r.hasItems()) {
		const JSON::ConstKeyValue &rkv = r.getNext();
		res.set(rkv.getStringKey(),rkv);
	}
	//finish left values
	while (l.hasItems()) {
		const JSON::ConstKeyValue &lkv = l.getNext();
		res.set(lkv.getStringKey(),lkv);
	}

	return res;
}

bool ConfilctResolver::isObjectDiff(const ConstValue &v) {
	ConstValue isdiff = v["diff"];
	return isdiff != null && isdiff.getBool() == true;
}


} /* namespace LightCouch */
