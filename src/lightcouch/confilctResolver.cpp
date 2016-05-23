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
}

Value ConfilctResolver::merge3w(const ConstValue& topdoc,const ConstValue& conflict, const ConstValue& base) {
	Document doc(topdoc);
	return mergeObject(doc,Path(""), topdoc, conflict, base);
}

Value ConfilctResolver::merge2w(const ConstValue& doc,const ConstValue& conflict) {
	return nil;
}

Value ConfilctResolver::mergeValue(Document& doc,const Path& path, const ConstValue& oldValue, const ConstValue& newValue) {
	if (oldValue->getType() == newValue->getType() && (oldValue->isArray() || oldValue->isObject()))
		return patchDiff(doc,path,oldValue,newValue);
	else
		return newValue->copy(db.json.factory);

}

ConstValue ConfilctResolver::diffValue(Document& doc, const Path& path, const ConstValue& oldValue, const ConstValue& newValue) {
	//different types, use new value
	if (oldValue->getType() != newValue->getType()) return newValue;
	//make diff
	if (oldValue->isObject() || oldValue->isArray()) {
		ConstValue diff = makeDiff(doc, path, oldValue,newValue);
		if (diff.empty()) return nil;
	}
	if (oldValue->operator == (*newValue)) return nil;
	else return newValue;
}

Value ConfilctResolver::mergeObject(Document& doc, const Path& path,const ConstValue& oldValue,
		const ConstValue& newValue,const ConstValue& baseValue) {

	ConstValue diff = makeDiff(doc,path, newValue, baseValue);
	return patchDiff(doc,path,oldValue, diff);

}

struct DiffPath {
	const natural indexPrev;
	const bool erase;
	const ConstValue v;

	DiffPath(natural indexPrev,bool erase,const ConstValue &v):indexPrev(indexPrev),erase(erase),v(v) {}
};

struct DiffState : public ComparableLess<DiffState> {
	const natural oindex;
	const natural nindex;
	const natural navindex;
	const natural price;

	DiffState(natural oindex,natural nindex,natural navIndex,natural price)
		:oindex(oindex),nindex(nindex),navindex(navIndex),price(price) {}
	bool lessThan(DiffState &other) const {
		return price > other.price;
	}
};


JSON::ConstValue ConfilctResolver::makeDiff(Document& doc, const Path& path, const ConstValue& oldValue, const ConstValue& newValue) {
	if (oldValue->getType() != newValue->getType()) return newValue;
	if (oldValue->isArray()) {
		if (arrayDiff == null) arrayDiff = db.json("__array_diff__");
		//path (organised from target to start)
		AutoArray<DiffPath> diffPath;
		//queue of opened states
		PriorityQueue<DiffState> stateQueue;
		//already processed stateds
		Set<std::pair<natural,natural> > processed;

		//push initial state
		stateQueue.push(DiffState(0,0,naturalNull,0));

		//repeat
		while (true) {
			//pop state
			DiffState x = stateQueue.top();
			stateQueue.pop();
			//store index of next step
			natural nxtStepIdx = diffPath.length();
			//we will try to stop this branch, if already solved
			bool stopBranch = false;
			//try to lock the state
			processed.insert(std::make_pair(x.oindex,x.nindex),&stopBranch);

			//if state is locked, we can stop this branch now
			if (stopBranch)
				//continue with next state
				continue;

			//if there is no items to process
			if (x.nindex >= newValue.length() && x.oindex >= oldValue.length()) {
				//finalize search
				Stack<natural> revNav;
				//collect path (in reverse order) to the stack
				natural p = x.navindex;
				while (p != naturalNull) {
					revNav.push(p);
					p = diffPath[p].indexPrev;
				}
				//create final result
				Container diff = db.json.array();
				//add magic
				diff.add(arrayDiff);
				//if lengths of arrays are equal
				if (oldValue.length() == newValue.length())
					//store the length
					diff.add(db.json(oldValue.length()));
				else
					//if not equal, we do not care about it, store nil
					diff.add(db.json(nil));

				//accumulates count of skip steps
				natural accum = 0;
				//pick steps from the stack until end
				while (!revNav.empty()) {
					p = revNav.top();
					revNav.pop();
					//p - index into path
					//if v is null - this is skip item, accumulate ot
					if (diffPath[p].v == null) accum++;
					else {
						//if there is accumulated items, write number to the output
						if (accum) {
							//write number
							diff.add(db.json(accum));
							//reset accumulator
							accum = 0;
						}
						//write erase status
						diff.add(db.json(diffPath[p].erase));
						//write value
						diff.add(diffPath[p].v);
					}
				}

				//in case, that diff has two items, arrays was same
				if (diff.length() == 2) return nil;
				return diff;

			} else if (x.nindex >= newValue.length()) {
				//if there is no new items, we have to remove rest of items in old array
				diffPath.add(DiffPath(x.navindex,true,oldValue[x.oindex]));
				//push next state
				stateQueue.push(DiffState(x.oindex+1,x.nindex, nxtStepIdx,x.price+2));
			} else if (x.oindex >= oldValue.length()) {
				//if there is no old items, we have to append rest of items from new array
				diffPath.add(DiffPath(x.navindex,false,newValue[x.nindex]));
				//push next state
				stateQueue.push(DiffState(x.oindex,x.nindex+1, nxtStepIdx,x.price+2));
			} else {
				//ok, we need to compare following two items
				const ConstValue &ov = oldValue[x.oindex];
				const ConstValue &nv = newValue[x.nindex];
				//compare items
				ConstValue d = diffValue(doc,Path(path,""),ov,nv);
				//above function should return  old or new value or nil
				//if nil or old value is returned,  assume, that values are equal or unchanged
				if (d == nil || d == ov) {
					//add unchange state
					diffPath.add(DiffPath(x.navindex,false,null));
					//continue next state
					stateQueue.push(DiffState(x.oindex+1,x.nindex+1, nxtStepIdx, x.price+1));
				//if new value posted?
				} else if (d == nv) {
					//now, we don't know, whether value has been replaced, inserted, or removed old value
					//replace is encoded by insert+remove
					//
					//add appned state with value
					diffPath.add(DiffPath(x.navindex,false,nv));
					//
					stateQueue.push(DiffState(x.oindex,x.nindex+1, nxtStepIdx, x.price+2));
					nxtStepIdx = diffPath.length();
					diffPath.add(DiffPath(x.navindex,true,ov));
					stateQueue.push(DiffState(x.oindex+1,x.nindex, nxtStepIdx, x.price+2));
				} else {
					diffPath.add(DiffPath(nxtStepIdx,false,d));
					natural nextNav2 = diffPath.length();
					diffPath.add(DiffPath(x.navindex,true,ov));
					stateQueue.push(DiffState(x.oindex,x.nindex+1, nextNav2, x.price+4+d.length()));
				}
			}
		}


	} else if (oldValue->isObject()) {
		if (deletedItem == null) deletedItem = db.json("deleted");
		Container diff = db.json.object();

		JSON::ConstIterator o = oldValue->getFwConstIter();
		JSON::ConstIterator n = newValue->getFwConstIter();

		while (n.hasItems() && o.hasItems()) {
			const JSON::ConstKeyValue &nkv = n.peek();
			const JSON::ConstKeyValue &okv = o.peek();
			ConstStrA nk = nkv.getStringKey();
			ConstStrA ok = okv.getStringKey();
			if (nk < ok) {
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
		return diff;
	} else {
		return newValue;
	}

}

Value ConfilctResolver::patchDiff(Document& doc, const Path& path, const ConstValue& oldValue, const ConstValue& newValue) {
	if (oldValue.getType() != newValue.getType()) return oldValue->copy(db.json.factory);
	if (oldValue->isArray()) {
		//patch array
	} else if (oldValue->isObject()) {
		Value res = db.json.object();

		JSON::ConstIterator o = oldValue->getFwConstIter();
		JSON::ConstIterator n = newValue->getFwConstIter();

		while (n.hasItems() && o.hasItems()) {
			const JSON::ConstKeyValue &nkv = n.peek();
			const JSON::ConstKeyValue &okv = o.peek();



			ConstStrA nk = nkv.getStringKey();
			ConstStrA ok = okv.getStringKey();
			if (nk < ok) {
				if (nkv != deletedItem) {
					if (nkv->isArray() || nkv->isObject())
						res.set(nk,patchDiff(doc,Path(path,nk),json.object(),nkv));
					else {
						res.set(nk, nkv->copy(db.json.factory));
					}
				}
				n.skip();
			} else if (nk > ok) {
				res.set(ok, okv->copy(db.json.factory));
				o.skip();
			} else {
				if (nkv != deletedItem) {
					if (nkv->isArray() || nkv->isObject())
						res.set(nk,patchDiff(doc,Path(path,nk),okv,nkv));
					else {
						res.set(nk, nkv->copy(db.json.factory));
					}
				}
				n.skip();
				o.skip();
			}
		}
		return res;
	} else {
		return newValue->copy(db.json.factory);
	}


}

} /* namespace LightCouch */
