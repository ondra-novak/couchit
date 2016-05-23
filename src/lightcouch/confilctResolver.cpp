/*
 * confilctResolver.cpp
 *
 *  Created on: May 22, 2016
 *      Author: ondra
 */

#include "document.h"
#include "confilctResolver.h"

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
}

ConstValue ConfilctResolver::mergeValue(Document& doc,const Path& path, const ConstValue& oldValue,
		const ConstValue& newValue, const ConstValue& baseValue) {
}

ConstValue ConfilctResolver::diffValue(Document& doc, const Path& path,
		const ConstValue& oldValue, const ConstValue& newValue) {
}

Value ConfilctResolver::mergeObject(Document& doc, const Path& path,const ConstValue& oldValue,
		const ConstValue& newValue,const ConstValue& baseValue) {

/*
	Value diff = db.json.object();
	AutoArray<ConstStrA, SmallAlloc<32> > delItems;
	JSON::ConstIterator o = oldValue->getFwConstIter();
	JSON::ConstIterator n = newValue->getFwConstIter();
	JSON::ConstIterator b = baseValue->getFwConstIter();

	while (n.hasItems() && b.hasItems()) {
		const JSON::ConstKeyValue &nkv = n.peek();
		const JSON::ConstKeyValue &bkv = b.peek();
		ConstStrA nk = nkv.getStringKey();
		ConstStrA bk = bkv.getStringKey();
		if (nk < bk) {
			diff.set(nk, nkv);
			n.skip();
		} else if (nk > bk) {
			delItems.add(bk);
			b.skip();
		} else {

		}
	}
*/

}

JSON::ConstValue ConfilctResolver::makeDiff(Document& doc, const Path& path, const ConstValue& oldValue, const ConstValue& newValue) {
	if (deletedItem == null) deletedItem = db.json("deleted");
	Container diff = db.json.object();

	JSON::ConstIterator o = oldValue->getFwConstIter();
	JSON::ConstIterator n = newValue->getFwConstIter();

	while (n.hasItems() && b.hasItems()) {
		const JSON::ConstKeyValue &nkv = n.peek();
		const JSON::ConstKeyValue &okv = o.peek();
		ConstStrA nk = nkv.getStringKey();
		ConstStrA ok = okv.getStringKey();
		if (nk < ok) {
			diff.set(nk, nkv);
			n.skip();
		} else if (nk > ok) {
			diff.set(ok, deletedItem);
			b.skip();
		} else {
			ConstValue x= diffValue(doc,Path(path,nk),okv,nkv);
			if (x != nil)
				diff.set(nk, x);
		}
	}
	return diff;

}

} /* namespace LightCouch */
