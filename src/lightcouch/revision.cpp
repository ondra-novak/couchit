/*
 * revision.cpp
 *
 *  Created on: May 28, 2016
 *      Author: ondra
 */

#include <lightspeed/base/exceptions/invalidNumberFormat.h>
#include <lightspeed/base/text/textParser.h>
#include "revision.h"
#include "lightspeed/base/text/toString.tcc"

#include "lightspeed/base/exceptions/invalidParamException.h"

#include "lightspeed/base/text/textParser.tcc"
namespace LightCouch {

Revision::Revision():revId(0) {
}

Revision::Revision(natural revId, ConstStrA tag):revId(revId),tag(tag) {

}

StringA Revision::toString() const {
	ToString<natural> strRev(revId);
	StringA ret;
	StringA::WriteIterator iter = ret.createBufferIter(strRev.length()+1+tag.length());
	iter.blockWrite(strRev, true);
	iter.write('-');
	iter.blockWrite(tag, true);
	return ret;
}

Revision::Revision(ConstStrA revStr):revId(getRevId(revStr)),tag(getTag(revStr)) {
}

natural Revision::getRevId(ConstStrA rev) {
	natural dash = rev.find('-');
	if (dash == naturalNull) throw InvalidParamException(THISLOCATION,1,"Argument is not correct CouchDB revision id");
	ConstStrA numbPart(rev.head(dash));
	ConstStrA::Iterator iter = numbPart.getFwIter();
	natural out;
	if (!parseUnsignedNumber(iter,out,10) || iter.hasItems())
		throw InvalidNumberFormatException(THISLOCATION, numbPart);
	return out;
}

ConstStrA Revision::getTag(ConstStrA rev) {
	natural dash = rev.find('-');
	return rev.offset(dash+1);
}

CompareResult Revision::compare(const Revision& other) const {
	if (revId < other.revId) return cmpResultLess;
	if (revId > other.revId) return cmpResultGreater;
	if (tag < other.tag) return cmpResultLess;
	if (tag > other.tag) return cmpResultGreater;
	return cmpResultEqual;
}

} /* namespace assetex */
