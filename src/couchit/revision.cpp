/*
 * revision.cpp
 *
 *  Created on: May 28, 2016
 *      Author: ondra
 */

#include "revision.h"

#include "num2str.h"
#include "str2num.h"
namespace couchit {



Revision::Revision():revId(0),revTagOffs(1) {

}

Revision::Revision(std::size_t revId, const String &revTag)
	:revId(revId),revTag(revTag), revTagOffs(0)
{
}

Revision::Revision(const String &rev) {
	std::size_t sep = rev.indexOf("-");
	if (sep == std::string_view::npos) {
		revId = 0;
		revTag = rev;
		revTagOffs = 0;
	} else {
		revId = stringToUnsigned(rev.substr(0,sep));
		revTag = rev;
		revTagOffs = sep+1;
	}
}

Revision::Revision(const Value &rev):Revision(String(rev)) {}

String Revision::toString() const {

	if (revTagOffs != 0) return revTag;
	else {
		char buff[50];
		BufferOutput out(buff);
		auto size = unsignedToString<BufferOutput &>(out,revId,50,10);
		return String( {std::string_view(buff,size),"-",getTag()} );
	}
}

CompareResult Revision::compare(const Revision& other) const {
	if (revId < other.revId) return -1;
	if (revId > other.revId) return 1;
	return getTag().compare(other.getTag());
}


Revision SeqNumber::initRev(const Value& sn) {
	if (sn.type() == json::number) {
		//CouchDB ver < 2
		return Revision(sn.getNumber(),String());
	} else {
		//couchDB ver >= 2
		return Revision(sn.toString());
	}
}



}

