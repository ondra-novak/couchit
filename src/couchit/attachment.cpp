/*
 * attachment.cpp
 *
 *  Created on: 19. 6. 2016
 *      Author: ondra
 */


#include "attachment.h"
#include <imtjson/binary.h>
#include <imtjson/stringValue.h>

namespace couchit {


String AttachmentDataRef::toBase64() const {
	std::ostringstream out;
	json::base64->encodeBinaryValue(BinaryView(*this), [&](StrViewA c){
		out.write(c.data, c.length);
	});
	return out.str();
}

Value AttachmentDataRef::toInline() const {
	return Object("content_type",contentType)
			   ("data",toBase64());
}

AttachmentData::AttachmentData(const Value &attachment)
	:AttachmentDataRef(BinaryView(nullptr, 0), attachment["content_type"].getString())
{

	AttachmentData x = fromBase64(attachment["data"].getString(),StrViewA());
	bindata = x.bindata;
}

AttachmentData::AttachmentData(Download&& dwn):AttachmentDataRef(BinaryView(0,0),dwn.contentType.str())
,ctx(dwn.contentType)
{
	RefCntPtr<StringValue> data = new(dwn.length) StringValue(json::base64,dwn.length,[&](char *b) {
		std::size_t remain = dwn.length;
		std::size_t sz = dwn.read(b,remain);
		while (sz != 0) {
			b+=sz;
			remain -=sz;
			if (remain == 0) break;
			sz = dwn.read(b,remain);
		}
		return dwn.length;
	});

	bindata = String(Value(PValue::staticCast(data)));
	BinaryView &x = (*this);
	x = BinaryView(bindata.str());


}


AttachmentData AttachmentData::fromBase64(const StrViewA &base64, const StrViewA &contentType) {
	Value v = json::base64->decodeBinaryValue(base64);
	return AttachmentData(String(v),contentType);
}



String Upload::finish() {
	String res = tptr->finish();
	tptr = nullptr;
	return res;

}

Upload::Upload(Target* t):write(*t),tptr(t) {

}

Download::Download(Source* s, const String contentType,
		const String etag, const std::size_t length, const bool notModified)
	:read(*s)
	,contentType(contentType)
	,etag(etag)
	,length(length)
	,notModified(notModified)
	,sptr(s)
{

}

}

