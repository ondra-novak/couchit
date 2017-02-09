/*
 * attachment.cpp
 *
 *  Created on: 19. 6. 2016
 *      Author: ondra
 */


#include "attachment.h"
#include <imtjson/binary.h>

namespace LightCouch {


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

AttachmentData::AttachmentData(Download&& dwn):AttachmentDataRef(ConstBin(),dwn.contentType.str())
,ctx(dwn.contentType)
{
	byte *b = bindata.createBuffer(dwn.length);
	std::size_t remain = dwn.length;
	std::size_t sz = dwn.read(b,remain);
	while (sz != 0) {
		b+=sz;
		remain -=sz;
		if (remain == 0) break;
		sz = dwn.read(b,remain);
	}

	ConstBin &x = (*this);
	x = bindata;
}


AttachmentData AttachmentData::fromBase64(const StrViewA &base64, const StrViewA &contentType) {
	StringB b = convertString(Base64ToByteConvert(), ConstStrA(base64));
	return AttachmentData(b,contentType);
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
	:read(*s),contentType(contentType),etag(etag),length(length),notModified(notModified)
{

}

}

