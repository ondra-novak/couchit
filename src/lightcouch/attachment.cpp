/*
 * attachment.cpp
 *
 *  Created on: 19. 6. 2016
 *      Author: ondra
 */

#include <lightspeed/base/containers/convertString.h>
#include <lightspeed/utils/base64.h>
#include "attachment.h"

#include <lightspeed/base/iter/iterConv.h>
namespace LightCouch {


String AttachmentDataRef::toBase64() const {
	AutoArrayStream<char, SmallAlloc<1024> > buff;
	ConvertWriteIter<ByteToBase64Convert,decltype(buff) &>(buff).copy(ConstBin(*this).getFwIter());
	return StringRef(buff.getArray());
}

Value AttachmentDataRef::toInline() const {
	return Object("content_type",contentType)
			   ("data",toBase64());
}

AttachmentData::AttachmentData(const Value &attachment)
	:AttachmentDataRef(ConstBin(), attachment["content_type"].getString())
{

	AttachmentData x = fromBase64(attachment["data"].getString(),String());
	bindata = x.bindata;
}

AttachmentData AttachmentData::fromBase64(const StringRef &base64, const StringRef &contentType) {
	StringB b = convertString(Base64ToByteConvert(), ConstStrA(base64));
	return AttachmentData(b,contentType);
}



}
