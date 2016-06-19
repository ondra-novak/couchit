/*
 * attachment.cpp
 *
 *  Created on: 19. 6. 2016
 *      Author: ondra
 */

#include <lightspeed/base/containers/convertString.h>
#include <lightspeed/utils/base64.h>
#include "attachment.h"

namespace LightCouch {


StringA AttachmentDataRef::toBase64() const {
	return convertString(Base64Encoder(), ConstBin(*this));
}

Value AttachmentDataRef::toInline(const Json& json) const {
	return json("content_type",contentType)
			   ("data",toBase64());
}

AttachmentData::AttachmentData(const ConstValue &attachment)
	:AttachmentDataRef(ConstBin(), attachment["content_type"].getStringA())
{
	AttachmentData x = fromBase64(attachment["data"].getStringA(),StringA());
	bindata = x.bindata;
}

AttachmentData AttachmentData::fromBase64(ConstStrA base64, ConstStrA contentType) {
	StringB b = convertString(Base64Decoder(), base64);
	return AttachmentData(b,contentType);
}



}
