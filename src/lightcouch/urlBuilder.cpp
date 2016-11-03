/*
 * urlBuilder.cpp
 *
 *  Created on: 24. 10. 2016
 *      Author: ondra
 */

#include <lightspeed/base/iter/iterConv.h>
#include <lightspeed/utils/urlencode.h>

#include "lightspeed/base/containers/autoArray.tcc"
#include "urlBuilder.h"

namespace LightCouch {


void LightCouch::UrlBuilder::init(ConstStrA basicUrl, ConstStrA dbname, ConstStrA resourcePath) {
	buffer.clear();
	if (resourcePath.empty()) {
		buffer.blockWrite(basicUrl,true);
		buffer.blockWrite(dbname,true);
	} else if (resourcePath[0] == '/') {
		buffer.blockWrite(basicUrl,true);
		buffer.blockWrite(resourcePath.offset(1),true);
	} else {
		buffer.blockWrite(basicUrl,true);
		buffer.blockWrite(dbname,true);
		buffer.write('/');
		buffer.blockWrite(resourcePath,true);
	}
	curSep = '/';
}

UrlBuilder &LightCouch::UrlBuilder::add(StrView path) {
	ConvertReadIter<UrlEncodeConvert, StrView::Iterator> rd(path.getFwIter());
	buffer.write(curSep);
	buffer.copy(rd);
	return *this;
}

UrlBuilder &LightCouch::UrlBuilder::add(StrView key, StrView value) {
	if (curSep == '/') curSep = '?'; else curSep = '&';
	ConvertReadIter<UrlEncodeConvert, StrView::Iterator> rdkey(key.getFwIter());
	ConvertReadIter<UrlEncodeConvert, StrView::Iterator> rdvalue(value.getFwIter());
	buffer.write(curSep);
	buffer.copy(rdkey);
	buffer.write('=');
	buffer.copy(rdvalue);
	return *this;
}

UrlBuilder &LightCouch::UrlBuilder::addJson(StrView key, Value value) {
	if (curSep == '/') curSep = '?'; else curSep = '&';
	ConvertReadIter<UrlEncodeConvert, StrView::Iterator> rdkey(key.getFwIter());
	buffer.write(curSep);
	buffer.copy(rdkey);
	buffer.write('=');
	UrlEncodeConvert conv;
	value.serialize([&](char c) {
		conv.write(c);
		while (conv.hasItems) buffer.write(conv.getNext());
	});
	return *this;
}

void UrlBuilder::init() {
}


} /* namespace LightCouch */
