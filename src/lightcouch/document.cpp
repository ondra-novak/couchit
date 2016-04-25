/*
 * document.cpp
 *
 *  Created on: 4. 4. 2016
 *      Author: ondra
 */

#include "document.h"
#include "lightspeed/base/containers/autoArray.tcc"

namespace LightCouch {

const JSON::INode& LightCouch::Document::operator [](ConstStrA key) const {
	return *(allData[key]);
}



LightCouch::Document::Document(JSON::Value allData):allData(allData)
	,id(allData->operator ()("_id",ConstStrA()))
	,revision(allData->operator ()("_rev",ConstStrA()))
	,conflicts(allData->getPtr("_conflicts"))
	,attachments(allData->getPtr("_attachments"))
{
}

Conflicts::Iterator Conflicts::findRevision(ConstStrA revision) const {

}


} /* namespace LightCouch */

