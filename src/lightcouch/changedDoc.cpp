/*
 * changeDoc.cpp
 *
 *  Created on: 21. 3. 2016
 *      Author: ondra
 */

#include "changedDoc.h"

namespace LightCouch {


static bool safeBool(const JSON::INode *ptr) {
	if (ptr == 0) return false;
	return ptr->getBool();
}



ChangedDoc::ChangedDoc(const JSON::Value& allData)
:allData(allData)
,seqId(allData["seq"]->getUInt())
,id(allData["id"]->getStringUtf8())
,revisions(allData["changes"])
,deleted(safeBool(allData->getPtr("deleted")))
,doc(allData->getPtr("doc"))
{
}

}
