/*
 * document.cpp
 *
 *  Created on: 4. 4. 2016
 *      Author: ondra
 */

#include "document.h"
#include "lightspeed/base/containers/autoArray.tcc"

namespace LightCouch {




template<typename V, typename W>
DocumentBase<V,W>::DocumentBase(const V &allData):V(allData)
	,id(allData["_id"].getOrDefault(ConstStrA()))
	,revision(allData["_rev"].getOrDefault(ConstStrA()))
	,conflicts(allData["_conflicts"])
	,attachments(allData["_attachments"])
{
}

Conflicts::Iterator Conflicts::findRevision(ConstStrA revision) const {

}

ConstDocument::ConstDocument(const JSON::ConstValue& allData)
:DocumentBase<JSON::ConstValue,JSON::ConstValue>(allData)
{
}

Document::Document(const JSON::Value& allData)
	:DocumentBase<JSON::Value,JSON::Container>(allData) {
}

Document ConstDocument::getEditable(const JSON::Builder &json) const {

	JSON::Value out = json.object();

	safeGet()->enumEntries(JSON::IEntryEnum::lambda([&out,&json](const JSON::INode *nd, ConstStrA key,natural){
		if (key.head(1) == ConstStrA('_')) {
			if (key != "_id" && key != "_rev") return false;
		}
		out.set(key,JSON::Value(nd->copy(json.factory)));
		return false;
	}));

	return Document(out);
}

} /* namespace LightCouch */

