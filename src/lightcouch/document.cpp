/*
 * document.cpp
 *
 *  Created on: 4. 4. 2016
 *      Author: ondra
 */

#include "document.h"
#include "lightspeed/base/containers/autoArray.tcc"

namespace LightCouch {


Document::Document(const ConstValue& base):ConstValue(base),base(base) {
}

Document::Document(const Value& editableBase):ConstValue(editableBase),base(editableBase),editing(editableBase) {
}

ConstStrA Document::getID() const {
	ConstValue v = (*this)["_id"];
	if (v != null) return v.getStringA();
	else return ConstStrA();
}

ConstStrA Document::getRev() const {
	ConstValue v = (*this)["_rev"];
	if (v != null) return v.getStringA();
	else return ConstStrA();
}

Document &Document::set(ConstStrA key, const Value& value) {
	editing.set(key,value);
	return *this;
}

Document &Document::unset(ConstStrA key) {
	editing.unset(key);
	return *this;
}

void Document::revert() {
	editing = null;
	*this = base;
}

Json::Object Document::edit(const Json& json) {
	if (editing == null) {
		if (base == null) editing = json.object();
		else editing = base->copy(json.factory);
		*this = editing;
	}
	return json.object(editing);
}



} /* namespace LightCouch */
