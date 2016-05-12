/*
 * document.h
 *
 *  Created on: 4. 4. 2016
 *      Author: ondra
 */

#ifndef LIBS_LIGHTCOUCH_SRC_LIGHTCOUCH_DOCUMENT_H_
#define LIBS_LIGHTCOUCH_SRC_LIGHTCOUCH_DOCUMENT_H_
#include "lightspeed/base/containers/constStr.h"
#include <lightspeed/utils/json/json.h>
#include "lightspeed/base/containers/autoArray.h"
namespace LightCouch {

using namespace LightSpeed;

///Base class for Document and ConstDocument
template<typename V, typename W>
class DocumentBase: public V {
public:


	const ConstStrA id;

	const ConstStrA revision;

	const W conflicts;

	const W attachments;


	DocumentBase(const V &allData);

protected:

};

class Document: public DocumentBase<JSON::Value, JSON::Container> {
public:
	Document(const JSON::Value &allData);
};

class ConstDocument: public DocumentBase<JSON::ConstValue, JSON::ConstValue> {
public:
	ConstDocument(const JSON::ConstValue &allData);

	///Makes copy of const document for editing
	Document getEditable(const JSON::Builder &json) const;
};


class Conflicts: public AutoArray<ConstDocument> {
public:
	Iterator findRevision(ConstStrA revision) const;
};

} /* namespace LightCouch */

#endif /* LIBS_LIGHTCOUCH_SRC_LIGHTCOUCH_DOCUMENT_H_ */
