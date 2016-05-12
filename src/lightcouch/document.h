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

template<typename V>
class DocumentT {
public:

	///contains whole document as json
	const V allData;

	const ConstStrA id;

	const ConstStrA revision;

	const V conflicts;

	const V attachments;

	const JSON::INode &operator[](ConstStrA key) const;


	DocumentT(JSON::Value allData);

protected:

};

class Conflicts: public AutoArray<Document> {
public:
	Iterator findRevision(ConstStrA revision) const;
};

} /* namespace LightCouch */

#endif /* LIBS_LIGHTCOUCH_SRC_LIGHTCOUCH_DOCUMENT_H_ */
