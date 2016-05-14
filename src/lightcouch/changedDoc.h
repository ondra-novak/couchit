/*
 * changeDoc.h
 *
 *  Created on: 21. 3. 2016
 *      Author: ondra
 */

#ifndef LIBS_LIGHTCOUCH_SRC_LIGHTCOUCH_CHANGEDDOC_H_
#define LIBS_LIGHTCOUCH_SRC_LIGHTCOUCH_CHANGEDDOC_H_

#include <lightspeed/base/containers/constStr.h>
#include <lightspeed/utils/json/json.h>

#include "object.h"


namespace LightCouch {

using namespace LightSpeed;


class ChangedDoc {

public:
	///contains whole changeset as it was received from the feed
	const Value allData;
	///sequence number
	const natural seqId;
	///document id
	const ConstStrA id;
	///list of revisions changed
	const Value revisions;
	///true, if document has been deleted
	const bool deleted;
	///document, if requested, or null if not available
	const Value doc;
	ChangedDoc(const JSON::Value &allData);
};


};


#endif /* LIBS_LIGHTCOUCH_SRC_LIGHTCOUCH_CHANGEDDOC_H_ */
