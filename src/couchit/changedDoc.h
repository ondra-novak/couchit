/*
 * changedDoc.h
 *
 *  Created on: 13. 8. 2018
 *      Author: ondra
 */

#ifndef SRC_COUCHIT_SRC_COUCHIT_CHANGEDDOC_H_
#define SRC_COUCHIT_SRC_COUCHIT_CHANGEDDOC_H_
#include <imtjson/value.h>


namespace couchit {


///Contains information about changed document
class ChangedDoc: public json::Value {

public:
	///sequence number
	const json::Value seqId;
	///document id
	const json::StrViewA id;
	///list of revisions changed
	const json::Value revisions;
	///true, if document has been deleted
	const bool deleted;
	///document, if requested, or null if not available
	const json::Value doc;
	///Constructor.
	/**
	 * @param allData json record containing information about changed document. You can use
	 * result of Changes::getNext() or Changes::peek()
	 */
	ChangedDoc(const json::Value &allData);
};

}


#endif /* SRC_COUCHIT_SRC_COUCHIT_CHANGEDDOC_H_ */
