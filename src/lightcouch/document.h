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

#include "object.h"

namespace LightCouch {

using namespace LightSpeed;

///Contains document fetched from couchdb, it also handles editing
/** By default, this is extension of ConstValue, however, you can change it.
 *
 *  Document still keeps original version. By calling edit() you can start updating fields.
 *  First call of edit() makes a copy of the document. This copy overrides original content, however
 *  you can anytime revert changes by calling rever() function.
 *
 */
class Document: public ConstValue {
public:

	///create empty document
	Document() {}
	///create document from existing JSON document
	Document(const ConstValue &base);
	///create empty document and attach updates from specified json
	Document(const Value &editableBase);


	///Retrieves current document ID. Empty if missing
	ConstStrA getID() const;
	///Retrieves current document revision ID. Empty if missing
	ConstStrA getRev() const;


	///sets field in document
	/** Function can be called after edit(), otherwise exception can be thrown() */
	Document &set(ConstStrA key, const Value &value);
	Document &unset(ConstStrA key);

	///Delete changes in document
	void revert();
	///Create new revision and enable editing
	/**
	 * @param json reference to json factory available in many objects of couchdb (such a json)
	 * @return object Json::Object - helper object that can be used to construct new values
	 *
	 * You can call edit() more then once. Each next call just construct Json::Object to
	 * add new values. Only first call creates new revision.
	 *
	 * once new revision is created, document starts to map fields to new revision. You can
	 * edit document without modifying original document.
	 */
	Json::Object edit(const Json &json);

	///Convert editing object to container
	operator const Container &() const {return editing;}

	///Retrieves base revision (if exists)
	const ConstValue& getBase() const {
		return base;
	}

	///Retrieves current editable revision (if created)
	const Value& getEditing() const {
		return editing;
	}

	bool dirty() const {return editing != null;}

protected:
	ConstValue base;
	Value editing;

};


class Conflicts: public  AutoArray<Document> {
public:


};

} /* namespace LightCouch */


#endif /* LIBS_LIGHTCOUCH_SRC_LIGHTCOUCH_DOCUMENT_H_ */
