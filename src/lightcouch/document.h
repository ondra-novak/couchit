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

#include "exception.h"
namespace LightCouch {

class AttachmentDataRef;

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

	///Retrieves current document ID. Function returns null if missing
	ConstValue getIDValue() const;
	///Retrieves current document revision ID. Function returns null if missing
	ConstValue getRevValue() const;

	void deleteAttachment(ConstStrA name);
	void inlineAttachment(const Json &json, ConstStrA name, const AttachmentDataRef &data);
	ConstValue getAttachment(ConstStrA name) const;


	///sets field in document
	/** Function can be called after edit(), otherwise exception can be thrown() */
	Document &set(ConstStrA key, const Value &value);
	Document &unset(ConstStrA key);

	///Delete changes in document
	void revert();
	///Create new revision and enable editing
	/**
	 * @param json reference to json factory available in many objects of couchdb (such a json).
	 *  If you have Changeset object, you can use public member "json" (Changeset::json) to provide
	 *  this argument
	 *
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




	///Sets complete revision from value
	/**
	 * @param v new document replaces old one. However, you cannot change _id and _revision (which are
	 * restored from base revision)
	 */
	void setRevision(const Value &v) {editing = v;ConstValue::operator=(v);cleanup();}

	///Sets revision from another const document, it creates copy
	/**
	 *
	 * @param json JSON factory that provides new value creation
	 * @param v new revision - it will replace current editing revision
	 *
	 */
	void setRevision(const Json &json, const ConstValue &v);

	///Sets document deleted
	/** Document marked as deleted updated through Changeset object will be deleted. You
	 * can specify which fields will be kept.
	 * @param json JSON factory that provides new value creation
	 * @param fieldsToKept list of fields tath will be kept. It is important for filtered replication.
	 * It is not good idea to keep whole document, because it still wastes a space
	 *
	 * Deleted document has always timestamp enabled
	 */
	void setDeleted(const Json &json, ConstStringT<ConstStrA> fieldsToKept = ConstStringT<ConstStrA>());

	///Enables timestamping of changes
	/** Document with timestamps carries field, which contains timestamp of last update. Once this is
	 * enabled, timestamp is updated everytime document is changed. This is behaviour of lightcouch. If
	 * document is modified outside of lightcouch, timestamp is not automatically updated.
	 *
	 * Timestamp is stored in "!timestamp" field.
	 *
	 * Deleted documents have always timestamp to be able to retrive time of deletion for tombstone cleanup.
	 * You can use filtered "changes" to receive very old tombstones
	 *
	  @param json json object that provides new value creation

	  @note timestamp is stored in CouchDB::fldTimestamp
	 */
	void enableTimestamp();

	///Enables tracking revision tree
	/** By default couchDb traces only revisions of main branch while side revision are considered
	 * as conflicts without history. Enabling revision tracking causes that special field will
	 * contain ID of previous revision. This is done by Changeset object before document is updated.
     *
	 * @param json json object that provides new value creation
	 *
	 * @note previous revision is stored in CouchDB::fldPrevRevision.
	 * First revision has this field equal to null
	 */
	void enableRevTracking();

	///States, that document resolved all conflicts so conflicts can be deleted during update
	/** This state can be cleared by revert()
	 *
	 * Function simply put _conflicts into separate field
	 *
	 *  */
	void resolveConflicts();

	void setID(const ConstValue &id);
	void setRev(const ConstValue &rev);

	ConstValue getConflictsToDelete() const;

protected:
	ConstValue base;
	Value editing;
	ConstValue conflictsToDelete;

	void cleanup();
};




class Conflicts: public  AutoArray<Document> {
public:


};

} /* namespace LightCouch */


#endif /* LIBS_LIGHTCOUCH_SRC_LIGHTCOUCH_DOCUMENT_H_ */
