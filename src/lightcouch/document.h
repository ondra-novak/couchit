/*
 * document.h
 *
 *  Created on: 4. 4. 2016
 *      Author: ondra
 */

#ifndef LIBS_LIGHTCOUCH_SRC_LIGHTCOUCH_DOCUMENT_H_
#define LIBS_LIGHTCOUCH_SRC_LIGHTCOUCH_DOCUMENT_H_
#include "lightspeed/base/containers/constStr.h"
#include "lightspeed/base/containers/autoArray.h"

#include "json.h"

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
class Document: public json::Object {
public:

	///create empty document
	Document() {}
	///create document from existing JSON document
	Document(const Value &base);
	///Initialize document with id and revision
	/**
	 * @param id id of document
	 * @param rev revision. It can be empty for new document
	 */
	Document(const StringRef &id, const StringRef &rev);


	///Retrieves current document ID. Empty if missing
	StringRef getID() const;
	///Retrieves current document revision ID. Empty if missing
	StringRef getRev() const;

	///Retrieves current document ID. Function returns null if missing
	Value getIDValue() const;
	///Retrieves current document revision ID. Function returns null if missing
	Value getRevValue() const;

	void deleteAttachment(const StringRef &name);
	void inlineAttachment(const StringRef &name, const AttachmentDataRef &data);
	Value getAttachment(const StringRef &name) const;



	///Sets complete revision content from a value
	/**
	 * @param v content of new document which replaces old document. The value must be
	 *  JSON object. The keys _id and _rev are replaced by values from the document.
	 *  You can use this function to set new content to current document while its _id and _rev
	 *  persists
	 */
	void setContent(const Value &v);


	///Sets document deleted
	/** Document marked as deleted updated through Changeset object will be deleted. You
	 * can specify which fields will be kept.
	 * @param fieldsToKept list of fields that will be kept. It is important for filtered replication.
	 * It is not good idea to keep whole document, because it still wastes a space
	 *
	 * @param timestamp if set true, the deleted document will have timestamp of deletion. It
	 * is recommended if you plan to purge the database someday. This will help to find out, how
	 * old the deleted document is. You can also let the replication to skip very old deleted
	 * documents so it is safe to purge them.
	 */
	void setDeleted(StringRefT<StringRef> fieldsToKept = StringRefT<StringRef>(), bool timestamp=true);

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

	void setID(const Value &id);
	void setRev(const Value &rev);

	///Retrieves list of conflicts to be deleted by this document (because conflicts has been resolved)
	Value getConflictsToDelete() const;

	///Retrieves all attatchments
	Value attachments() const;

	///Retrieves all conflicts
	Value conflicts() const;

	///Retrieves timestamp of last change
	/**
	 * @return timestamp of last change
	 *
	 * @note Timestamping must be enabled for the document. See enableTimestamp(). If
	 * timestamp is not available, function returns undefined
	 */
	Value getTimestamp() const;

	///Retrieves id of previous revision
	/**
	 * @return id of previous revision.
	 *
	 * @note Revision tracking must be enabled for the document. see enableRevTracking()
	 */
	String getPrevRevision() const;

	///Determines, whether document is deleted
	/**
	 * @retval true, document is deleted
	 * @retval false, document is not deleted
	 */
	bool isDeleted() const;

protected:
	Value conflictsToDelete;

};




class Conflicts: public  AutoArray<Document> {
public:


};

} /* namespace LightCouch */


#endif /* LIBS_LIGHTCOUCH_SRC_LIGHTCOUCH_DOCUMENT_H_ */
