/*
 * document.h
 *
 *  Created on: 4. 4. 2016
 *      Author: ondra
 */

#ifndef LIBS_LIGHTCOUCH_SRC_LIGHTCOUCH_DOCUMENT_H_
#define LIBS_LIGHTCOUCH_SRC_LIGHTCOUCH_DOCUMENT_H_

#include "json.h"

#include "exception.h"
namespace couchit {

class AttachmentDataRef;


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
	Document(const StrViewA &id, const StrViewA &rev);


	///Retrieves current document ID. Empty if missing
	StrViewA getID() const;
	///Retrieves current document revision ID. Empty if missing
	StrViewA getRev() const;

	///Retrieves current document ID. Function returns null if missing
	Value getIDValue() const;
	///Retrieves current document revision ID. Function returns null if missing
	Value getRevValue() const;

	void deleteAttachment(const StrViewA &name);
	void inlineAttachment(const StrViewA &name, const AttachmentDataRef &data);
	Value getAttachment(const StrViewA &name) const;
	void optimizeAttachments();



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
	void setDeleted(StringView<StrViewA> fieldsToKept = StringView<StrViewA>(), bool timestamp=true);

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


	void setID(const Value &id);
	void setRev(const Value &rev);


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


	///Determines, whether document is deleted
	/**
	 * @retval true, document is deleted
	 * @retval false, document is not deleted
	 */
	bool isDeleted() const;

	///Clears content, but left _id and _rev
	void clear();

	bool isSame(const Value &otherDoc) const;
	bool isNewer(const Value &otherDoc) const;
	bool isNew() const {return !getRevValue().defined();}



};


} /* namespace couchit */


#endif /* LIBS_LIGHTCOUCH_SRC_LIGHTCOUCH_DOCUMENT_H_ */
