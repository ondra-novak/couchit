/*
 * changeSet.h
 *
 *  Created on: 11. 3. 2016
 *      Author: ondra
 */

#ifndef LIBS_LIGHTCOUCH_SRC_LIGHTCOUCH_CHANGESET_H_
#define LIBS_LIGHTCOUCH_SRC_LIGHTCOUCH_CHANGESET_H_
#include <lightspeed/base/containers/constStr.h>
#include <lightspeed/base/exceptions/exception.h>
#include <lightspeed/utils/json/json.h>
#include "couchDB.h"


namespace LightCouch {

class LocalView;

using namespace LightSpeed;

///Collects changes and commits them as one request
class Changeset {
public:
	Changeset(CouchDB &db);
	Changeset(const Changeset& other);



	///Updates document in database
	/** Function schedules document for update. Once document is scheduled, Document instance can
	 * be destroyed, because document is kept inside of changeset. Changes are applied by function commit()
	 *
	 * @param document document to schedule for update
	 * @return reference to this
	 *
	 * @note If document has no _id, it is created automatically by this function,so you can
	 * Immediately read it.
	 *
	 */
	Changeset &update(Document &document);

	///Erases document defined only by documentId and revisionId. Useful to erase conflicts
	/**
	 * @param docId document id as JSON-string. you can use doc["_id"] directly without parsing and composing
	 * @param revId revision id as JSON-string. you can use dic["_rev"] directly without parsing and composing
	 *
	 * @return reference to Changeset to allow create chains
	 *
	 * @note erasing document without its content causes, that minimal tombstone will be created, however
	 * any filters that triggers on content will not triggered. This is best for erasing conflicts because they are
	 * no longer valid.
	 */
	Changeset &erase(ConstValue docId, ConstValue revId);


	///Commits all changes in the database
	/**
	 * @param db database where data will be committed
	 * @param all_or_nothing ensures that whole batch of changes will be updated at
	 * once. If some change is rejected, nothing written to the database. In this
	 * mode, conflict are not checked, so you can easy created conflicted state similar
	 * to conflict during replication. If you set this variable to false, function
	 * writes documents one-by-one and also rejects conflicts
	 *
	 * @return reference to the Changeset to create chains
	 */
	Changeset &commit(CouchDB &db, bool all_or_nothing=true);


	///Commits all changes in the database
	/**
	 * @param all_or_nothing ensures that whole batch of changes will be updated at
	 * once. If some change is rejected, nothing written to the database. In this
	 * mode, conflict are not checked, so you can easy created conflicted state similar
	 * to conflict during replication. If you set this variable to false, function
	 * writes documents one-by-one and also rejects conflicts
	 *
	 * @note change will be committed to the database connection which was used for creation of this
	 * changeset
	 *
	 * @return reference to the Changeset to create chains
	 */
	Changeset &commit(bool all_or_nothing=true);


	///Preview all changes in a local view
	/** Function just only sends all changes to a local view, without making the
	 * set "committed". You can use this function to preview all changes in the view
	 * before it is committed to the database
	 * @param view local view
     *
	 * @return reference to the Changeset to create chains
	 *
	 * @note it is recomended to define map function to better preview, because the function
	 * preview() will use updateDoc()
	 */
	Changeset &preview(LocalView &view);


	///Mark current state of changeset
	/** you can use stored mark to revert unsaved changes later */
	natural mark() const;

	///Revert changes up to mark
	/** Function removes all document stored beyond that mark
	 *
	 * @param mark mark stored by mark()
	 *
	 * @note function only removes documents from the change-set. It cannot
	 * revert changes made in the documents
	 */

	void revert(natural mark);

	///Revert single document
	/**
	 * @param doc document to remove. It must be exact reference to the document
	 *
	 * @note function only removes document from the change-set. It cannot
	 * revert changes made in the document.
	 *
	 * @note function can be slow for large changesets because it searches document using
	 * sequential scan. If you want to revert recent changes, use mark-revert
	 * couple to achieve this.
	 */
	void revert(Value doc);

	///Creates new document
	/** Function creates empty object and puts _id in it
	 *
	 * @return newly created document. It is already in edit mode, so you can set new atributes
	 */
	Document newDocument();

	///Creates new document
	/** Function creates empty object and puts _id in it
	 *
	 * @param suffix suffix append to the ID
	 * @return newly created document. It is already in edit mode, so you can set new atributes
	 */
	Document newDocument(ConstStrA suffix);



	CouchDB &getDatabase() {return db;}
	const CouchDB &getDatabase() const {return db;}


	const JSON::Builder json;
protected:

	JSON::Value docs;
	JSON::Container wholeRequest;
	CouchDB &db;

	void init();

	void eraseConflicts(ConstValue docId, ConstValue conflictList);

};



} /* namespace assetex */

#endif /* LIBS_LIGHTCOUCH_SRC_LIGHTCOUCH_CHANGESET_H_ */
