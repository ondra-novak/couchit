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
#include "uid.h"
#include "couchDB.h"


namespace LightCouch {


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
	 * @note Document should have ID otherwise function fails. To insert new document, use insert(). You
	 * can also specify ID manually and then call update()
	 *
	 */
	Changeset &update(Document &document);

	///Erases document from database
	/**
	 * @param document document to erase. Note document will switched to editable, because deleting
	 * is implemented by creating new special revision
	 *
	 * @return reference to Changeset to allow create chains
	 */
	Changeset &erase(Document &document);

	///Inserts new document to the database
	/**
	 * @param document document to insert. Note that document is always inserted as new. New ID will
	 * be always generated. You can retrieve the ID by method getID();
	 *
	 * @return reference to Changeset to allow create chains
	 */
	Changeset &insert(Document &document);

	///Inserts new document to the database
	/**
	 * @param id document id
	 * @param document document to insert.
	 *
	 * @return reference to Changeset to allow create chains
	 */
	Changeset &insert(ConstStrA id, Document &document);

	///Commints all changes in the database
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


	Changeset &commit(bool all_or_nothing=true);


	///Mark current state of changeset
	/** you can use stored mark to revert unsaved changes later */
	natural mark() const;

	///Revert changes up to mark
	/** Function removes all document stored beyond that mark
	 *
	 * @param mark mark stored by mark()
	 *
	 * @note function only removes document from the change-set. It cannot
	 * revert changes made in the document
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

	///Resolves conflict
	/**
	 * @param conflicts The Conflict object which containing details about the conflict. You
	 *  can obtain this object by calling the function CouchDB::loadConflicts
	 *
	 * @param mergedDocument result document created by combining of the all conflicts.
	 */
	Changeset &resolveConflict(const Conflicts &conflicts, Document &mergedDocument);

	struct ErrorItem {
		ConstStrA errorType;
		ConstStrA reason;
		JSON::ConstValue document;
		JSON::ConstValue errorDetails;
	};

	CouchDB &getDatabase() {return db;}
	const CouchDB &getDatabase() const {return db;}

	class UpdateException: public Exception{
	public:
		LIGHTSPEED_EXCEPTIONFINAL;
		UpdateException(const ProgramLocation &loc, const StringCore<ErrorItem> &errors);
		ConstStringT<ErrorItem> getErrors() const;

	protected:
		StringCore<ErrorItem> errors;

		void message(ExceptionMsg &msg) const;
	};

	const JSON::Builder json;
protected:

	JSON::Container docs;
	JSON::Container wholeRequest;
	CouchDB &db;

	void init();

};



} /* namespace assetex */

#endif /* LIBS_LIGHTCOUCH_SRC_LIGHTCOUCH_CHANGESET_H_ */
