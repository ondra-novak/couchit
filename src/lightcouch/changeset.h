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


	///Updates document in the CouchDB
	/** Function can be used for ether simple updating, or insert or deleting the document.
	 * It depends on which attributes are included.
	 *
	 * If document has no _rev attribute, it will be inserted. However, the document must have
	 * the attribute "_id", otherwise an exception is thrown.
	 *
	 * If document has attribute _deleted:true, it will be deleted.
	 *
	 * In all cases expect creation, the document must have _rev. This attribute is presented
	 * at current version of document - you have to download the document before it can be
	 * updated.
	 *
	 * You can also use insert() to insert document to the database. You can also use erase()
	 * to erase document
	 *
	 * @param document document to update
	 *
	 * @note after updating, revision id of the document will be changed
	 */
	Changeset &update(JSON::Value document);

	///Erases document from database
	/**
	 * @param document document to erase
	 * @return reference to Changeset to allow create chains
	 */
	Changeset &erase(JSON::Value document);

	///Inserts new document to the database
	/**
	 * @param document document to insert
	 * @param id pointer to variable, which receives ID of document (optional)
	 * @return referenc to Changeset to allow create chains
	 */
	Changeset &insert(JSON::Value document, ConstStrA *id = 0);

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
	void revert(JValue doc);

	///Resolves conflict
	/**
	 * @param conflicts The Conflict object which containing details about the conflict. You
	 *  can obtain this object by calling the function CouchDB::loadConflicts
	 *
	 * @param mergedDocument result document created by combining of the all conflicts. You can
	 * however choose winning revision instead
	 * @param merge true if mergedDocument refers to new revision, which must be stored
	 * to the database. Specify false, when you just picked winner without modifying it. When
	 * merge is false, function just deletes other revision keeping the winning one
	 */
	Changeset &resolveConflict(const Conflicts &conflicts, JSON::Value mergedDocument, bool merge=true);

	struct ErrorItem {
		ConstStrA errorType;
		ConstStrA reason;
		JSON::Value document;
		JSON::ConstValue errorDetails;
	};

	///easy chain all changes with operator()
	Changeset &operator()(JSON::Value &v) {
		return update(v);
	}

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

	JSON::Value docs;
	JSON::Value wholeRequest;
	CouchDB &db;

	void init();

};



} /* namespace assetex */

#endif /* LIBS_LIGHTCOUCH_SRC_LIGHTCOUCH_CHANGESET_H_ */
