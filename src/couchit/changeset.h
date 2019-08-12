/*
 * changeSet.h
 *
 *  Created on: 11. 3. 2016
 *      Author: ondra
 */

#ifndef LIBS_LIGHTCOUCH_SRC_LIGHTCOUCH_CHANGESET_H_
#define LIBS_LIGHTCOUCH_SRC_LIGHTCOUCH_CHANGESET_H_
#include "couchDB.h"
#include "document.h"


namespace couchit {

class LocalView;

///Collects changes and commits them as one request
/** Helps to store multiple documents and resolve conflicts
 *
 * You can call the function update() to mark document for update. By calling commit(), all updated
 * documents are stored to the database
 *
 * @note You cannot update one document by multiple times
 *
 * @note if the document is set for update with _conflicts field, it automatically set all conflicts
 * for delete.
 *
 */
class Changeset {
public:
	Changeset();
	explicit Changeset(CouchDB &db);
	Changeset(const Changeset &db) = default;
	Changeset(Changeset &&db) = default;

	///Updates document in database
	/** Function schedules document for update. Once document is scheduled, Document instance can
	 * be destroyed, because document is kept inside of changeset. Changes are applied by function commit()
	 *
	 * @param document document to schedule for update. Do not schedule one document by multiple times.
	 *  It cannot replace previous schedule, and result is conflict during commit().
	 *  Also note, that function can manipulate with system fields. For instance, the timestamp
	 *  field is always updated to current timestamp. If the document contains the field _conflicts,
	 *  these conflicted revisions are scheduled for deletion.
	 *
	 * @return reference to this
	 *
	 *
	 */
	Changeset &update(const Document &document);

	///Performs mass update of query's Result.
	/**
	 * Each row must define a document, so it is good idea to execute query with include_docs=true
	 *
	 * @param results results
	 * @param updateFn function called for every result. The return value of the function is stored for update. If the
	 * return value is undefined or null, no document is updated
	 *
	 * @return this
	 */
	template<typename Fn>
	Changeset &update(const Result &results, Fn &&updateFn);

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
	Changeset &erase(const String &docId, const String &revId);


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
	Changeset &commit(CouchDB &db);


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
	Changeset &commit();


	///(deprecated) Retrieves revision of the committed document
	/** This function is deprecated and can be slow. To work with commited documents, use getCommited()

	 */
	String getCommitRev(const StrViewA &docId) const;

	///(deprecated) Retrieves revision of the committed document
	/** This function is deprecated and can be slow. To work with commited documents, use getCommited()

	 */
	String getCommitRev(const Document &doc) const;

	///(deprecated) Retrieves updated document by id
	/**
	 * @param docId id of document
	 * @return updated document. Document has updated "_rev", so it can be modified and updated again without re-downloading it.
	 *
	 * @note if document is not in changeset, returns undefined
	 *
	 * @note  This function is deprecated and can be slow. To work with commited documents, use getCommited()
	 *
	 *
	 *
	 */
	Value getUpdatedDoc(const StrViewA &docId) const;

	///Revets changes made in document docId
	/** Removes document from the changeset */
	void revert(const StrViewA &docId);

	///Marks current state of changeset
	/**
	 * @return a number that represents current mark. The mark can be used to revert future changes
	 */
	std::size_t mark() const;

	///Reverts all scheduled changes until the mark is reached.
	/**
	 * All changes scheduled after mark has been created will be reverted
	 * @param mark
	 */
	void revertToMark(std::size_t mark);

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


	CouchDB &getDatabase() {checkDB();return *db;}
	const CouchDB &getDatabase() const {checkDB();return *db;}
	bool hasDatabse() const {return db != nullptr;}




	typedef std::vector<Value> ScheduledDocs;


	struct CommitedDoc {
		//document id
		StrViewA id;
		//new revision
		String newRev;
		//original document
		Value doc;

		CommitedDoc(const StrViewA &id, const String &newRev, const Value &doc)
			:id(id),newRev(newRev),doc(doc) {}

		operator Document() const;
	};
	typedef std::vector<CommitedDoc> CommitedDocs;

	///Returns collection commited documents
	/**
	 * Deprecated feature - the collection is no longer ordered
	 * @return collection of commited docus
	 */
	const CommitedDocs &getCommitedDocs() const {return commitedDocs;}

	///Generates associative collection containing all commited documents
	/**
	 *
	 * @return JSON object where each key is ID of document,
	 * and the value is the whole updated document as it was stored in the
	 * DB, including updated _rev
	 *
	 * @note there can be automatic conflict resolver in the future version of the library. It
	 * is better to take whole document, not just the _rev field. The document
	 * can be merged with conflicted version, so more fields can be updated.
	 */
	Value getCommited() const;

	///moves changes from one container to other
	Changeset &operator=(Changeset &&other) {
		this->scheduledDocs = std::move(other.scheduledDocs);
		this->commitedDocs = std::move(other.commitedDocs);
		return *this;
	}

	std::size_t size() const {
		return scheduledDocs.size() + commitedDocs.size();
	}
	bool empty() const {
		return scheduledDocs.empty() && commitedDocs.empty();
	}


protected:

	ScheduledDocs scheduledDocs;
	CommitedDocs commitedDocs;

	CouchDB *db;

	void checkDB() const ;
};

template<typename Fn>
Changeset &Changeset::update(const Result &result, Fn &&updateFn) {
	for (Row rw : result) {
		Value doc = rw.doc;
		if (doc.defined()) {
			Value udoc = updateFn(doc);
			if (udoc.defined() && udoc != nullptr) {
				update(Document(udoc));
			}
		}
	}
	return *this;
}



} /* namespace assetex */

#endif /* LIBS_LIGHTCOUCH_SRC_LIGHTCOUCH_CHANGESET_H_ */
