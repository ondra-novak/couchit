#pragma once

#include <functional>
#include <mutex>

#include "json.h"
#include "attachment.h"
#include "iqueryable.h"
#include "revision.h"


namespace couchit {

class ChangesFeed;
class ChangesFeedHandler;
class Changes;
class IIDGen;
class Document;
class Query;
class Changeset;
class Upload;
class Validator;

///Base class for all document-base databases.
/** The CouchDB class implements this class. However there
 * can be other document-base database providers. Some
 * other classes can use this base class to access services
 * provided by the database provider
 */
class DocumentDB {
public:


	///Contains name of key which holds time of document's last update
	/** Note that timestamping is optional feature supported only by couchit. If document
	 * is updated by other way, the timestamp don't need to be updated. Timestamping
	 * is supported by Changeset class
	 */
	static StrViewA fldTimestamp;
	///Contains ID of previous revision
	/** Note this feature is optional and supported only by couchit. The class Changeset
	 * will put there id of previous revision.
	 */
	static StrViewA fldPrevRevision;


	typedef std::size_t Flags;


	///Download flag - disable cache while retrieving resule
	/** Downloader will not include etag into header, so database will return complete response
	 * Result will not stored into cache. This flag is useful if you need
	 * to retrieve content which will be used only once. For example when
	 * url contains a random number.
	 */
	static const Flags flgDisableCache = 1;
	///Download flag - refresh cache with new value
	/** Cache can quess, whether there is chance that content of URL has been updated. If
	 * cache doesn't detect such condition, no request is generated and result is
	 * returned from the cache. This flag disables that feature. With this flag, request
	 * is always generated but eTags will be used. So cache can still be used when
	 * server returns state 304. If there is changed content, cache is also updated.
	 */
	static const Flags flgRefreshCache = 2;
	///Download flag - store headers to the argument
	/** You have to supply empty object as header field (if you don't need to send
	 * headers with the request) Function will store response headers to the object.
	 */
	static const Flags flgStoreHeaders = 4;


	///Retrieve all revision IDs
	static const Flags flgRevisions = 8;
	///Retrieve revision info
	static const Flags flgRevisionsInfo = 0x10;
	///Retrieve update seq
	static const Flags flgSeqNumber = 0x40;
	///Retrieve attachments
	static const Flags flgAttachments = 0x80;
	///Retrieve attachment encoding info
	static const Flags flgAttEncodingInfo = 0x100;
	///Retrieve all conflicts
	static const Flags flgConflicts = 0x200;
	///Retrieve all deleted conflicts
	static const Flags flgDeletedConflicts = 0x400;
	///create new document when requesting document doesn't exists
	static const Flags flgCreateNew = 0x1000;
	///do not use authentification (token is not used)
	static const Flags flgNoAuth = 0x2000;
	///use syncQueryTimeout instead normal timeout
	static const Flags flgLongOperation = 0x4000;
	///disable exception if document is missing (for function get()), instead null is returned
	static const Flags flgNullIfMissing = 0x8000;


	virtual Changes receiveChanges(ChangesFeed &sink) = 0;
	virtual void receiveChangesContinuous(ChangesFeed &sink, ChangesFeedHandler &fn) = 0;

	///Generates new UID and returns it as Value. It can be directly used to create new document.
	/**
	 * @return UID stored as Value object
	 *
	 * @note In contrast to the function getUID(), this function can be called
	 * by multiple threads without loosing return value. However you should avoid to mix getUID and getUIDValue
	 * in MT environment or use additional synchronization
	 */
	Value genUIDValue() const;
	///Generates new UID and returns it as Value. It can be directly used to create new document.
	/**
	 * @param prefix user defined prefix which is put at the beginning of the ID
	 * @return UID stored as Value object
	 *
	 * @note In contrast to the function getUID(), this function can be called
	 * by multiple threads without loosing return value. However you should avoid to mix getUID and getUIDValue
	 * in MT environment or use additional synchronization
	 */
	Value genUIDValue(StrViewA prefix) const;


	///Generates new UID
	/** Function is alias to genUIDValue() - for compatibility reasons */
	Value genUID() const {return genUIDValue();}

	///Generates new UID
	/** Function is alias to genUIDValue() - for compatibility reasons */
	Value genUID(StrViewA prefix) const {return genUIDValue(prefix);}

	///Creates new document
	/**
	 * Function creates new object and puts _id in it. Generates new id
	 * @return Value which can be converted to Document object
	 */
	Document newDocument();

	///Creates new document
	/**
	 * Function creates new object and puts _id in it. Generates new id
	 * @param prefix prefix append to UID - can be used to specify type of the document
	 * @return Value which can be converted to Document object
	 */
	Document newDocument(const StrViewA &prefix);


	///Bulk upload
	virtual Value bulkUpload(const Value docs) = 0;

	virtual SeqNumber getLastSeqNumber() = 0;

	virtual SeqNumber getLastKnownSeqNumber() const = 0;

	virtual Query createQuery(const View &view) = 0;

	virtual Query createQuery(Flags viewFlags) = 0;

	virtual Changeset createChangeset() = 0;

	virtual ChangesFeed createChangesFeed() = 0;

	virtual Value getLocal(const StrViewA &localId, Flags flags = 0) = 0;

	virtual Value get(const StrViewA &docId, Flags flags = 0) = 0;

	virtual Value get(const StrViewA &docId, const StrViewA &revId, Flags flags = flgDisableCache) = 0;

	virtual Upload putAttachment(const Value &document, const StrViewA &attachmentName, const StrViewA &contentType) = 0;


	///Uploads attachment with specified document
	/**
	 * Function allows to upload attachment stored complete in the memory.
	 *
	 * @param document document object. The document don't need to be complete, only _id and _rev must be there.
	 * @param attachmentName name of attachment
	 * @param attachmentData content of attachment
	 * @return Funtcion returns new revision of the document, if successful.
	 */
	String putAttachment(const Value &document, const StrViewA &attachmentName, const AttachmentDataRef &attachmentData);

	///Downloads attachment
	/**
	 * @param document document. The document don't need to be complete, only _id and _rev must be there.
	 * @param attachmentName name of attachment to retrieve
	 * @param etag if not empty, function puts string as "if-none-match" header.
	 */
	virtual Download getAttachment(const Document &document, const StrViewA &attachmentName,  const StrViewA &etag=StrViewA()) = 0;
	///Downloads latest attachments
	/** Allows to easily download the latest attachment by given docId an dattachmentName
	 *
	 * @param docId document Id
	 * @param attachmentName attachment name
	 * @param etag (optional) etag, if not empty, then it is put to the header and result can be notModified
	 * @return download object
	 */
	virtual Download getAttachment(const StrViewA &docId, const StrViewA &attachmentName,  const StrViewA &etag=StrViewA()) = 0;
	///Updates single document
	/**
	 * @param doc document to update. It must have the fields "_id" and "_rev" set. The function updates the field "_rev" with new revisionId.
	 * @exception UpdateException Update is not possible (exception contains one item)
	 * @exception RequestException Other error
	 */
	virtual void put(Document &doc) = 0;

	///Retrieves pointer to validator
	/**
	 * @return function returns null, when validator is not defined. Otherwise function
	 * returns pointer to current validator
	 */
	virtual Validator *getValidator() const = 0;

	virtual IIDGen &getIDGenerator() const = 0;
protected:
	DocumentDB ();
	DocumentDB (const DocumentDB &other);
	virtual ~DocumentDB() {}

	mutable std::mutex lock;
	typedef std::lock_guard<std::mutex> LockGuard;
	mutable std::vector<char> uidBuffer;

	StrViewA lkGenUID() const;
	StrViewA lkGenUID(StrViewA prefix) const;
};


}
