/*
 * couchDB.h
 *
 *  Created on: 8. 3. 2016
 *      Author: ondra
 */

#ifndef ASSETEX_SRC_COUCHDB_H_BREDY_5205456032
#define ASSETEX_SRC_COUCHDB_H_BREDY_5205456032

#include <httpclient/httpClient.h>
#include <lightspeed/base/streams/netio.h>
#include <lightspeed/utils/json/json.h>

#include "uid.h"
#include "object.h"

#include "ichangeNotify.h"

#include "view.h"
#include "config.h"
#include "lightspeed/mt/fastlock.h"

#include "lightspeed/base/actions/message.h"

#include "attachment.h"
namespace LightSpeed {
class PoolAlloc;
}

namespace LightCouch {

using namespace LightSpeed;
using namespace BredyHttpClient;

class UIDIterator;
class Query;
class Changeset;
class QueryCache;
class Conflicts;
class Document;
class Validator;

///Client connection to CouchDB server
/** Each instance can keep only one connection at time. However, you can create
 * multiple instances, or use CouchDBPool to manage multiple connections to the database.
 *
 * Although CouchDB uses http/https protocol, keeping one connection can benefit from keep-alive
 * feature.
 *
 * The instance should be MT safe, however, you are limited to max one request at time. This also
 * includes listenChanges() feature which means that request can take a long time to process blocking
 * other threads to process other requests. Then you should consider to use extra instances
 * of CouchDB class.
 *
 */
class CouchDB {
public:

	///Contains name of key which holds time of document's last update
	/** Note that timestamping is optional feature supported only by LightCouch. If document
	 * is updated by other way, the timestamp don't need to be updated. Timestamping
	 * is supported by Changeset class
	 */
	static ConstStrA fldTimestamp;
	///Contains ID of previous revision
	/** Note this feature is optional and supported only by LightCouch. The class Changeset
	 * will put there id of previous revision.
	 */
	static ConstStrA fldPrevRevision;

	///Download flag - disable cache while retrieving resule
	/** Downloader will not include etag into header, so database will return complete response
	 * Result will not stored into cache. This flag is useful if you need
	 * to retrieve content which will be used only once. For example when
	 * url contains a random number.
	 */
	static const natural flgDisableCache = 1;
	///Download flag - refresh cache with new value
	/** Cache can quess, whether there is chance that content of URL has been updated. If
	 * cache doesn't detect such condition, no request is generated and result is
	 * returned from the cache. This flag disabled that feature. With this flag, request
	 * is always generated but eTags will be used. So cache can still be used when
	 * server returns state 304. If there is changed content, cache is also updated.
	 */
	static const natural flgRefreshCache = 2;
	///Download flag - store headers to the argument
	/** You have to supply empty object as header field (if you don't need to send
	 * headers with the request) Function will store response headers to the object.
	 */
	static const natural flgStoreHeaders = 4;

	///Retrieve all revision IDs
	static const natural flgRevisions = 8;
	///Retrieve revision info
	static const natural flgRevisionsInfo = 0x10;
	///Retrieve update seq
	static const natural flgSeqNumber = 0x40;
	///Retrieve attachments
	static const natural flgAttachments = 0x80;
	///Retrieve attachment encoding info
	static const natural flgAttEncodingInfo = 0x100;
	///Retrieve all conflicts
	static const natural flgConflicts = 0x200;
	///Retrieve all deleted conflicts
	static const natural flgDeletedConflicts = 0x400;
	CouchDB(const Config &cfg);
	~CouchDB();


	///Perform GET request from the database
	/** GET request can be cached or complete returned from the cache
	 *
	 * @param path absolute or relative path to the database. Absolute path must start with a slash '/'
	 * @param headers optional argument, headers sent with the request as key-value structure.
	 *    if flag storeHeaders is set, function will store response's headers into
	 *    object referenced by the argument. If object already contains a data,
	 *    they will be deleted.
	 * @param flags various flags that controls caching or behaviour
	 * @return parsed response
	 */
	JSON::ConstValue requestGET(ConstStrA path, JSON::Value headers = null, natural flags = 0);
	///Performs POST request from the database
	/** POST request are not cached.
	 *
	 * @param path absolute or relative path to the database. Absolute path must start with a slash '/'
	 * @param postData JSON data to send to the server
	 * @param headers optional argument, headers sent with the request as key-value structure.
	 *    if flag storeHeaders is set, function will store response's headers into
	 *    object referenced by the argument. If object already contains a data,
	 *    they will be deleted.
	 * @param flags various flags that controls behaviour
	 * @return parsed response
	 */
	JSON::ConstValue requestPOST(ConstStrA path, JSON::ConstValue postData, JSON::Container headers = null, natural flags = 0);
	///Performs PUT request from the database
	/** PUT request are not cached.
	 *
	 * @param path absolute or relative path to the database. Absolute path must start with a slash '/'
	 * @param postData JSON data to send to the server
	 * @param headers optional argument, headers sent with the request as key-value structure.
	 *    if flag storeHeaders is set, function will store response's headers into
	 *    object referenced by the argument. If object already contains a data,
	 *    they will be deleted.
	 * @param flags various flags that controls behaviour
	 * @return parsed response
	 */
	JSON::ConstValue requestPUT(ConstStrA path, JSON::ConstValue postData, JSON::Container headers = null, natural flags = 0);

	///Performs DELETE request at database
	/**
	 *
	 * @param path absolute path to the resource to delete
	 * @param headers aditional headers
	 * @param flags flags that controls behaviour
	 * @return
	 */
	JSON::ConstValue requestDELETE(ConstStrA path, JSON::Value headers = null, natural flags = 0);




	ConstStrA

	///Generates new unique ID
	/** Unique ID is generated as series numbers and letters. It consists from timestamp part,
	 * counter part and random part. The random is choosen when application starts. The counter
	 * part counts number of objects created from the start of the application. If the application
	 * is restarted, counter is reset, however, the application generates different random part, so
	 * newly generated UID will be truly unique. The timestamp parts allows to order objects
	 * by time of creation.
	 * @return UUID object which can be converted to ConstStrA
	 */
	UID getUID();
	///Generates new unique ID
	/** Unique ID is generated as series numbers and letters. It consists from timestamp part,
	 * counter part and random part. The random is choosen when application starts. The counter
	 * part counts number of objects created from the start of the application. If the application
	 * is restarted, counter is reset, however, the application generates different random part, so
	 * newly generated UID will be truly unique. The timestamp parts allows to order objects
	 * by time of creation.
	 * @return Value object which can be put directly to the document
	 */
	Value getUIDValue();

	///Generates new unique ID
	/** Unique ID is generated as series numbers and letters. It consists from timestamp part,
	 * counter part and random part. The random is choosen when application starts. The counter
	 * part counts number of objects created from the start of the application. If the application
	 * is restarted, counter is reset, however, the application generates different random part, so
	 * newly generated UID will be truly unique. The timestamp parts allows to order objects
	 * by time of creation.
	 *
	 * @param suffix - user defined suffix - it can be used to identify type of the document. If
	 * you need a separator between the UID and suffix, you have to specify it with the suffix.
	 * For example ".invoice" will add ".invoice" to the UID, so any filter can take advantage
	 * from knowledge, that document is invoice.
	 *
	 * @return UUID object which can be converted to ConstStrA
	 */
	UID getUID(ConstStrA prefix);
	///Generates new unique ID
	/** Unique ID is generated as series numbers and letters. It consists from timestamp part,
	 * counter part and random part. The random is choosen when application starts. The counter
	 * part counts number of objects created from the start of the application. If the application
	 * is restarted, counter is reset, however, the application generates different random part, so
	 * newly generated UID will be truly unique. The timestamp parts allows to order objects
	 * by time of creation.
	 *
	 * @param suffix - user defined suffix - it can be used to identify type of the document. If
	 * you need a separator between the UID and suffix, you have to specify it with the suffix.
	 * For example ".invoice" will add ".invoice" to the UID, so any filter can take advantage
	 * from knowledge, that document is invoice.
	 *
	 * @return Value object which can be put directly to the document
	 **/
	Value getUIDValue(ConstStrA suffix);


	///Changes current database
	void use(ConstStrA database);
	///Retrieves current database name
	ConstStrA getCurrentDB() const;

	///Performs URL encode of the argument
	static StringA urlencode(ConstStrA text);

	///creates database
	/** Creates database. Database is specified by function use()*/
	void createDatabase();

	///Deletes database
	/** Deletes current database. Database is specified by function use */
	void deleteDatabase();


	enum ListenMode {

		lmForever,
		lmOneShot,
		lmNoWait

	};


	///Starts listening for changes processed by a filter
	/** Function block while it process all changes for indefinite time. Every
	 * change is passed to the callback. During this process, this object cannot process
	 * any other requests, expect time, when callback is running. It is also
	 * allowed to work with this connection inside the handler.
	 *
	 * Calling any function expect stopListenChanges() will block until change is received
	 *
	 * There are two ways, how to stop listening
	 *  1. handler itself can return false and cause stop listening
	 *  2. calling stopListenChanges from the other thread interrupts listening.
	 *
	 *  Note that object always finish current change before it leaves listening cycle
	 *
	 * @param cb callback object which will receive changes
	 * @param fromSeq start with give sequence number
	 * @param filter filter definition
	 * @param lm behaviour of the function.
	 * @return returns last processed sequence number. You can use number to resume listening by
	 * simple passing this number as the argument fromSeq
	 */
	template<typename Fn>
	natural listenChanges(natural fromSeq, const Filter &filter, ListenMode lm, const Fn &cb);

	///Starts listening without filtering
	/** Function block while it process all changes for indefinite time. Every
	 * change is passed to the callback. During this process, this object cannot process
	 * any other requests, expect time, when callback is running. It is also
	 * allowed to work with this connection inside the handler.
	 *
	 * Calling any function expect stopListenChanges() will block until change is received
	 *
	 * There are two ways, how to stop listening
	 *  1. handler itself can return false and cause stop listening
	 *  2. calling stopListenChanges from the other thread interrupts listening.
	 *
	 *  Note that object always finish current change before it leaves listening cycle
	 *
	 * @param cb callback function bool operator()(ChangeDoc doc)
	 * @param fromSeq start with given sequence number
	 * @param filterFlags combination of flags defined for Filter / View
	 * @param lm behaviour of the function.
	 * @return returns last processed sequence number. You can use number to resume listening by
	 * simple passing this number as the argument fromSeq
	 */
	template<typename Fn>
	natural listenChanges(natural fromSeq, natural filterFlags, ListenMode lm, const Fn &cb);




	///Stops listening changes
	/** Function just requests current listening to finish as soon as possible. It doesn't
	 * wait for real finish. You have to use proper multi-thread comunication to
	 * achieve this. Function can be also called from the handler. It doesn't matter
	 * from which thread it is called
	 */
	void stopListenChanges();

	///Determines last sequence number
	/** Function requests server to retrieve last sequence number. You will need
	 * this to start listening for changes. It is recomended to determine last sequence
	 * number before application starts to download current state to receive
	 * changes made during synchronization
	 * @return last sequence number
	 */
	natural getLastSeqNumber();

	///Creates query for specified view
	/**
	 * @param view a view definition
	 * @return
	 */
	Query createQuery(const View &view);

	///Creates query object for retrieve documents
	/**
	 * @param viewFlags flags defined in View.
	 * @return
	 */
	Query createQuery(natural viewFlags);

	///Creates changeset
	/** Changeset will use this database connection to update document
	 *
	 * @return
	 */
	Changeset createChangeset();



	///Enables tracking of sequence numbers for caching
	/**
	 * Function directly call QueryCache::trackSeqNumbers with correct database. It enables
	 * tracking sequence numbers for caching. You have to update number everytime database is changed. This
	 * can be achieved by installing dispatcher through listenChanges(). When listenChanges is active,
	 * sequence number is updated automatically (but be careful with filters, you need to install at least
	 * one dispatcher without filtering)
	 *
	 * @return reference to variable which should be updated with every change in the database. It will be initialized
	 * using function getLastSeqNumber()
	 *
	 * @exception - Function throws exception if there is no current database or when caching is not enabled
	 */
	atomicValue &trackSeqNumbers();


	///Retrieves local document (by its id)
	/** You can use function to retrieve local document, because Query object will not retrieve it. To store
	 * document, you can use ChangeSet object. Note that mixing writting standard documents and local documents
	 * in single ChangeSet can cause undefined behaviour when local document is in conflict.
	 * @param localId
	 * @param flags can contain 0 or flgDisableCache, other flags are ignored
	 * @return JSON document;
	 *
	 * @note if sequence numbers are tracked, function disables caching, because
	 * sequence numbers are not updated when local document is stored
	 */
	ConstValue retrieveLocalDocument(ConstStrA localId, natural flags = 0);

	///Retrieves document (by its id)
	/**
	 *
	 * @param docId document id
	 * @param flags various flags defined in CouchDB::flgXXXX. The flag flgStoreHeaders is ignored
	 * @return json with document
	 *
	 * @note Retrieveing many documents using this method is slow. You should use Query
	 * to retrieve multiple documents. However, some document properties are not available through the Query
	 */
	ConstValue retrieveDocument(ConstStrA docId, natural flags = 0);


	///Retrieves other revision of the document
	/**
	 * @param docId document id
	 * @param revId revision id
	 * @param flags can be only flgDisableCache or zero. The default value is recommended.
	 * @return json with document
	 */
	ConstValue retrieveDocument(ConstStrA docId, ConstStrA revId, natural flags = flgDisableCache);

	///Creates new document
	/**
	 * Function creates new object and puts _id in it. Generates new id
	 * @return Value which can be converted to Document object
	 */
	Value newDocument();

	///Creates new document
	/**
	 * Function creates new object and puts _id in it. Generates new id
	 * @param prefix prefix append to UID - can be used to specify type of the document
	 * @return Value which can be converted to Document object
	 */
	Value newDocument(ConstStrA prefix);

	///Retrieves pointer to validator
	/**
	 * @return function returns null, when validator is not defined. Otherwise function
	 * returns pointer to current validator
	 */
	Pointer<Validator> getValidator() const {return validator;}

	class UpdateResult: public ConstValue {
	public:
		UpdateResult(ConstValue v, StringA newRev):ConstValue(v),newRev(newRev) {}

		const StringA newRev;
	};

	///Calls update handler to update document using server-side function
	/** Server side function must be defined in design documents. You have to know relative path to
	 * the database root.
	 *
	 * @param updateHandlerPath relative path to the update handler
	 * @param documentId document id to update. If you need to create new document, you have to supply newly
	 * generated ID. LightCouch doesn't support POST requests without document id.
	 * @param arguments optional arguments (can be NULL)passed as object which is transformed to the key=value structure. Arguments are converted to strings (without quotes). Objects and arrays are serialized
	 * @return Response of the function. Response has following format
	 *
	 * @code
	 * {
	 *    "content":"...",
	 *    "content-type":"...",
	 *    "rev":"...",
	 *    "id":"..."
	 * }
	 *
	 * @b content - string or binary or parsed json value if possible.
	 * @b content-type - content type. If you need to parse response as json, specify application/json here.
	 * @b rev - revision id of the update.
	 * @b id - id of the document
	 *
	 *
	 * @note contenr can be binary string. It is not valid in json, however, in C++ it is simply
	 *  a string, which can contain a binary information
	 *
	 *
	 *  @exception RequestError if function returns any other status then 200 or 201
	 */
	UpdateResult updateDoc(ConstStrA updateHandlerPath, ConstStrA documentId, JSON::ConstValue arguments);


	///Calls show handler.
	/** Calls special show handler for specified document. The document doesn't need to exists, handler
	 * must be able to handle such situation
	 *
	 * @param showHandlerPath relative to database's root path to the show handler
	 * @param documentId document id
	 * @param arguments optional arguments (can be NULL) passed as object which is transformed to the key=value structure. Arguments are converted to strings (without quotes). Objects and arrays are serialized
	 * @param flags allowed flags flgDisableCache or flgRefreshCache or zero. If cache available caching is in effect by default.
	 * @return Response of the function. Response must be application/json, otherwise following object is returned
	 *
	 * @code
	 * {
	 *    "content":"...",
	 *    "content-type":"...",
	 *    "id":"..."
	 * }
	 *
	 * @b content - string or binary or parsed json value if possible.
	 * @b content-type - content type. If you need to parse response as json, specify application/json here.
1	 * @b id - id of the document
	 *
	 *
	 * @note contenr can be binary string. It is not valid in json, however, in C++ it is simply
	 *  a string, which can contain a binary information
	 *
	 *
	 * @exception RequestError if function returns any other status then 200 or 201
	 */
	ConstValue showDoc(ConstStrA showHandlerPath, ConstStrA documentId, JSON::ConstValue arguments, natural flags = 0);


	class DownloadFile: public SeqFileInput {
	public:
		DownloadFile(const SeqFileInput &stream, ConstStrA contentType, natural contentLength):SeqFileInput(stream)
				,contentType(contentType),contentLength(contentLength) {}
		const ConstStrA contentType;
		natural contentLength;
	};

	typedef Message<void, SeqFileOutput> UploadFn;
	typedef Message<void, DownloadFile> DownloadFn;

	///Uploads attachment with specified document
	/**
	 * @param document document object. The document don't need to be complete, only _id and _rev must be there.
	 * @param attachmentName name of attachment
	 * @param contentType content type of the attachment
	 * @param fn function which will be called to supply attachment body. Function accepts one argument
	 * SeqFileOuput.
	 * @return Funtcion returns new revision of the document, if successful.
	 */
	StringA uploadAttachment(Document &document, ConstStrA attachmentName, ConstStrA contentType, const UploadFn &uploadFn);

	///Uploads attachment with specified document
	/**
	 * Function allows to upload attachment stored complete in the memory.
	 *
	 * @param document document object. The document don't need to be complete, only _id and _rev must be there.
	 * @param attachmentName name of attachment
	 * @param attachmentData content of attachment
	 * @return Funtcion returns new revision of the document, if successful.
	 */
	StringA uploadAttachment(Document &document, ConstStrA attachmentName, const AttachmentDataRef &attachmentData);

	///Downloads attachment
	/**
	 * @param document document. The document don't need to be complete, only _id and _rev must be there.
	 * @param attachmentName name of attachment to retrieve
	 * @param downloadFn function, which accepts DownloadFile where you can
	 */
	void downloadAttachment(Document &document, ConstStrA attachmentName, const DownloadFn &downloadFn);

	///Downloads attachment
	/**
	 * Function allows to download attachment to the memory. Large attachments may cost lot of memory.
	 *
	 * @param document  document. The document don't need to be complete, only _id and _rev must be there.
	 * @param attachmentName name of attachment to retrieve
	 * @return content of attachment
	 */
	AttachmentData downloadAttachment(Document &document, ConstStrA attachmentName);


	///Use json variable to build objects
	const Json json;




	struct HttpConfig: BredyHttpClient::ClientConfig {
		HttpConfig(const Config &cfg);
	};


	static StringA generateServerID();


protected:

	mutable FastLock lock;




	friend class ChangeFeed;

	StringA baseUrl;
	StringA database;
	StringA serverid;
	JSON::PFactory factory;
	natural lastStatus;
	bool listenExitFlag;
	Pointer<QueryCache> cache;
	Pointer<Validator> validator;
	atomicValue *seqNumSlot;

	StringA lastConnectError;

	HttpConfig httpConfig;
	HttpClient http;



	natural listenChangesInternal(IChangeNotify &cb,  natural fromSeq, const Filter &filter, ListenMode lm);

	template<typename C>
	void reqPathToFullPath(ConstStrA reqPath, C &output);

	template<typename C>
	void urlEncodeToStream(ConstStrA str, C &output);


	JSON::ConstValue jsonPUTPOST(HttpClient::Method method, ConstStrA path, JSON::ConstValue postData, JSON::Container headers, natural flags);
public:


};

template<typename Fn>
natural CouchDB::listenChanges(natural fromSeq, const Filter &filter, ListenMode lm, const Fn &cb) {

	class CB: public IChangeNotify {
	public:
		Fn cb;
		CB(const Fn &cb):cb(cb) {}
		virtual bool onChange(const ChangedDoc &changeInfo) throw() {
			return cb(changeInfo);
		}
	};
	CB cbc(cb);
	return listenChangesInternal(cbc,fromSeq,filter,lm);
}

template<typename Fn>
natural CouchDB::listenChanges(natural fromSeq, natural filterFlags, ListenMode lm, const Fn &cb) {
	Filter f(StringA(), filterFlags);
	return listenChanges(fromSeq, f, lm, cb);
}



} /* namespace assetex */

#endif /* ASSETEX_SRC_COUCHDB_H_BREDY_5205456032 */
