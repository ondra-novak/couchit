/*
 * couchDB.h
 *
 *  Created on: 8. 3. 2016
 *      Author: ondra
 */

#ifndef ASSETEX_SRC_COUCHDB_H_BREDY_5205456032
#define ASSETEX_SRC_COUCHDB_H_BREDY_5205456032

#include <functional>
#include <httpclient/httpClient.h>
#include <lightspeed/base/streams/netio.h>

#include "json.h"


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
class Changes;
class ChangesSink;

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
	static StringRef fldTimestamp;
	///Contains ID of previous revision
	/** Note this feature is optional and supported only by LightCouch. The class Changeset
	 * will put there id of previous revision.
	 */
	static StringRef fldPrevRevision;

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

	static const natural flgTryAgainCounterMask = 0xF800;
	static const natural flgTryAgainCounterStep = 0x0800;

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
	Value requestGET(const StringRef &path, Value *headers = nullptr, natural flags = 0);
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
	Value requestPOST(const StringRef& path, const Value &postData, Value *headers = nullptr, natural flags = 0);
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
	Value requestPUT(const StringRef &path, const Value &postData, Value *headers = nullptr, natural flags = 0);

	///Performs DELETE request at database
	/**
	 *
	 * @param path absolute path to the resource to delete
	 * @param headers aditional headers
	 * @param flags flags that controls behaviour
	 * @return
	 */
	Value requestDELETE(const StringRef &path, Value *headers = nullptr, natural flags = 0);



	///Generates new UID using preconfigured generator
	/** See Config how to setup custom generator
	 *
	 * @return string reference to UID. Note that reference is valid until method is called again. You
	 * should immediatelly create a copy. Function itself is MT safe, however the reference can be
	 * lost in time of return
	 *
	 */
	StringRef genUID();

	///Generates new UID using preconfigured generator
	/**See Config how to setup custom generator
	 *
	 * @param prefix user defined prefix which is put at the beginning of the ID
	 * @return string reference to UID. Note that reference is valid until method is called again. You
	 * should immediatelly create a copy. Function itself is MT safe, however the reference can be
	 * lost in time of return
	 */
	StringRef genUID(StringRef prefix);

	///Generates new UID and returns it as Value. It can be directly used to create new document.
	/**
	 * @return UID stored as Value object
	 *
	 * @note In contrast to the function getUID(), this function can be called
	 * by multiple threads without loosing return value. However you should avoid to mix getUID and getUIDValue
	 * in MT environment or use additional synchronization
	 */
	Value genUIDValue();
	///Generates new UID and returns it as Value. It can be directly used to create new document.
	/**
	 * @param prefix user defined prefix which is put at the beginning of the ID
	 * @return UID stored as Value object
	 *
	 * @note In contrast to the function getUID(), this function can be called
	 * by multiple threads without loosing return value. However you should avoid to mix getUID and getUIDValue
	 * in MT environment or use additional synchronization
	 */
	Value genUIDValue(StringRef prefix);


	///Changes current database
	void use(String database);
	///Retrieves current database name
	String getCurrentDB() const;

	///creates database
	/** Creates database. Database is specified by function use()*/
	void createDatabase();

	///Deletes database
	/** Deletes current database. Database is specified by function use */
	void deleteDatabase();



	///Determines last sequence number
	/** Function requests server to retrieve last sequence number. You will need
	 * this to start listening for changes. It is recomended to determine last sequence
	 * number before application starts to download current state to receive
	 * changes made during synchronization
	 * @return last sequence number
	 */
	Value getLastSeqNumber();

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


	///Creates object which is used to receive database changes
	ChangesSink createChangesSink();





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
	Value retrieveLocalDocument(const StringRef &localId, natural flags = 0);

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
	Value retrieveDocument(const StringRef &docId, natural flags = 0);


	///Retrieves other revision of the document
	/**
	 * @param docId document id
	 * @param revId revision id
	 * @param flags can be only flgDisableCache or zero. The default value is recommended.
	 * @return json with document
	 */
	Value retrieveDocument(const StringRef &docId, const StringRef &revId, natural flags = flgDisableCache);

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
	Value newDocument(const StringRef &prefix);

	///Creates empty document with specified ID
	/**
	 * @param id id of document
	 * @return empty document as Value, you can assign result to an Document instance
	 *
	 * @note You can use empty document to create new document with predefined ID.
	 *
	 */
	Value emptyDocument(const StringRef &id);



	///Retrieves pointer to validator
	/**
	 * @return function returns null, when validator is not defined. Otherwise function
	 * returns pointer to current validator
	 */
	Pointer<Validator> getValidator() const {return validator;}

	class UpdateResult: public Value {
	public:
		UpdateResult(Value v, const String &newRev):Value(v),newRev(newRev) {}

		const String newRev;
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
	 * @endcode
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
	UpdateResult updateDoc(StringRef updateHandlerPath, StringRef documentId, Value arguments);


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
	 * @endcode
	 *
	 * @b content - string or binary or parsed json value if possible.
	 * @b content-type - content type. If you need to parse response as json, specify application/json here.
	 * @b id - id of the document
	 *
	 *
	 * @note contenr can be binary string. It is not valid in json, however, in C++ it is simply
	 *  a string, which can contain a binary information
	 *
	 *
	 * @exception RequestError if function returns any other status then 200 or 201
	 */
	Value showDoc(const StringRef &showHandlerPath, const StringRef &documentId, const Value &arguments, natural flags = 0);


	///Contains downloaded file
	class Download {
	public:


		class Source: public RefCntObj {
		public:
			virtual ~Source() {}
			virtual std::size_t operator()(unsigned char *buffer, std::size_t size) = 0;
		};



		///ETag of the attachment
		const String etag;
		///Content type of the attachment
		const String contentType;
		///Content length of the attachment
		const std::size_t contentLength;
		///if true, then document has not been modified
		/** In this case, source is empty and contentLength is zero */
		bool notModified;

		///Contains data of the attachment
		/** You have to read data as fast as possible, otherwise the instance of the CouchDB is blocked.
		 * Slow reading can also trigger timeout error on the server side.
		 */
		Source &source;

		Download(Source *src,
				const String etag,
				const String contentType,
				const std::size_t contentLength,
				bool notModified);

		private:

		RefCntPtr<Source> srcptr;

	};

	///Function called to collect data for upload
	/**
	 * @param first_arg pointer to buffer allocated for data. Function must but data there.
	 * @param second_arg size of the buffer
	 * @return count of written bytes to the buffer. Function should write at least one byte. To
	 * write EOF, the function must return 0
	 */
	typedef std::function<std::size_t(unsigned char *, std::size_t)> UploadFn;

	///Uploads attachment with specified document
	/**
	 * @param document document object. The document don't need to be complete, only _id and _rev must be there.
	 * @param attachmentName name of attachment
	 * @param contentType content type of the attachment
	 * @param fn function which will be called to supply attachment body. Function accepts one argument
	 * SeqFileOuput.
	 * @return Funtcion returns new revision of the document, if successful.
	 */
	String uploadAttachment(const Value &document, const StringRef &attachmentName, const StringRef &contentType, const UploadFn &uploadFn);

	///Uploads attachment with specified document
	/**
	 * Function allows to upload attachment stored complete in the memory.
	 *
	 * @param document document object. The document don't need to be complete, only _id and _rev must be there.
	 * @param attachmentName name of attachment
	 * @param attachmentData content of attachment
	 * @return Funtcion returns new revision of the document, if successful.
	 */
	String uploadAttachment(const Value &document, const StringRef &attachmentName, const AttachmentDataRef &attachmentData);

	///Downloads attachment
	/**
	 * @param document document. The document don't need to be complete, only _id and _rev must be there.
	 * @param attachmentName name of attachment to retrieve
	 * @param etag if not empty, function puts string as "if-none-match" header.
	 *
	 *
	 */
	Download downloadAttachment(const Value &document, const StringRef &attachmentName,  StringRef etag=StringRef());


	///For function updateDesignDocument
	enum DesignDocUpdateRule {
		///if design document exists, skip upload
		ddurSkipExisting,
		///if design documenr exists, check whether it is the same, and throw exception when it is different
		ddurCheck,
		///Always overwrite design document
		ddurOverwrite,
	/*	///Merges documents leaving skipping existing parts
		ddurMergeSkip,
		///Merges documents overwritting existing parts
		ddurMergeOverwrite,*/
	};


	///Uploads design document to the server
	/**
	 * @note the connection needs administration permissions to do that!
	 *
	 *
	 * @param content content of design document as pure JSON
	 * @param updateRule specifies rule how to handle existing design document
	 * @retval true design document has been updated
	 * @retval false design document unchanged
	 * will accept both variants with or without. If parameter is empty, function picks name from
	 * the document (under _id key)
	 * @exception UpdateException document cannot be updated because it already exists and it is different
	 */
	bool uploadDesignDocument(const Value &content, DesignDocUpdateRule updateRule = ddurOverwrite);

	///Uploads design document from the file
	/**
	 * @note the connection needs administration permissions to do that!
	 *
	 *
	 * @param pathcontent name of file that contains design document. Document must be valid JSON
	 * @param updateRule specifies rule how to handle existing design document
	 * @param name name of the design document. It don't need to have "_design/" prefix, however, function
	 * will accept both variants with or without.If parameter is empty, function picks name from
	 * the document (under _id key)
	 * @retval true design document has been updated
	 * @retval false design document unchanged
	 * @exception UpdateException document cannot be updated because it already exists and it is different
	 *
	 */
	bool uploadDesignDocument(ConstStrW pathname, DesignDocUpdateRule updateRule = ddurOverwrite);

	///Uploads design document from the resource
	/**
	 * It allows to upload design document from resource generated by bin2c utility distributed with jsonrpcserver.
	 * Result of this utility is put into two variables. The content which contains pointer to the resource, and
	 * the contentLen which contains length of the resource.
	 *
	 * @param content pointer to the resource
	 * @param contentLen length of the resource
	 * @param updateRule specifies rule how to handle existing design document
	 * @retval true design document has been updated
	 * @retval false design document unchanged
	 * @exception UpdateException document cannot be updated because it already exists and it is different
	 */
	bool uploadDesignDocument(const char *content, natural contentLen, DesignDocUpdateRule updateRule = ddurOverwrite);




	struct HttpConfig: BredyHttpClient::ClientConfig {
		HttpConfig(const Config &cfg);
	};


protected:

	mutable FastLock lock;




	friend class ChangeFeed;

	String baseUrl;
	String database;
	natural lastStatus;
	Pointer<QueryCache> cache;
	Pointer<Validator> validator;
	AutoArray<char> uidBuffer;
	IIDGen& uidGen;

	String lastConnectError;

	HttpConfig httpConfig;
	HttpClient http;



	template<typename C>
	void reqPathToFullPath(StringRef reqPath, C &output);


	Value jsonPUTPOST(HttpClient::Method method, const StringRef &path, Value data, Value *headers, natural flags);


	friend class ChangesSink;

	Changes receiveChanges(ChangesSink &sink);

public:


};




} /* namespace assetex */


#endif /* ASSETEX_SRC_COUCHDB_H_BREDY_5205456032 */
