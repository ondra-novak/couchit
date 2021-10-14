/*
 * couchDB.h
 *
 *  Created on: 8. 3. 2016
 *      Author: ondra
 */

#ifndef ASSETEX_SRC_COUCHDB_H_BREDY_5205456032
#define ASSETEX_SRC_COUCHDB_H_BREDY_5205456032

#include <functional>
#include <stack>
#include <mutex>
#include <condition_variable>

#include "json.h"


#include "view.h"
#include "config.h"


#include "minihttp/httpclient.h"
#include "attachment.h"
#include "iqueryable.h"
#include "urlBuilder.h"
#include "revision.h"
#include "result.h"


namespace couchit {


class UIDIterator;
class Query;
class Changeset;
class QueryCache;
class Document;
class Validator;
class Changes;
class ChangesFeed;
class ChangesFeedHandler;
class Validator;
class UpdateResult;
class ShowResult;
class Result;
class BatchWrite;

///Client connection to CouchDB server
class CouchDB {
public:

	///Contains name of key which holds time of document's last update
	/** Note that timestamping is optional feature supported only by couchit. If document
	 * is updated by other way, the timestamp don't need to be updated. Timestamping
	 * is supported by Changeset class
	 */
	static std::string_view fldTimestamp;


	typedef std::size_t Flags;

	/**Maximal length of serialized form of keys to use GET request. Longer
	 * keys are send through POST. Queries sent by POST cannot be cached. However if you
	 * increase the number, it can also generate a lot of cache entries, because whole
	 * requests are cached, not separate keys. Default value is 1024
	 */
	static Flags maxSerializedKeysSizeForGETRequest;

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
	///Retrieve all open revisions
	/** Used with mget(). It returns document of latest revision, other open revisions
	 *  are put into _conflicts or _deleted_conflicts. Whole documents are put there (not just revisions)
	 */
	static const Flags flgOpenRevs = 0x800;
	///create new document when requesting document doesn't exists
	static const Flags flgCreateNew = 0x1000;
	///do not use authentification (token is not used)
	static const Flags flgNoAuth = 0x2000;
	///use syncQueryTimeout instead normal timeout
	static const Flags flgLongOperation = 0x4000;
	///disable exception if document is missing (for function get()), instead null is returned
	static const Flags flgNullIfMissing = 0x8000;
	///retrieves document local for current node (in the cluster)
	/**This flag can be used with get and getLocal. If used with getLocal, then document
	 *  is replicated inside of the cluster, but it is not replicated between databases. If
	 *  used with get(), then document is also replicated between databases.
	 *
	 *  nodeLocal documents has prefix/suffix cointaining node's unique ID
	 */
	static const Flags flgNodeLocal = 0x10000;


	CouchDB(const Config &cfg);
	~CouchDB();

	///Creates copy of instance
	/**
	 * @param other source instance
	 *
	 * @note Function creates new instance using the configuration of the source instance.
	 *
	 */
	CouchDB(const CouchDB &source);



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
	Value genUIDValue(std::string_view prefix) const;


	///Generates new UID
	/** Function is alias to genUIDValue() - for compatibility reasons */
	Value genUID() const {return genUIDValue();}

	///Generates new UID
	/** Function is alias to genUIDValue() - for compatibility reasons */
	Value genUID(std::string_view prefix) const {return genUIDValue(prefix);}


	///Changes current database
	void setCurrentDB(String database);
	///Retrieves current database name
	String getCurrentDB() const;

	///creates database
	/** Creates database. Database is specified by function use()*/
	void createDatabase();

	///creates database
	/** Creates database. Database is specified by function use()
	 * This function works well on CouchDB 2.0. In erlier versions, arguments
	 * are ignored
	 *
	 *
	 * @param numShards specify count of shards. Thi
	 *
	 * */
	void createDatabase(unsigned int numShards, unsigned int numReplicas);

	///Deletes database
	/** Deletes current database. Database is specified by function use */
	void deleteDatabase();



	///Bulk upload
	Value bulkUpload(const Value docs, bool replication = false);


	///Determines last sequence number
	/** Function requests server to retrieve last sequence number. You will need
	 * this to start listening for changes. It is recomended to determine last sequence
	 * number before application starts to download current state to receive
	 * changes made during synchronization
	 * @return last sequence number
	 */
	SeqNumber getLastSeqNumber();

	///Retrieves last known sequence number (LKSQID)
	/** Function returns last known sequence number without asking the
	 * server. This value is updated from various sources as the client is used.
	 *
	 * @return last known sequence number
	 *
	 * The sequence number can be marked as old. In this situation, the client knows that
	 * sequence number is old without knowning the latest number. The sequence number is marked as old after calling
	 * some update functions.
	 *
	 * Sources used to update LKSQID:
	 *    views - their sequence number if it is fresh than LKSQID
	 *    changes feed
	 *
	 *
	 * Starting from CouchDB 2.0.0, the sequence number is presented as string, starting
	 * with a number followed by dash and number of characters. It looks like revision id.
	 * The class SequenceNumber allows to work with sequence numbers regardless on which
	 * version of CouchDB is used.
	 *
	 */
	SeqNumber getLastKnownSeqNumber() const;

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
	Query createQuery(Flags viewFlags);

	///Create query to ask _all_docs
	Query allDocs(Flags viewFlags);

	///Create query to ask _all_docs
	Query localDocs(Flags viewFlags);


	///Creates changeset
	/** Changeset will use this database connection to update document
	 *
	 * @return
	 */
	Changeset createChangeset();


	///Creates object which is used to receive database changes
	ChangesFeed createChangesFeed();





	///Retrieves local document (by its id)
	/**
	 *
	 * @param localId id of the document without _local prefix
	 * @param flags similar flags as get(). If you include flgNodeLocal, the returned
	 * document is assigned to this node. Note that node local documents are still
	 * replicated to the other nodes, but this flag prevent to mixing these documents into
	 * one.
	 *
	 * @return returned document. Note that _ID of the document doesn't contain requested localId.
	 * The _ID contains whole ID including _local prefix and thus whole document can be
	 * stored by put() or by ChangeSet. You can also use get() to access this document if you
	 * use _ID instead its local name.
	 */
	Value getLocal(const std::string_view &localId, Flags flags = 0);

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
	Value get(const std::string_view &docId, Flags flags = 0);


	///Retrieves other revision of the document
	/**
	 * @param docId document id
	 * @param revId revision id
	 * @param flags can be only flgDisableCache or zero. The default value is recommended.
	 * @return json with document
	 */
	Value get(const std::string_view &docId, const std::string_view &revId, Flags flags = flgDisableCache);

	///Asynchronously retrieve document
	/**
	 * @param docId document to retrieve
	 * @param cb callback which is called when document is retrieved
	 *
	 * @note function uses background batch to retrieve documents. It combines multiple
	 * requests into single database request. Function always retrieves document with conflicts.
	 * If the document doesn't exists, it generates new empty document. Use Document::isNew() to
	 * determine, whether document has been found or generated empty
	 */
	void getAsync(const json::String &docId, std::function<void(Document &doc)> &&cb);

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
	Document newDocument(const std::string_view &prefix);

	///Retrieves pointer to validator
	/**
	 * @return function returns null, when validator is not defined. Otherwise function
	 * returns pointer to current validator
	 */
	Validator *getValidator() const {return cfg.validator;}


	///Calls update handler to update document using server-side function
	/** Server side function must be defined in design documents.
	 *
	 * See: UpdateProc
	 */
	UpdateResult execUpdateProc(std::string_view updateHaauthInfondlerPath, std::string_view documentId, Value arguments);


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
	ShowResult execShowProc(const std::string_view &showHandlerPath, const std::string_view &documentId, const Value &arguments, Flags flags = 0);


	///Uploads attachment with specified document
	/**
	 * @param document document object. The document don't need to be complete, only _id and _rev must be there.
	 * @param attachmentName name of attachment
	 * @param contentType content type of the attachment
	 * @return Returns Upload object which can be used to stream new data into the database
	 * @note The returned object should not be stored for long way, because it blocks whole Couchdb instance
	 * until the upload is finished
	 */
	Upload putAttachment(const Value &document, const std::string_view &attachmentName, const std::string_view &contentType);

	///Uploads attachment with specified document
	/**
	 * Function allows to upload attachment stored complete in the memory.
	 *
	 * @param document document object. The document don't need to be complete, only _id and _rev must be there.
	 * @param attachmentName name of attachment
	 * @param attachmentData content of attachment
	 * @return Funtcion returns new revision of the document, if successful.
	 */
	String putAttachment(const Value &document, const std::string_view &attachmentName, const AttachmentDataRef &attachmentData);

	///Downloads attachment
	/**
	 * @param document document. The document don't need to be complete, only _id and _rev must be there.
	 * @param attachmentName name of attachment to retrieve
	 * @param etag if not empty, function puts string as "if-none-match" header.
	 */
	Download getAttachment(const Document &document, const std::string_view &attachmentName,  const std::string_view &etag=std::string_view());

	///Downloads latest attachments
	/** Allows to easily download the latest attachment by given docId an dattachmentName
	 *
	 * @param docId document Id
	 * @param attachmentName attachment name
	 * @param etag (optional) etag, if not empty, then it is put to the header and result can be notModified
	 * @param rev (optional) revision, if empty, latest revision is used
	 * @return download object
	 */
	Download getAttachment(const std::string_view &docId, const std::string_view &attachmentName,  const std::string_view &etag=std::string_view(), const std::string_view &rev = std::string_view());


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
	bool putDesignDocument(const Value &content, DesignDocUpdateRule updateRule = ddurOverwrite);

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
	bool putDesignDocument(const std::string &pathname, DesignDocUpdateRule updateRule = ddurOverwrite);

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
	bool putDesignDocument(const char *content, std::size_t contentLen, DesignDocUpdateRule updateRule = ddurOverwrite);

	struct WriteOptions {
		///enable batch mode
		bool batchok = false;
		///document is being replicated
		bool replication = false;
		///setting value above 0 defines minimum count of writes to return
		unsigned int quorum = 0;
		///setting value to true causes asynchornous write
		/** asynchronous write is done using separate thread. The main benefit of such thread is
		 * to batch writes into single request.
		 */
		bool async = false;
		///callback for asynchronous write
		/**
		 * used when async = true to report result of write
		 * @param $1 true=ok, false=error
		 * @param $2 error description: "conflict" in case of conflict
		 */
		std::function<void(bool, json::Value)> async_cb;

	};


	///Updates single document
	/**
	 * @param doc document to update. It must have the fields "_id" and "_rev" set. The function updates the field "_rev" with new revisionId.
	 * @exception UpdateException Update is not possible (exception contains one item)
	 * @exception RequestException Other error
	 *
	 * @note function "collapses" document and its changes
	 */
	void put(Document &doc);

	///Updates single document
	/**
	 * @param doc document to update
	 * @param opts write options
	 * @param no_exception set true to prevent throwing conflict exception
	 * @retval true updated
	 * @retval false conflict
	 *
	 * @note in case of asynchronous write, function returns true and conflict error state is send through the asynchronous function
	 */
	bool put(Document &doc, const WriteOptions &opts, bool no_exception = false);



	///Updates single document
	/**
	 * @param doc document as JSON value, it must have _id, and for update, it needs _rev
	 * @return new revision value
	 */
	Value put(const Value &doc);
	Value put(const Value &doc, const WriteOptions &opts, bool no_exception = false);


	///Commands the database to update specified view
	/**
	 * @param view view to update. Function just uses URL to the view, flags and postprocessing is ignored.
	 *  Note that CouchDB generally updates all view in the same design document, so you
	 *  don't need to call this function for every view in the same design document.
	 *
	 * @param wait specify true if you want to wait for the completion. Otherwise, the
	 * operation is executed in the background (on the server)
	 *
	 * @return Count of rows in the view in time of request. Function may return 0 when wait is set to false
	 * that indicates, that view is being rebuild complete
	 */
	std::size_t updateView(const View &view, bool wait = false);



	///retrieves current configuration
	const Config &getConfig() const {return cfg;}

	///Retrieves serial number of the database
	Value getSerialNr() ;



	///Function receives all opened or specified revisions
	/**
	 *
	 * @param docId id of document
	 * @param revisions list of revisions. If the list is empty, null or undefined, returns all
	 * opened revisions.
	 * @param flags list of flags, note that only some supported
	 * @return function returns array of documents. CouchDB should store documents in the same
	 * order as they has been request.
	 *
	 * @note There is small difference between return value and result generated by HTTP api.
	 * Function returns array of documents without intermediate object as it is generated by
	 * HTTP api. However, the key used as document status is perserved and you can optain it by
	 * calling function getKey(). Also note that missing document is not stored as object.
	 */
	Value getRevisions(const std::string_view docId, Value revisions, Flags flags);


	///Updates document and removes all conflicts
	/**
	 * @param doc new revision of the document (merged). The document must have field _conflict filled with
	 * conflicted revisions ids to prune them.
	 *
	 * @note it is strongly recommended to run a local validator. If the validation fails on the server,
	 * the document is left unchanged, but conflicted revisions are removed. No-one is able to repeate the merging,
	 * because the document is considered without conflicts. Local validator prevents updating when validation fails
	 *
	 * @note There is different system of calculating revision numbers. The revision ID of the
	 *  merged document is ends with letter "M" (main). Deleted conflicts ends with "R" (resolved). This the only
	 *  way how to distinguish between conflict deleted due resolution and deleted by a user.
	 */
	void pruneConflicts(Document &doc);



	///Item used in function mget
	struct MGetItem {
		///contains id of document
		std::string_view id;
		///contains revision, it can be empty for latest revision
		std::string_view rev;

		template<typename T>
		static std::string_view getId(const T &x) {
			if constexpr(std::is_class<T>::value) {
				return x.id;
			} else {
				return x;
			}
		}
		template<typename T>
		static std::string_view getRev(const T &x) {
			if constexpr(std::is_class<T>::value) {
				return x.rev;
			} else {
				return std::string_view();
			}
		}

	};


	///Multiple get documents
	/**
	 * @param begin first item (each item should have MGetItem layout)
	 * @param end end of selection
	 * @param flags various flags. Supported: flgRevisionsInfo,  flgOpenRevs
	 * @return
	 */
	template<typename Iter>
	Result mget(Iter &&begin, Iter &&end, Flags flags = 0) {
		Array req;

		while (begin != end) {
			const auto &x = *begin;
			std::string_view rev = MGetItem::getRev(x);
			std::string_view id = MGetItem::getId(x);
			if (rev.empty()) {
				req.push_back(Object{{"id", id}});
			} else {
				req.push_back(Object{{"id", id},{"rev", rev}});
			}
			++begin;
		}
		return mget_impl(req, flags);
	}

protected:
	static constexpr std::uint64_t magic_const = 0x1234567812345678UL;
	std::uint64_t magic = magic_const;
	mutable std::mutex lock;
	std::condition_variable connRelease;
	typedef std::lock_guard<std::mutex> LockGuard;





	Config cfg;

	std::size_t lastStatus = 0;
	std::size_t curConnections;
	mutable std::vector<char> uidBuffer;
	IIDGen& uidGen;
	SeqNumber lksqid;

	///stores current token (as cookie)
	String token;
	///stores previous token - this need to keep old token during it is changed by other thread
	String prevToken;
	///time when token expires
	time_t tokenExpireTime = 0;
	///time when token should be refreshed
	time_t tokenRefreshTime = 0;
	///only one thread can refresh token at time
	std::mutex tokenLock;

	Value authObj;


	Value jsonPUTPOST(bool methodPost, const std::string_view &path, Value data, Value *headers, Flags flags);


	friend class ChangesFeed;

	Changes receiveChanges(ChangesFeed &sink);
	void receiveChangesContinuous(ChangesFeed &sink, ChangesFeedHandler &fn);

	class Queryable: public IQueryableObject {
	public:
		Queryable(CouchDB &owner);


		virtual Value executeQuery(const QueryRequest &r);

	protected:
		CouchDB &owner;

	};


	Queryable queryable;

	Value serialNr;

	std::unique_ptr<BatchWrite> batchWrite;

public:

	typedef std::chrono::system_clock SysClock;
	typedef std::chrono::time_point<SysClock> SysTime;



	class Connection: public UrlBuilder {
	public:
		HttpClient http;

		Connection();

		std::string_view getUrl() const {return UrlBuilder::operator std::string_view();}
		String lastConnectError;

	protected:
		friend class CouchDB;
		SysTime lastUse, firstUse;
	};

	class ConnectionDeleter {
	public:
		CouchDB *owner;
		ConnectionDeleter():owner(nullptr) {}
		ConnectionDeleter(CouchDB *owner):owner(owner) {}
		void operator()(Connection *b) {
			if (owner)
				owner->releaseConnection(b);
			else
				delete b;
		}
	};

	typedef std::unique_ptr<Connection, ConnectionDeleter> PConnection;


	///Retrieve connection to perform direct requests
	/** Direct requests such as requestGET and requestPUT need
	 * to be called with connection object. The object also
	 * provides url builder
	 *
	 * @param resourcePath path to the resource relative to current database.
	 * If the path starts by slash '/', then the path refers to
	 * the whole couchDB server. Otherwise (without the slash) the path
	 * refers current database.
	 * @param fresh request for fresh connection (do not reuse any opened keep-alive connection).
	 *
	 * @return Function returns connection object along with url builder.
	 * You should release the connection object as soon as possible.
	 *
	 * @note If you want to reuse connection, call setUrl()
	 *
	 * Note, the function is MT safe, but the connection itself not. This
	 * allows to use CouchDB instance by multiple threads, where each
	 * thread acquired connection
	 */
	PConnection getConnection(std::string_view resourcePath = std::string_view(), bool fresh = false);

	///Allows to reuse connection to additional request
	/** Function just sets the url of the connection to be in relation
	 * with the current database
	 *
	 * @param conn opened connection (see getConnection() )
	 * @param resourcePath path to the resource relative to current database.
	 * If the path starts by slash '/', then the path refers to
	 * the whole couchDB server. Otherwise (without the slash) the path
	 * refers current database.
	 *
	 */
	void setUrl(PConnection &conn, std::string_view resourcePath = std::string_view());


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
	Value requestGET(PConnection &conn, Value *headers = nullptr, Flags flags = 0);
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
	Value requestPOST(PConnection &conn, const Value &postData, Value *headers = nullptr, Flags flags = 0);
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
	Value requestPUT(PConnection &conn, const Value &postData, Value *headers = nullptr, Flags flags = 0);

	///Performs DELETE request at database
	/**
	 *
	 * @param path absolute path to the resource to delete
	 * @param headers aditional headers
	 * @param flags flags that controls behaviour
	 * @return
	 */
	Value requestDELETE(PConnection &conn, Value *headers = nullptr, Flags flags = 0);


	enum class Filter {
		///no filter
		no_filter,
		///list of documents in filter_spec
		docids,
		///selector in filter_spec
		selector,
		///design document filter (doc/filter)
		custom,
		///view name
		view,
		///receive only design docs
		design
	};


	///New implementation for receiving changes
	/** Due to very buggy implementation of continuous feed, this is
	 * simplified version for streaming changes using longpoll feature.
	 * You can repeatedly call the function receiveChanges while keeping
	 * this state - or you can use ChangesDistributor or ChangesReader
	 */
	struct ChangeFeedState {

		///specify sequence number since to read changes
		/** This field is updated after each call, so you can read multiple changes */
		json::Value since = Value();
		///filter specification (depend on filter)
		json::Value filter_spec = Value();
		///extra arguments
		json::Value extra_args = Value();
		///filter type
		Filter filter = Filter::no_filter;
		///set true to include docs
		bool include_docs =false;
		///set true to include conflicts
		bool conflicts = false;
		///set true to include attachments
		bool attachments = false;
		///set true to read changes in reverse order
		bool descending = false;
		///set timeout for waiting to changes
		unsigned int timeout = 0;
		///set maximum count of changes read by single call
		unsigned int limit = ~0;
		///allow batching - this means, that seqNumber can be null, thus don't need to be stored
		bool batching = true;
		///Specifies minimal poll interval in milliseconds when changes not comming in batches
		/** Adds sleep between cycles if interval would be lower. It doesn't affect performance
		 * if there are a lot of changes
		 */
		unsigned int poll_interval=0;

		///internally used to store current connection
		/**Note that if you no longer need to receive changes, destroy this object or
		 * set this pointer to nullptr, otherwise single connection is still reserved
		 */
		PConnection connection = nullptr;
		///is set to true, when request has been canceled
		std::atomic<bool> canceled = false;

		std::chrono::system_clock::time_point last_request_time;



		ChangeFeedState();
		ChangeFeedState(ChangeFeedState && other);
		ChangeFeedState(const ChangeFeedState & other);
	};

	///Receives changes
	/**
	 * @param feedState state of feed
	 * @return received changes
	 *
	 * @note function blocks operation until the request is fullfilled or canceled
	 */
	Changes receiveChanges(ChangeFeedState &feedState);

	///Cancels pending receiveChanges
	/**
	 * @param feedState pending state.
	 *
	 */
	static void abortReceiveChanges(ChangeFeedState &feedState);



protected:
	typedef std::vector<PConnection> ConnPool;
	ConnPool connPool;
	SysTime lastPoolCheck;

	Value jsonPUTPOST(PConnection &conn, bool methodPost, Value data, Value *headers, Flags flags);

	void handleUnexpectedStatus(PConnection& conn);
	Download downloadAttachmentCont(PConnection &conn, const std::string_view &etag);
	static Value parseResponse(PConnection &conn);
	static Value parseResponseBin(PConnection &conn);
	void releaseConnection(Connection *b);
	Value postRequest(PConnection &conn, const std::string_view &cacheKey, Value *headers, Flags flags);
	Value getToken();
	void setupHttpConn(HttpClient &http, Flags flags);

private:
	int initChangesFeed(const PConnection& conn, ChangesFeed& sink);
	void updateSeqNum(const Value& seq);
	void operator=(const CouchDB &other) = delete;

	///Generates new UID using preconfigured generator
	/** See Config how to setup custom generator
	 *
	 * @return string reference to UID. Note that reference is valid until method is called again. You
	 * should immediatelly create a copy. Function itself is MT safe, however the reference can be
	 * lost in time of return
	 *
	 */
	std::string_view lkGenUID() const;

	///Generates new UID using preconfigured generator
	/**See Config how to setup custom generator
	 *
	 * @param prefix user defined prefix which is put at the beginning of the ID
	 * @return string reference to UID. Note that reference is valid until method is called again. You
	 * should immediatelly create a copy. Function itself is MT safe, however the reference can be
	 * lost in time of return
	 */
	std::string_view lkGenUID(std::string_view prefix) const;

	Result mget_impl(Array &req, Flags flags = 0);

	template<typename Fn>
	auto retry(Fn &&fn) -> decltype(std::declval<Fn>()());

};




} /* namespace assetex */


#endif /* ASSETEX_SRC_COUCHDB_H_BREDY_5205456032 */
