/*
 * couchDB.h
 *
 *  Created on: 8. 3. 2016
 *      Author: ondra
 */

#ifndef ASSETEX_SRC_COUCHDB_H_BREDY_5205456032
#define ASSETEX_SRC_COUCHDB_H_BREDY_5205456032

#include <lightspeed/base/streams/netio.h>
#include <lightspeed/utils/json/json.h>
#include <httpclient/httpStream.h>
#include <lightspeed/mt/gate.h>

#include "uid.h"
#include "object.h"

#include "ichangeNotify.h"

#include "view.h"
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

class CouchDB {
public:


	static const ConstStrA GET;
	static const ConstStrA POST;
	static const ConstStrA PUT;
	static const ConstStrA DELETE;

	///specify this as metaheader for requestJson() - it disables caching for this request
	/**
	 * Specifying this header causes, that request will ignore cache and goes directly to the database. It will
	 * also doesn't store result to the cache. It is equivalent to use CouchDB object without cache
	 *
	 * (CouchDB::disableCache, true)
	 */
	static const ConstStrA disableCache;
	///specify this as metaheader for requestJson() - it always generates a request regardless on current seqNum
	/**
	 * Specifying this header causes, that request will be always performed regardless on, whether current seqNum
	 * changed or not. Request will be performed with ETag and cache will be updated with the result if necesery
	 *
	 * (CouchDB::force, true)
	 */
	static const ConstStrA refreshCache;
	///specify this as metaheader for requestJson() - header field will be replaced with output headers on return
	/**
	 * Response headers are not stored by default. If you need to read them, enable storeHeaders feature. This
	 * causes, that original object will be replaced with response headers (including this header line).
	 */
	static const ConstStrA storeHeaders;


	struct Config {
		///Connection source that can open TCP connection to db's server and port
		NetworkStreamSource connSource;
		///Relative path to from the server's root where database is mapped (optional)
		ConstStrA pathPrefix;
		///name of database (optional) if set, object initializes self to work with database
		ConstStrA databaseName;
		///JSON factory to create JSON objects
		JSON::PFactory factory;
		///Pointer to query cache.
		/** This pointer can be NULL to disable caching
		 * Otherwise, you have to keep pointer valid until the CouchDB object is destroyed
		 * QueryCache can be shared between many instances of CouchDB of the same server.
		 * Please don't share query cache between instances of different servers.
		 *
		 * QueryCache causes, that  every GET query will be stored in the cache. Next time
		 * ETags will be used to determine, whether cache will be used. Using cache
		 * can reduce time by skipping data transfering and parsing
		 */
		Pointer<QueryCache> cache;
	};

	CouchDB(const Config &cfg);
	~CouchDB();

	///Helper object to perform raw http requests to the cauchdb server
	/** It could be useful to put or receive attachments
	 *
	 * This object reuses database connection. It also acquires internal lock
	 * so no other thread can interrupt the connection until object is released.
	 *
	 * After object is released, connect is kept alive
	 *
	 */
	class HttpReq: public BredyHttpSrv::HeaderFieldDef {
	public:

		///Constructs Http object and perform request
		/**
		 * @param db reference to database connection which will be used
		 * @param method method of the request (for example CauchDB::GET)
		 * @param path path relative to database's server root. Current chosen database
		 *  is ignored here
		 * @param body content of body if needed. For GET and DELETE should be empty
		 * @param headers a json object contains key-value structure of custom request headers
		 *
		 * Once object is constructed, it can be used to read response
		 */
		HttpReq(CouchDB &db, ConstStrA method, ConstStrA path, ConstStrA body=ConstStrA(), JSON::Value headers=null);

		///Destructor cleanup connection state.
		~HttpReq();
		///Retrieves a status code
		natural getStatus() const;
		///Retrieves status message
		ConstStrA getStatusMessage() const;

		///Retrieves header
		BredyHttpSrv::HeaderValue getHeaderField(Field field) const;
		///Retrieves header
		BredyHttpSrv::HeaderValue getHeaderField(ConstStrA field) const;
		natural getContentLength() const;

		///Retrieves body of the response as a stream
		/** @note there is no "unwind" once all data are read, they are lost.
		 */
		SeqFileInput getBody();

		template<typename Fn>
		bool enumHeaders(const Fn &fn) const {return response.enumHeaders(fn);}
	protected:

		HttpReq(const HttpReq &x);
		Synchronized<FastLock> lock;
		HttpResponse &response;
		CouchDB &owner;

	};


	///Performs JSON request and response
	/**
	 *
	 * @param method request method (GET,POST,PUT,DELETE)
	 * @param path path relative to the selected database. If path starts with '/',
	 *         it is considered to be relative to server's root. Requests with absolute path are not cached!
	 *         Always use relative path to activate cache
	 * @param postData data posted via JSON. Argument can be null for the method GET.
	 * @param headers contains headers in form key-value. It can also contain following meta-headers which are configures
	 *  the behavior: disableCache, refreshCache, storeHeaders
	 *
	 * @return parsed JSON response
	 */
	JSON::Value requestJson(ConstStrA method, ConstStrA path, JSON::Value postData = null, JSON::Value headers = null);



	///Retrieves UID using CouchDB
	UIDIterator genUID(natural count = 1);


	///Retrieves UID generated localy
	/** UID is generated from
	 * |utc|counter|random
	 *
	 * it uses numbers base 62 (0-9A-Za-z)
	 *
	 * where UTC is time in miliseconds
	 * random is random string generted when applications starts (from secure generator)
	 * counter is local counter increased everytime when UID is generated
	 *
	 * @return
	 */
	static LocalUID genUIDFast();


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

	Changeset createChangeset();

	JSON::IFactory &getJsonFactory();


	///Enables tracking of sequence numbers for caching
	/**
	 * Function directly call QueryCache::trackSeqNumbers with correct database. It enables
	 * tracking sequence numbers for caching. You have to update number everytime database is changed. This
	 * can be achieved by installing dispatcher through listenChanges(). When listenChanges is active,
	 * sequence number is updated automatically (but carefully with filters, you need to install at least
	 * one dispatcher without filtering)
	 *
	 * @return reference to variable which should be updated with every change in the database. It will be initialized
	 * using function getLastSeqNumber()
	 *
	 * @exception - Function throws exception if there is no current database or when caching is not enabled
	 */
	atomicValue &trackSeqNumbers();

	///Loads all conflicts to explore and resolve
	/** You can use this function to download all opened revision for resolution
	 * Function will fill Conflicts container with all opened revisions including their
	 * content.
	 * @param conflictedDoc - conflicted document (must have _conflicts)
	 * @return container all conflicted revisions. If document has no conflicts, results
	 * is empty container. You should test result before using it
	 */
	Conflicts loadConflicts(const Document &conflictedDoc);

	///Retrieves local document (by its id)
	/** You can use function to retrieve local document, because Query object will not retrieve it. To store
	 * document, you can use ChangeSet object. Note that mixing writting standard documents and local documents
	 * in single ChangeSet can cause undefined behaviour when local document is in conflict.
	 * @param localId
	 * @return JSON document;
	 *
	 * @note if sequence numbers are tracked, function disables caching, because
	 * sequence numbers are not updated when local document is stored
	 */
	JSON::Value retrieveLocalDocument(ConstStrA localId);


	struct UpdateFnResult {
		JSON::Value response;
		StringA newRevID;
	};

	UpdateFnResult callUpdateFn(ConstStrA updateFnPath, ConstStrA documentId, JSON::Value arguments);

	///Use json variable to build objects
	const JBuilder json;

protected:

	mutable FastLock lock;

	///Performs raw request
	/**
	 * Function performs a raw request with ability to reconnect the database in case
	 * that keep-alive fails to keep connection open for longer time
	 *
	 * @param method request method (GET,POST,PUT,DELETE)
	 * @param path path relative to the server's root. Current database selection doesn't affect this option
	 * @param postData data to be post with the request. It applied only for methods POST an PUT
	 * @param headers JSON-object which contains additional headers. It shoudl be object otherwise it is ignored
	 * @return reference to HttpResponse object. This object is alredy initialized from the header,
	 * you can use it to read response. Object also handles transfer encoding.
	 *
	 * @note Until all data are read, you cannot perform any other request on this object.
	 * If you left a data in the stream, they are discarded.
	 */
	HttpResponse &rawRequest(ConstStrA method, ConstStrA path, ConstStrA postData, JSON::Value headers);

	///Performs raw request
	/**
	 *
	 * Function performs a raw request. It will not reconnect in case of I/O error
	 *
	 * @param method request method (GET,POST,PUT,DELETE)
	 * @param path path relative to the server's root. Current database selection doesn't affect this option
	 * @param postData data to be post with the request. It applied only for methods POST an PUT
	 * @param headers JSON-object which contains additional headers. It shoudl be object otherwise it is ignored
	 * @return reference to HttpResponse object. This object is alredy initialized from the header,
	 * you can use it to read response. Object also handles transfer encoding.
	 *
	 *  @note Until all data are read, you cannot perform any other request on this object.
	 * If you left a data in the stream, they are discarded.
	 *
	 */

	HttpResponse &rawRequest_noErrorRetry(ConstStrA method, ConstStrA path, ConstStrA postData, JSON::Value headers);


	typedef NetworkStream<> NStream;

	friend class ChangeFeed;

	NetworkStreamSource connSource;
	StringA pathPrefix;
	StringA database;
	Optional<NStream> stream;
	Optional<HttpResponse> response;
	JSON::PFactory factory;
	natural lastStatus;
	bool listenExitFlag;
	QueryCache *cache;
	atomicValue *seqNumSlot;

	StringA lastConnectError;


	natural listenChangesInternal(IChangeNotify &cb,  natural fromSeq, const Filter &filter, ListenMode lm);

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
