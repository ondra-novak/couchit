/*
 * config.h
 *
 *  Created on: 20. 6. 2016
 *      Author: ondra
 */

#ifndef SRC_LIGHTCOUCH_CONFIG_H_
#define SRC_LIGHTCOUCH_CONFIG_H_
#include <string>

namespace BredyHttpClient {
	class IHttpsProvider;
	class IHttpProxyProvider;
}

///couchit namespace
namespace couchit {

class QueryCache;
class Validator;
class IIDGen;


struct AuthInfo {
	std::string username;
	std::string password;

};


///CouchDB client configuration
/**
 * It contains all necessary parameters to create CouchDB instance. Because single
 * variable carries all required arguments, it is very easy to create multiple connections
 * to the server or make pool of the connections.
 */
struct Config {
	///Database's base url
	/** Put there database's root url (path to the server's root). Don't specify path to
	 * particular database.
	 */
	std::string baseUrl;
	///name of database (optional) if set, object initializes self to work with database
	std::string databaseName;
	///specify unique identifier of this node
	/** Starting from CouchDB 2.0, the local documents are replicated between nodes in
	 * the cluster. Because of this, you cannot use local databse to store node-local
	 * documents. However, the CouchDB class supports flgNodeLocal for retrival and
	 * storing node-local documents. This field is used as prefix.
	 *
	 * This field can be empty. In this case, the library tries to generate or determine
	 * some unique id for the current computer.
	 *
	 */
	std::string node_id;

	///authentification information
	AuthInfo authInfo;
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
	QueryCache *cache = nullptr;
	///Pointer to object validator
	/** Everytime anything is being put into database, validator is called. Failed
	 * validation is thrown as exception.
	 */
	Validator *validator = nullptr;

	///Pointer to function that is responsible toUID generation
	/** Pointer can be NULL, then default UID generator is used - See: DefaultUIDGen; */
	IIDGen *uidgen = nullptr;

	///Defines I/O timeout. Default value is 30 seconds.
	/** I/O timeout is applied only for standard requests. It is not applied on pooling through
	 * function CouchDB::listenChanges(). That function defines temporarily own timeout.
	 */
	std::size_t iotimeout = 30000;

	///Defines, how long is connection to the database keep alive. Older connections are closed
	std::size_t keepAliveTimeout = 3000;

	///Defines, how long the connection can be used in row. Too old connections are closed
	/** because BUG in couchdb creates file descriptor leaks, it needs to close connection
	 * after 5 minutes of use. The 5 minutes is default interval for compaction daemon
	 */
	std::size_t keepAliveMaxAge= 60000*5;

	///Defines timeout for synchronous query.
	/** Queries can rebuild for long time. Default value is 10 minutes.
	 * This timeout is applied only for queries with sync flag, where
	 * the indexing can take some time.
	 */
	std::size_t syncQueryTimeout = 600000;


	///Specifies amount of seconds to wait after initial connection if connection cannot be established
	/** This helps to start the application as service when database is not yet started */
	std::size_t inintialWait = 30;

	///Allows to limit maximum connections per client instance. Default is unlimited
	std::size_t maxConnections = (std::size_t)-1;

	///when token expires.
	/** Default settings is 600 seconds (10 minutes). The client will ask
	 * for the token sooner to prevent race condition when tokens becomes invalid*/
	std::size_t tokenTimeout = (std::size_t)600;

	///Maximum count of documents send by single _bulk_doc request
	/** If there are more documents in single request, the request is split. This operation is invisible, but
	 * it results to multiple requests and reduced performance
	 */
	std::size_t maxBulkSizeDocs = 5000;

	///Minimum count of documents sned by the _bulkd_doc request
	/** There is no reason to set this value other than zero, unless you need to debug updates through
	 * the couchdb's log. Bulk updates of size less then this value are send as standalone PUT requests, so they
	 * appear in the couchdb's log file. However for high numbers this drastically reduces update speed.
	 */
	std::size_t minBulkSizeDocs = 0;
};


}



#endif /* SRC_LIGHTCOUCH_CONFIG_H_ */
