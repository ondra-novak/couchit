/*
 * config.h
 *
 *  Created on: 20. 6. 2016
 *      Author: ondra
 */

#ifndef SRC_LIGHTCOUCH_CONFIG_H_
#define SRC_LIGHTCOUCH_CONFIG_H_

namespace BredyHttpClient {
	class IHttpsProvider;
	class IHttpProxyProvider;
}

///LightCouch namespace
namespace LightCouch {

class QueryCache;
class Validator;
class IIDGen;



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
	String baseUrl;
	///name of database (optional) if set, object initializes self to work with database
	String databaseName;
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
	///Pointer to object validator
	/** Everytime anything is being put into database, validator is called. Failed
	 * validation is thrown as exception.
	 */
	Pointer<Validator> validator;

	///Pointer to function that is responsible toUID generation
	/** Pointer can be NULL, then default UID generator is used - See: DefaultUIDGen; */
	Pointer<IIDGen> uidgen;

	///Define provider for https.
	/** Standard http client has disabled https unless you specify https provider. You
	 * can use SimpleHttps::getInstance from the library "jsonrpcserver". However you will
	 * need to link along with libssl
	 */
	Pointer<BredyHttpClient::IHttpsProvider> httpsProvider;

	////Define provider for proxies
	/** Use only if you need to communicate through the proxy. Otherwise you can leave undefined.
	 */
	Pointer<BredyHttpClient::IHttpProxyProvider> proxyProvider;

	///Defines I/O timeout. Default value is 30 seconds.
	/** I/O timeout is applied only for standard requests. It is not applied on pooling through
	 * function CouchDB::listenChanges(). That function defines temporarily own timeout.
	 */
	Optional<natural> iotimeout;

};


}



#endif /* SRC_LIGHTCOUCH_CONFIG_H_ */
