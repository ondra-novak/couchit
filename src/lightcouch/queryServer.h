/*
 * queryServer.h
 *
 *  Created on: 4. 7. 2016
 *      Author: ondra
 */

#ifndef LIGHTCOUCH_QUERYSERVER_H_09888912AE57CCE472
#define LIGHTCOUCH_QUERYSERVER_H_09888912AE57CCE472
#include <lightspeed/base/containers/constStr.h>
#include <lightspeed/base/streams/fileio.h>
#include "lightspeed/base/framework/app.h"

#include "lightspeed/base/containers/stringKey.h"

#include "lightspeed/base/containers/map.h"
#include "lightspeed/base/memory/sharedPtr.h"

#include "couchDB.h"
#include "document.h"
#include "query.h"

#include "queryServerIfc.h"

namespace LightCouch {


using namespace LightSpeed;



///Helps to create query server
/** CouchDB allows to define custom query server. This class helps to build custom query server.
 * It implements CouchDB query protocol, transfers it and call functions available on this interface.
 *
 * CouchDB design documents contain execution code. This is not case of C++ query server, because
 * all functions must be already coded. Instead of the code, design document can contain just function
 * name and/or arguments.
 */
class QueryServer {
public:

	///Constructor specified name of this query server
	/**
	 * @param name name of query server. It should match name of query server registered in
	 * couchdb's ini section. This is important if you need to generate design documents
	 * from the QueryServer's function defintions. Otherwise, you can use constructor without argument
	 *
	 * @param path path to query server's application. If path is not empty, it must be
	 *  valid and should contain pathname to the file, which is modified everytime the
	 *  application is updated (i.e. path to the current binary is enough).	 *
	 *  The object will monitor the binary and drops connection anytime the referenced
	 *  binary is updated
	 */
	QueryServer(ConstStrA name, ConstStrW path);


	///Registers view
	/**
 	 * @param viewName name of the view. It can be simple name, or name in the format "ddoc/name". This
 	 * name also appears in the design document.
	 * @param impl pointer to created instance of view. Ownership of the pointer is maintained by
	 * the QueryServer and it is automatically destroyed when QueryServer object dies.
	 */
	void regView(StringA viewName, AbstractViewBase *impl);

	///Registers list
	/**
 	 * @param listName name of the list. It can be simple name, or name in the format "ddoc/name". This
 	 * name also appears in the design document.
	 * @param impl pointer to created instance of the list. Ownership of the pointer is maintained by
	 * the QueryServer and it is automatically destroyed when QueryServer object dies.
	 */
	void regList(StringA listName, AbstractListBase *impl);

	///Register show
	/**
	 *
	 * @param showName name of the show. It can be simple name, or name in the format "ddoc/name". This
 	 * name also appears in the design document.
	 * @param impl pointer to created instance of the list. Ownership of the pointer is maintained by
	 * the QueryServer and it is automatically destroyed when QueryServer object dies.
	 */
	void regShow(StringA showName, AbstractShowBase *impl);

	///Register update function
	/**
	 * Update function allows to update document directly in couchDb server.
	 *
	 * @param updateName name of the update. It can be simple name, or name in the format "ddoc/name". This
 	 * name also appears in the design document.
	 * @param impl pointer to created instance of the list. Ownership of the pointer is maintained by
	 * the QueryServer and it is automatically destroyed when QueryServer object dies.
	 */
	void regUpdateFn(StringA updateName, AbstractUpdateFnBase *impl);

	///Register filter
	/**
	 *
 	 * @param filterName name of the filter. It can be simple name, or name in the format "ddoc/name". This
 	 * name also appears in the design document.
	 * @param impl pointer to created instance of the list. Ownership of the pointer is maintained by
	 * the QueryServer and it is automatically destroyed when QueryServer object dies.
	 */
	void regFilter(StringA filterName, AbstractFilterBase *impl);


	///Starts queryServer's dispatcher
	/**
	 * @param stream reference to input-output stream which will be dispatched
	 */
	void runDispatch(PInOutStream stream);

	///start dispatching from standard input/output
	virtual integer runDispatchStdIO();


	///Generates array of design documents
	/**
	 * @return list of design documents (array). If function's name is in form name1/name2, then
	 * name1 specified name of document. It will put into document _design/name1 as function name2.
	 * If function's name hasn't format specified above, then it will put into document _design/qsname under
	 * its name (where qsname is name of the query server).
	 *
	 * for example
	 * @code
	 * assume that query server's name is "userserver"
	 * user/by_login =>  _design/user/_view/by_login
	 * lastlogin => _design/userserver/_view/lastlogin
	 * @endcode
	 *
	 * @note lists functions must be included into same document as views which will be
	 * taken as source. In this case, you need to specify same prefix for view and list
	 *
	 * @code
	 * view:user/by_login + list:user/as_table => _design/user/_list/as_table/by_login
	 * view:lastlogin + list:details => _design/userserver/_list/details/lastlogin
	 * @endcode
	 *
	 */
	Value generateDesignDocuments();


	///synchronizes design documents
	/**
	 *
	 * @param designDocuments design documents generated by function generateDesignDocuments()
	 * @param couch instance of couchDB client
	 *
	 * @note function tries to restart currently running QueryServer by accessing the changed
	 * view. This can cause that error 500 will appear in the CouchDB log file. This error is
	 * ignored. Because of this, function cannot check, whether restart has been successful.
	 */
	static void syncDesignDocuments(Value designDocuments, CouchDB &couch, CouchDB::DesignDocUpdateRule updateRule = CouchDB::ddurOverwrite);




protected:

	///Checks, whether application has been updated.
	/** The function compares modification time of the program with stored time
	 * the differ, exception VersionMistmatchException is thrown.
	 *
	 * Function is called during "reset" phase
	 */
	virtual void checkAppUpdate();




	typedef StringKey<StringA>  StrKey;
	typedef Map<StrKey, SharedPtr<AbstractViewBase> > RegView;
	typedef Map<StrKey, SharedPtr<AbstractShowBase> > RegShowFn;
	typedef Map<StrKey, SharedPtr<AbstractListBase> > RegListFn;
	typedef Map<StrKey, SharedPtr<AbstractUpdateFnBase> > RegUpdateFn;
	typedef Map<StrKey, SharedPtr<AbstractFilterBase> > RegFilterFn;

	RegView views;
	RegShowFn shows;
	RegListFn lists;
	RegUpdateFn updates;
	RegFilterFn filters;
	StringA qserverName;
	StringW qserverPath;
	Object ddcache;

	std::vector<RowWithKey> rowBuffer;
	std::vector<ReducedRow> valueBuffer;

	TimeStamp appUpdateTime;

private:
	struct PreparedMap {
		AbstractViewBase &fn;

		PreparedMap(AbstractViewBase &fn):fn(fn) {}
	};
	AutoArray<PreparedMap> preparedMaps;

	Value commandReset(const Value &req);
	Value commandAddLib(const Value &req);
	Value commandAddFun(const Value &req);
	Value commandMapDoc(const Value &req);
	Value commandReduce(const Value &req);
	Value commandReReduce(const Value &req);
	Value commandDDoc(const Value &req, const PInOutStream &stream);

	Value commandShow(const Value &fn, const Value &args);
	Value commandList(const Value &fn, const Value &args, const PInOutStream &stream);
	Value commandUpdate(const Value &fn, const Value &args);
	Value commandView(const Value &fn, const Value &args);
	Value commandFilter(const Value &fn, const Value &args);




	Value compileDesignDocument(const Value &document);
	template<typename T>
	Value compileDesignSection(T &reg, const Value &section, ConstStrA sectionName);

	Value createDesignDocument(Object &container, ConstStrA fnName, ConstStrA &suffix);
};


class QueryServerApp : public LightSpeed::App, public QueryServer {
public:
	QueryServerApp(ConstStrA name, integer priority = 0);

	virtual integer start(const Args &args);

	///Start function of query server
	/**
	 * The only function query server need to implement. However this is also only place, where
	 * to call various regXXXFn() to register native functions
	 *
	 * @param args arguments passed from command line
	 * @return function must return zero to continue query server, other value immediately exits
	 * process with given value as the exit code
	 */
	virtual integer initServer(const LightSpeed::App::Args &args) = 0;

};

} /* namespace LightCouch */

#endif /* LIGHTCOUCH_QUERYSERVER_H_09888912AE57CCE472 */

