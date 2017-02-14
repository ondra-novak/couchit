/*
 * queryServer.h
 *
 *  Created on: 4. 7. 2016
 *      Author: ondra
 */

#ifndef LIGHTCOUCH_QUERYSERVER_H_09888912AE57CCE472
#define LIGHTCOUCH_QUERYSERVER_H_09888912AE57CCE472

#include "couchDB.h"
#include "document.h"
#include "query.h"

#include "queryServerIfc.h"

namespace couchit {


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
	 */
	QueryServer(const StrViewA &name);


	///Registers view
	/**
 	 * @param viewName name of the view. It can be simple name, or name in the format "ddoc/name". This
 	 * name also appears in the design document.
	 * @param impl pointer to created instance of view. Ownership of the pointer is maintained by
	 * the QueryServer and it is automatically destroyed when QueryServer object dies.
	 */
	void regView(String viewName, AbstractViewBase *impl);

	///Registers list
	/**
 	 * @param listName name of the list. It can be simple name, or name in the format "ddoc/name". This
 	 * name also appears in the design document.
	 * @param impl pointer to created instance of the list. Ownership of the pointer is maintained by
	 * the QueryServer and it is automatically destroyed when QueryServer object dies.
	 */
	void regList(String listName, AbstractListBase *impl);

	///Register show
	/**
	 *
	 * @param showName name of the show. It can be simple name, or name in the format "ddoc/name". This
 	 * name also appears in the design document.
	 * @param impl pointer to created instance of the list. Ownership of the pointer is maintained by
	 * the QueryServer and it is automatically destroyed when QueryServer object dies.
	 */
	void regShow(String showName, AbstractShowBase *impl);

	///Register update function
	/**
	 * Update function allows to update document directly in couchDb server.
	 *
	 * @param updateName name of the update. It can be simple name, or name in the format "ddoc/name". This
 	 * name also appears in the design document.
	 * @param impl pointer to created instance of the list. Ownership of the pointer is maintained by
	 * the QueryServer and it is automatically destroyed when QueryServer object dies.
	 */
	void regUpdateFn(String updateName, AbstractUpdateFnBase *impl);

	///Register filter
	/**
	 *
 	 * @param filterName name of the filter. It can be simple name, or name in the format "ddoc/name". This
 	 * name also appears in the design document.
	 * @param impl pointer to created instance of the list. Ownership of the pointer is maintained by
	 * the QueryServer and it is automatically destroyed when QueryServer object dies.
	 */
	void regFilter(String filterName, AbstractFilterBase *impl);


	///Starts queryServer's dispatcher
	/**
	 */
	int runDispatch(std::istream &input, std::ostream &output);

	///start dispatching from standard input/output
	virtual int runDispatchStdIO();


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







	typedef std::function<bool()> RestartRule;

	void setRestartRule(const RestartRule &rule);



protected:


	class StrKey: public StrViewA {
	public:
		StrKey(StrViewA data):StrViewA(data) {}
		StrKey(const String data):StrViewA(data),data(data) {}
	protected:
		String data;
	};


	typedef std::map<StrKey, json::RefCntPtr<AbstractViewBase> > RegView;
	typedef std::map<StrKey, json::RefCntPtr<AbstractShowBase> > RegShowFn;
	typedef std::map<StrKey, json::RefCntPtr<AbstractListBase> > RegListFn;
	typedef std::map<StrKey, json::RefCntPtr<AbstractUpdateFnBase> > RegUpdateFn;
	typedef std::map<StrKey, json::RefCntPtr<AbstractFilterBase> > RegFilterFn;

	RegView views;
	RegShowFn shows;
	RegListFn lists;
	RegUpdateFn updates;
	RegFilterFn filters;
	String qserverName;
	Object ddcache;
	RestartRule rrule;

	std::vector<RowWithKey> rowBuffer;
	std::vector<ReducedRow> valueBuffer;


private:
	std::vector<AbstractViewBase  *> preparedMaps;

	Value commandReset(const Value &req);
	Value commandAddLib(const Value &req);
	Value commandAddFun(const Value &req);
	Value commandMapDoc(const Value &req);
	Value commandReduce(const Value &req);
	Value commandReReduce(const Value &req);
	Value commandDDoc(const Value &req, std::istream &input, std::ostream &output);

	Value commandShow(const Value &fn, const Value &args);
	Value commandList(const Value &fn, const Value &args, std::istream &input, std::ostream &output);
	Value commandUpdate(const Value &fn, const Value &args);
	Value commandView(const Value &fn, const Value &args);
	Value commandFilter(const Value &fn, const Value &args);




	Value compileDesignDocument(const Value &document);
	template<typename T>
	Value compileDesignSection(T &reg, const Value &section, StrViewA sectionName);

	Value createDesignDocument(Object &container, StrViewA fnName, StrViewA &suffix);
};

class RestartRuleChangedFile {
public:

	RestartRuleChangedFile(const String fname);
	bool operator()() const;


protected:
	String file;
	std::time_t mtime;
	static std::time_t getMTime(const String &file);

};


} /* namespace couchit */

#endif /* LIGHTCOUCH_QUERYSERVER_H_09888912AE57CCE472 */

