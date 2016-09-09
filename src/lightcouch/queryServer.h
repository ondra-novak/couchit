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
#include <lightspeed/utils/json.h>
#include "lightspeed/base/framework/app.h"

#include "lightspeed/base/containers/stringKey.h"

#include "lightspeed/base/containers/map.h"
#include "lightspeed/base/memory/sharedPtr.h"

#include "couchDB.h"
#include "document.h"
#include "object.h"
#include "query.h"


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

	///Standard constructor
	QueryServer();
	///Constructor specified name of this query server
	/**
	 * @param name name of query server. It should match name of query server registered in
	 * couchdb's ini section. This is important if you need to generate design documents
	 * from the QueryServer's function defintions. Otherwise, you can use constructor without argument
	 */
	QueryServer(ConstStrA name);


	///Register map function
	/**
	 * @param name name of map function.
	 *  The name can contain any character above space. Some characters has special meaning for generation
	 *  of design documents, for example / and *. Name starting by _ will not expirted to design document
	 * @param fn a function with arguments (const Document &doc, const IEmitFn &emit, const ConstStrA &args)
	 *
	 * @see IMapDocFn
	 */
	template<typename Fn> void regMapFn(StringA name, natural version, const Fn &fn);
	///Register map+reduce function
	/**
	 * @param name name of the reduce function
	 * @param mapFn map function with arguments (const Document &doc, const IEmitFn &emit, const ConstStrA &args)
	 * @param reduce reduce function with arguments (const ConstValue &kvlist, const ConstStrA &args)
	 * @param rereduce rereduce function with arguments (const ConstValue &list, const ConstStrA &args)
	 * @see IMapDocFn, IReduceFn
	 */
	template<typename MapFn, typename ReduceFn, typename ReReduceFn>
			void regMapReduceFn(StringA name, natural version, const MapFn &mapFn, const ReduceFn &reduce, const ReReduceFn &rereduce);
	template<typename Fn> void regListFn(StringA name, const Fn &reduce);
	template<typename Fn> void regShowFn(StringA name, const Fn &showFn);
	template<typename Fn> void regUpdateFn(StringA name, const Fn &updateFn);
	template<typename Fn> void regFilterFn(StringA name, const Fn &filterFn);

	void runDispatch(PInOutStream stream);


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
	ConstValue generateDesignDocuments();


	///synchronizes design documents
	/**
	 *
	 * @param designDocuments design documents generated by function generateDesignDocuments()
	 * @param couch instance of couchDB client
	 */
	static void syncDesignDocuments(ConstValue designDocuments, CouchDB &couch, CouchDB::DesignDocUpdateRule updateRule = CouchDB::ddurOverwrite);


	///Emit function - passed as argument to map function
	class IEmitFn {
	public:
		///Emit function
		/**
		 * @param key key to emit
		 * @param value value to emit
		 */
		virtual void operator()(const ConstValue &key, const ConstValue &value) = 0;
		virtual ~IEmitFn() {}
	};


	///start dispatching from standard input/output
	virtual integer runDispatchStdIO();


	Json json;

protected:




	///Map function
	class IMapDocFn {
	public:
		///Map function
		/**
		 * @param doc document to map
		 * @param emit emit function
		 */
		virtual void operator()(const Document &doc,  IEmitFn &emit) = 0;
		///Receives function version
		/** For maps, version is read from the argumemt line and checked. If version mismatches,
		 *  process is terminated to give couchdb chance to start new instance containing new version
		 * @return
		 */
		virtual natural getVersion() const = 0;
		virtual ~IMapDocFn() {}
	};

	///Reduce function
	class IReduceFn {
	public:
		///Reduce function
		/**
		 * @param kvlist list of key-value. Each item is [[key,id], value]
		 * @return reduced value
		 */
		virtual ConstValue operator()(const ConstValue &kvlist) = 0;
		virtual ~IReduceFn() {}
	};


	///context of every function
	class Context {
	public:
		///request object (see CouchDB documentation)
		ConstValue request;
		///arguments passed from design document
		ConstStrA args;
		///response object (see CouchDB documentation)
		Value response;
	};

	///List function context
	class IListContext {
	public:
		virtual ~IListContext() {}
		///Receive next row from view
		/**
		 * @return a valid row data, or null if none available.
		 *
		 * @note function also flushes any data sent by send() function. The response
		 * object in the context is also sent, so any modification in it will not applied.
		 */
		virtual ConstValue getRow() = 0;
		///send text to output
		virtual void send(ConstStrA text) = 0;
		///send json to output
		virtual void send(ConstValue jsonValue) = 0;
		///retrieve view header
		virtual ConstValue getViewHeader() const = 0;
	};


	///List function
	class IListFunction {
	public:
		///List function
		/**
		 * @param list contains interface to enumerate rows
		 *
		 * Works similar as JS version. You receive IListContext through the variable list. You
		 * can call method of this interface inside of the function. The function getRow returns
		 * next row from the view. Object can be converted to Row object.
		 *
		 * According to couchdb's query protocol, the function send() will emit its output
		 * during the getRow is processed. Sends after last row has been received will
		 * be processed on function exit. You don't need to process all rows.
		 */
		virtual void operator()(IListContext &list, ConstValue request) = 0;
		virtual ~IListFunction() {}
	};

	///show function
	class IShowFunction {
	public:
		///Show function
		/**
		 * @param doc document to show
		 * @param request request (see CouchDB documentation)
		 * @return response object
		 */

		virtual ConstValue operator()(const Document &doc, ConstValue request) = 0;
		virtual ~IShowFunction() {}
	};

	///update function
	class IUpdateFunction {
	public:
		///Update function
		/**
		 * @param doc document to change. Change the document (make it dirty) and the document will be stored
		 *   (note it can be null)
		 * @param request request
		 * @return response object
		 */
		virtual ConstValue operator()(Document &doc, ConstValue request) = 0;
		virtual ~IUpdateFunction() {}
	};

	///Filter function
	class IFilterFunction {
	public:
		///Filter function
		/**
		 * @param doc document
		 * @param request request object
		 * @retval true allow document
		 * @retval false disallow document
		 */
		virtual bool operator()(const Document &doc, ConstValue request) = 0;
		virtual ~IFilterFunction() {}
	};

	class Error: public Exception {
	public:
		LIGHTSPEED_EXCEPTIONFINAL;

		Error(const ProgramLocation &loc, StringA type, StringA explain)
			:Exception(loc),type(type),explain(explain) {}
		const StringA &getType() const;
		const StringA &getExplain() const;
	protected:
		StringA type, explain;
		void message(ExceptionMsg &msg) const;

	};

	class VersionMistmatch: public Exception {
	public:
		LIGHTSPEED_EXCEPTIONFINAL;

		VersionMistmatch(const ProgramLocation &loc):Exception(loc) {}
	protected:
		void message(ExceptionMsg &msg) const;

	};

	typedef StringKey<StringA>  StrKey;
	typedef Map<StrKey, SharedPtr<IMapDocFn> > RegMapFn;
	typedef Map<StrKey, SharedPtr<IReduceFn> > RegReduceFn;
	typedef Map<StrKey, SharedPtr<IShowFunction> > RegShowFn;
	typedef Map<StrKey, SharedPtr<IListFunction> > RegListFn;
	typedef Map<StrKey, SharedPtr<IUpdateFunction> > RegUpdateFn;
	typedef Map<StrKey, SharedPtr<IFilterFunction> > RegFilterFn;

	RegMapFn regMap;
	RegReduceFn regReduce;
	RegReduceFn regReReduce;
	RegShowFn regShow;
	RegListFn regList;
	RegUpdateFn regUpdate;
	RegFilterFn regFilter;
	Value trueVal;
	StringA qserverName;

private:
	struct PreparedMap {
		IMapDocFn &fn;

		PreparedMap(IMapDocFn &fn):fn(fn) {}

	};
	AutoArray<PreparedMap> preparedMaps;

	ConstValue commandReset(const ConstValue &req);
	ConstValue commandAddLib(const ConstValue &req);
	ConstValue commandAddFun(const ConstValue &req);
	ConstValue commandMapDoc(const ConstValue &req);
	ConstValue commandReduce(const ConstValue &req);
	ConstValue commandReReduce(const ConstValue &req);
	ConstValue commandDDoc(const ConstValue &req, const PInOutStream &stream);

	ConstValue commandReduceGen(const ConstValue &req, RegReduceFn &regReduce);

	ConstValue commandShow(const ConstValue &fn, const ConstValue &args);
	ConstValue commandList(const ConstValue &fn, const ConstValue &args, const PInOutStream &stream);
	ConstValue commandUpdate(const ConstValue &fn, const ConstValue &args);
	ConstValue commandView(const ConstValue &fn, const ConstValue &args);
	ConstValue commandFilter(const ConstValue &fn, const ConstValue &args);


	static void splitToNameAndArgs(ConstStrA cmd, ConstStrA &name, ConstStrA &args);

	Container ddcache;

	ConstValue compileDesignDocument(const ConstValue &document);
	template<typename T>
	ConstValue compileDesignSection(T &reg, const ConstValue &section, ConstStrA sectionName);

	Value createDesignDocument(Value container, ConstStrA fnName, ConstStrA &suffix);
};

template<typename Fn>
inline void LightCouch::QueryServer::regMapFn(StringA name, natural version, const Fn &fn) {
	class X: public IMapDocFn {
		Fn fn;
		natural version;
	public:
		X(const Fn &fn, natural version):fn(fn),version(version) {}
		virtual void operator()(const Document &doc,  IEmitFn &emit) override {
			fn(doc,emit);
		}
		virtual natural getVersion() const {return version;}
	};
	this->regMap(StrKey(name)) =  new X(fn,version);
}

template<typename MapFn, typename ReduceFn, typename ReReduceFn>
void QueryServer::regMapReduceFn(StringA name, natural version, const MapFn &mapFn, const ReduceFn &reduce, const ReReduceFn &rereduce) {
	class X: public IReduceFn {
		ReduceFn fn;
	public:
		X(const ReduceFn &fn):fn(fn){}
		virtual ConstValue operator()(const ConstValue &kvlist) override {
			return fn(kvlist);
		}
	};
	class Y: public IReduceFn {
		ReReduceFn fn;
	public:
		Y(const ReReduceFn &fn):fn(fn){}
		virtual ConstValue operator()(const ConstValue &values)  override{
			return fn(values);
		}
	};
	this->regReduce(StrKey(name)) =  new X(reduce);
	this->regReReduce(StrKey(name)) =  new Y(rereduce);
	regMapFn(name,version, mapFn);
}

template<typename Fn>
inline void QueryServer::regListFn(StringA name, const Fn& fn) {
	class X: public IListFunction {
		Fn fn;
	public:
		X(const Fn &fn):fn(fn){}
		virtual void operator()(IListContext &list, ConstValue request) override {
			fn(list,request);
		}
	};
	this->regList(StrKey(name)) =  new X(fn);
}

template<typename Fn>
inline void LightCouch::QueryServer::regShowFn(StringA name, const Fn& showFn) {
	class X: public IShowFunction {
		Fn fn;
	public:
		X(const Fn &fn):fn(fn){}
		virtual ConstValue operator()(const Document &document, ConstValue request) override {
			return fn(document,request);
		}
	};
	this->regShow(StrKey(name)) =  new X(showFn);

}

template<typename Fn>
inline void LightCouch::QueryServer::regUpdateFn(StringA name, const Fn& updateFn) {

	class X: public IUpdateFunction {
		Fn fn;
	public:
		X(const Fn &fn):fn(fn){}
		virtual ConstValue operator()(
				Document &document,
				ConstValue request) override {
			return fn(document,request);
		}
	};
	this->regUpdate(StrKey(name)) =  new X(updateFn);


}

template<typename Fn>
inline void LightCouch::QueryServer::regFilterFn(StringA name, const Fn& filterFn) {

	class X: public IFilterFunction {
		Fn fn;
	public:
		X(const Fn &fn):fn(fn){}
		virtual bool operator() (const Document &doc, ConstValue request) override  {
			return fn(doc,request);
		}
	};
	this->regFilter(StrKey(name)) =  new X(filterFn);


}

class QueryServerApp : public LightSpeed::App, public QueryServer {
public:
	QueryServerApp(integer priority = 0);
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

