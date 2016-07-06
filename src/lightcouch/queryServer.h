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
class QueryServer: public LightSpeed::App {
public:


	///Start function of query server
	/**
	 * The only function query server need to implement. However this is also only place, where
	 * to call various regXXXFn() to register native functions
	 *
	 * @param args arguments passed from command line
	 * @return function must return zero to continue query server, other value immediately exits
	 * process with given value as the exit code
	 */
	virtual integer initServer(const Args &args) = 0;

	///Register map function
	/**
	 * @param name name of map function.
	 * @param fn a function with arguments (const Document &doc, const IEmitFn &emit, const ConstStrA &args)
	 *
	 * @see IMapDocFn
	 */
	template<typename Fn> void regMapFn(StringA name, const Fn &fn);
	///Register reduce function
	/**
	 * @param name name of the reduce function
	 * @param reduce reduce function with arguments (const ConstValue &kvlist, const ConstStrA &args)
	 * @param rereduce rereduce function with arguments (const ConstValue &list, const ConstStrA &args)
	 * @see IReduceFn
	 */
	template<typename Fn1,typename Fn2> void regReduceFn(StringA name, const Fn1 &reduce, const Fn2 &rereduce);
	template<typename Fn> void regListFn(StringA name, const Fn &reduce);
	template<typename Fn> void regShowFn(StringA name, const Fn &showFn);
	template<typename Fn> void regUpdateFn(StringA name, const Fn &updateFn);
	template<typename Fn> void regFilterFn(StringA name, const Fn &filterFn);

	void runDispatch(PInOutStream stream);

protected:


	virtual integer start(const Args &args);

	Json json;

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

	///Map function
	class IMapDocFn {
	public:
		///Map function
		/**
		 * @param doc document to map
		 * @param emit emit function
		 * @param args arguments passed from the design document. Arguments are passed as they are.
		 */
		virtual void operator()(const Document &doc, const IEmitFn &emit, const ConstStrA &args) = 0;
		virtual ~IMapDocFn() {}
	};

	///Reduce function
	class IReduceFn {
	public:
		///Reduce function
		/**
		 * @param kvlist list of key-value. Each item is [[key,id], value]
		 * @param args arguments passed from the design document. Arguments are passed as they are.
		 * @return reduced value
		 */
		virtual ConstValue operator()(const ConstValue &kvlist, const ConstStrA &args) = 0;
		virtual ~IReduceFn() {}
	};

	struct Context {
		///couchDB request object
		ConstValue request;
		///arguments passed from design documents as they are
		ConstStrA args;
		///output header - only used when function generates an output
		/** List function uses this header before first row is received */
		Value outHeader;
		///output stream - write content of page here
		SeqFileOutput output;

		Context(ConstValue request, ConstStrA args, Value outHeader, const SeqFileOutput &output)
			:request(request),args(args),outHeader(outHeader),output(output) {}
	};

	///List function
	class IListFunction {
	public:
		///List function
		/**
		 * @param viewHeader contains basic information about view
		 * @param rows source of rows
		 * @param context context of call
		 */
		virtual void operator()(
				const ConstValue &viewHeader,
				IVtIterator<Row> &rows,
				Context &context) = 0;
		virtual ~IListFunction() {}
	};

	///show function
	class IShowFunction {
	public:
		///Show function
		/**
		 * @param doc document to show
		 * @param context context of the call
		 */

		virtual void operator()(
				const Document &doc,
				Context &context) = 0;

		virtual ~IShowFunction() {}
	};

	///update function
	class IUpdateFunction {
	public:
		///Update function
		/**
		 * @param doc document to change. Change the document (make it dirty) and the document will be stored
		 *   (note it can be null)
		 * @param output you can generate an output here
		 * @param context context of the call
		 */
		virtual void operator()(
				Document &doc,
				Context &context) = 0;
		virtual ~IUpdateFunction() {}
	};

	///Filter function
	class IFilterFunction {
	public:
		///Filter function
		/**
		 * @param doc document
		 * @param req request
		 * @param args arguments
		 * @retval true allow document
		 * @retval false disallow document
		 */
		virtual bool operator()(
				const Document &doc,
				const ConstValue &req,
				const ConstStrA &args) = 0;
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

	typedef StringKey<StringA>  StrKey;
	typedef Map<StrKey, AllocPointer<IMapDocFn> > RegMapFn;
	typedef Map<StrKey, AllocPointer<IReduceFn> > RegReduceFn;
	typedef Map<StrKey, AllocPointer<IShowFunction> > RegShowFn;
	typedef Map<StrKey, AllocPointer<IListFunction> > RegListFn;
	typedef Map<StrKey, AllocPointer<IUpdateFunction> > RegUpdateFn;
	typedef Map<StrKey, AllocPointer<IFilterFunction> > RegFilterFn;

	RegMapFn regMap;
	RegReduceFn regReduce;
	RegReduceFn regReReduce;
	RegShowFn regShow;
	RegListFn regList;
	RegUpdateFn regUpdate;
	RegFilterFn regFilter;
	Value trueVal;

private:
	struct PreparedMap {
		IMapDocFn &fn;
		StringA args;

		PreparedMap(IMapDocFn &fn, const StringA &args):fn(fn),args(args) {}

	};
	AutoArray<PreparedMap> preparedMaps;

	ConstValue commandReset(const ConstValue &req);
	ConstValue commandAddLib(const ConstValue &req);
	ConstValue commandAddFun(const ConstValue &req);
	ConstValue commandMapDoc(const ConstValue &req);
	ConstValue commandReduce(const ConstValue &req);
	ConstValue commandReReduce(const ConstValue &req);
	ConstValue commandDDoc(const ConstValue &req, const PInOutStream &stream);

	static void splitToNameAndArgs(ConstStrA cmd, ConstStrA &name, ConstStrA &args);

};

} /* namespace LightCouch */

template<typename Fn>
inline void LightCouch::QueryServer::regMapFn(StringA name, const Fn& fn) {
	class X: public IMapDocFn {
		Fn fn;
	public:
		X(const Fn &fn):fn(fn){}
		virtual void operator()(const ConstValue &doc, const IEmitFn &emit, const ConstStrA &args) {
			fn(doc,emit,args);
		}
	};
	this->regMap(StrKey(name)) =  new X(fn);
}

template<typename Fn1, typename Fn2>
inline void LightCouch::QueryServer::regReduceFn(StringA name, const Fn1& reduce, const Fn2& rereduce) {
	class X: public IReduceFn {
		Fn1 fn;
	public:
		X(const Fn1 &fn):fn(fn){}
		virtual ConstValue operator()(const ConstValue &kvlist, const ConstStrA &args) {
			return fn(kvlist,args);
		}
	};
	class Y: public IReduceFn {
		Fn2 fn;
	public:
		Y(const Fn2 &fn):fn(fn){}
		virtual ConstValue operator()(const ConstValue &values, const ConstStrA &args) {
			return fn(values,args);
		}
	};
	this->regReduce(StrKey(name)) =  new X(reduce);
	this->regReReduce(StrKey(name)) =  new Y(rereduce);
}

template<typename Fn>
inline void LightCouch::QueryServer::regListFn(StringA name, const Fn& fn) {
	class X: public IListFunction {
		Fn fn;
	public:
		X(const Fn &fn):fn(fn){}
		virtual void operator()(
				const ConstValue &viewHeader,
				IVtIterator<Row> &rows,
				Context &context) {
			fn(viewHeader,rows,context);
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
		virtual void operator()(
				const Document &document,
				Context &context) {
			fn(document,context);
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
		virtual void operator()(
				Document &document,
				Context &context) {
			fn(document,context);
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
		virtual bool operator() (
				const Document &doc,
				const ConstValue &req,
				const ConstStrA &args) {
			fn(doc,req,args);
		}
	};
	this->regFilter(StrKey(name)) =  new X(filterFn);


}

#endif /* LIGHTCOUCH_QUERYSERVER_H_09888912AE57CCE472 */
