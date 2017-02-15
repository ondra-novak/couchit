/*
 * queryServer.cpp
 *
 *  Created on: 4. 7. 2016
 *      Author: ondra
 */

#include "queryServer.h"

#include <sys/types.h>
#include <sys/stat.h>

#include <imtjson/abstractValue.h>


#include "changeset.h"
#include "namedEnum.h"
#include "num2str.h"

namespace couchit {

QueryServer::QueryServer(const StrViewA &name):qserverName(name) {}



int QueryServer::runDispatchStdIO() {

	return runDispatch(std::cin, std::cout);
}

enum Command {
	cmdReset,
	cmdAddLib,
	cmdAddFun,
	cmdMapDoc,
	cmdReduce,
	cmdReReduce,
	cmdDDoc,
};

enum DDocCommand {

	ddcmdShows,
	ddcmdLists,
	ddcmdUpdates,
	ddcmdFilters,
	ddcmdViews,
};

static NamedEnumDef<Command> commandsDef[] = {
		{cmdReset, "reset"},
		{cmdAddLib, "add_lib"},
		{cmdAddFun, "add_fun"},
		{cmdMapDoc, "map_doc"},
		{cmdReduce, "reduce"},
		{cmdReReduce, "rereduce"},
		{cmdDDoc, "ddoc"}
};
static NamedEnum<Command> commands(commandsDef);

static NamedEnumDef<DDocCommand> ddocCommandsDef[] = {
		{ddcmdShows,"shows"},
		{ddcmdLists,"lists"},
		{ddcmdUpdates,"updates"},
		{ddcmdFilters,"filters"},
		{ddcmdViews,"views"}
};

static NamedEnum<DDocCommand> ddocCommands(ddocCommandsDef);


int couchit::QueryServer::runDispatch(std::istream &in, std::ostream &out) {

	while (!in.eof()) {
		Value req = Value::fromStream(in);

		try {

			Command cmd = commands[req[0].getString()];
			Value resp;
			switch (cmd) {
				case cmdReset:
					if (rrule != nullptr && rrule())
						return 100;
					resp=commandReset(req);
					break;
				case cmdAddLib: resp=commandAddLib(req);break;
				case cmdAddFun: resp=commandAddFun(req);break;
				case cmdMapDoc: resp=commandMapDoc(req);break;
				case cmdReduce: resp=commandReduce(req);break;
				case cmdReReduce: resp=commandReReduce(req);break;
				case cmdDDoc: resp=commandDDoc(req,in,out);break;
			}
			resp.toStream(out);
		} catch (QueryServerError &e) {
			Value resp({"error",e.type,e.explain});
			resp.toStream(out);
		} catch (VersionMistmatch &) {
			Value resp({"error","try_again","restarting query server, please try again"});
			resp.toStream(out);
			out << std::endl;
			throw;
		} catch (std::exception &e) {
			Value resp({"error","internal_error",e.what()});
			resp.toStream(out);
		}
		out << std::endl;

	}
	return 0;
}

Value QueryServer::commandReset(const Value& ) {
	preparedMaps.clear();
	return true;
}

Value QueryServer::commandAddLib(const Value& ) {
	throw QueryServerError("unsupported","You cannot add lib to native C++ query server");
}

static std::pair<StrViewA,std::size_t> extractVersion(const StrViewA &name) {
	std::size_t versep = name.indexOf("@",0);
	if (versep == ((std::size_t)-1))
		throw QueryServerError("no_version_defined", "View definition must contain version marker @version");
	StrViewA ver = name.substr(versep+1);
	StrViewA rawname = name.substr(0,versep);
	Value verid = Value::fromString(ver);
	if (verid.type() != json::number) {
		throw QueryServerError("invald version", "Version must be number");
	}
	return std::make_pair(rawname,verid.getUInt());

}

Value QueryServer::commandAddFun(const Value& req) {
	auto vinfo = extractVersion(req[1].getString());

	auto fniter = views.find(vinfo.first);
	if (fniter == views.end()) {
		throw QueryServerError("not_found",String({"Map Function '",vinfo.first,"' not found"}));
	}
	if (fniter->second->version() != vinfo.second)
		throw VersionMistmatch();
	preparedMaps.push_back(fniter->second);
	return true;
}

Value QueryServer::commandMapDoc(const Value& req) {

	class Emit: public IEmitFn {
	public:
		Array &container;
		Emit(Array &container):container(container) {}

		virtual void operator()() override {
			container.add( {nullptr,nullptr} );
		}

		virtual void operator()(const Value &key) override {
			container.add( {key,nullptr} );
		}

		virtual void operator()(const Value &key, const Value &value) override {
			container.add( {key,value} );
		}
	};

	Document doc = req[1];
	Array result;
	Array subres;

	for (auto &&x : preparedMaps) {
		subres.clear();
		Emit emit(subres);
		x->map(doc,emit);
		result.push_back(subres);
	}

	return result;

}


Value QueryServer::commandReduce(const Value& req) {
	Value fnlist = req[1];
	Value values = req[2];
	Array output;
	rowBuffer.clear();

	for (auto &&val : values) {
		Value keyPart = val[0];
		Value key = keyPart[0];
		Value docId = keyPart[1];
		Value valPart = val[1];
		rowBuffer.push_back(RowWithKey(docId,key,valPart));
	}

	for (auto &&val: fnlist) {
		StrViewA name = extractVersion(val.getString()).first;
		auto fniter = views.find(name);
		if (fniter == views.end() ) {
			throw QueryServerError("not_found",String({"Reduce Function '",name,"' not found"}));
		}
		AbstractViewBase &view = *fniter->second;
		if (view.reduceMode() != AbstractViewBase::rmFunction) {
			throw QueryServerError("invalid_view",String({"Specified view '",name,"' doesn't define reduce function"}));
		}
		output.add(view.reduce(rowBuffer));
	}
	rowBuffer.clear();
	return {true,output};
}
Value QueryServer::commandReReduce(const Value& req) {
	Value fnlist = req[1];
	Value values = req[2];
	Array output;
	valueBuffer.clear();

	for(auto &&item : values) {
		valueBuffer.push_back(ReducedRow(item));
	}

	for (auto &&val: fnlist) {
		StrViewA name = extractVersion(val.getString()).first;

		auto fniter = views.find(name);
		if (fniter == views.end() ) {
			throw QueryServerError("not_found",String({"Reduce Function '",name,"' not found"}));
		}
		AbstractViewBase &view = *fniter->second;
		if (view.reduceMode() != AbstractViewBase::rmFunction) {
			throw QueryServerError("invalid_view",String({"Specified view '",name,"' doesn't define reduce function"}));
		}
		output.add(view.rereduce(valueBuffer));
	}
	rowBuffer.clear();
	return {true,output};
}



template<typename Ifc>
class FnCallValue: public json::AbstractValue {
public:
	FnCallValue(Ifc &ifc):ifc(ifc) {}

	Ifc &getFunction() const {return ifc;}
	json::ValueType type() const {return json::null;}

protected:
	Ifc &ifc;

};


Value QueryServer::compileDesignDocument(const Value &document) {
	Value vshows = document["shows"];
	Value vlists = document["lists"];
	Value vupdates = document["updates"];
	Value vfilters = document["filters"];
	Value vviews = document["views"];
	Object compiled;

	if (vshows.defined()) {
		compiled.set("shows",compileDesignSection(this->shows,vshows,"shows"));
	}
	if (vlists.defined()) {
		compiled.set("lists",compileDesignSection(this->lists,vlists,"lists"));
	}
	if (vupdates.defined()) {
		compiled.set("updates",compileDesignSection(this->updates,vupdates,"updates"));
	}
	if (vfilters.defined()) {
		compiled.set("filters",compileDesignSection(this->filters,vfilters,"filters"));
	}
	if (vviews.defined()) {
		compiled.set("views",compileDesignSection(this->views,vviews,"views"));
	}
	return compiled;
}

template<typename T> struct RemoveRef { typedef T Type; };
template<typename T> struct RemoveRef<T &> { typedef T Type; };
template<typename T> struct RemoveRef<const T &> { typedef T Type; };
template<typename T> struct RemoveRef<T &&> { typedef T Type; };
template<typename T> struct RemoveRef<const T &&> { typedef T Type; };

template<typename T>
Value createCompiledFnRef(T &fnRef) {
	typedef decltype(*fnRef) IT;
	return new FnCallValue<typename RemoveRef<IT>::Type>(*fnRef);
}



template<typename T>
Value QueryServer::compileDesignSection(T &reg, const Value &section, StrViewA sectionName) {

	Object out;
	for(auto value: section){
		StrViewA itemname = value.getKey();
		bool inmap = false;
		if (value.type() == json::object) {
			value = value["map"];
			if (!value.defined()) return false;
			inmap = true;
		}
		auto verId = extractVersion(value.getString());

		auto fnptr = reg.find(StrKey(StrViewA(verId.first)));
		if (fnptr == reg.end()) {
			throw QueryServerError("not_found",String({"Function '",verId.first,"' in section '",sectionName,"' not found"}));
		}

		if (fnptr->second->version() != verId.second) {
			throw VersionMistmatch();
		}

		Value compiled = createCompiledFnRef(fnptr->second);
		if (inmap) {
			Object o;
			o.set("map", createCompiledFnRef(fnptr->second));
			compiled = o;
		} else{
			compiled = createCompiledFnRef(fnptr->second);
		}
		out.set(itemname, compiled);
	}
	return out;

}


Value QueryServer::commandDDoc(const Value& req, std::istream &input, std::ostream &output) {
	StrViewA docid = req[1].getString();
	if (docid == "new") {
		//cache new document

		StrViewA docid = req[2].getString();
		Value ddoc = req[3];

		Value compiledDDoc = compileDesignDocument(ddoc);
		ddcache.set(docid, compiledDDoc);
		return true;
	} else {
		Value doc = ddcache[docid];
		if (!doc.defined())
			throw QueryServerError("not_found",{"The document '",docid,"' is not cached at the query server"});
		Value fn = doc;
		Value path = req[2];
		for (std::size_t i = 0, cnt = path.size(); i < cnt; i++) {
			fn = fn[path[i].getString()];
			if (!fn.defined())
				QueryServerError("not_found",{"Path not found'",path.toString(),"'"});
		}
		Value arguments = req[3];
		DDocCommand cmd = ddocCommands[path[0].getString()];
		Value resp;
		try {
			switch(cmd){
				case ddcmdShows: resp = commandShow(fn,arguments);break;
				case ddcmdLists: resp = commandList(fn, arguments, input, output);break;
				case ddcmdFilters: resp = commandFilter(fn,arguments);break;
				case ddcmdUpdates: resp = commandUpdate(fn,arguments);break;
				case ddcmdViews: resp = commandView(fn,arguments);break;
			}
			return resp;
		} catch (std::bad_cast) {
			throw QueryServerError("compile error",{"Error to execute compiled function: ", typeid(*fn.getHandle()->unproxy()).name()});
		}
	}
}

Value QueryServer::commandShow(const Value& fn, const Value& args) {
	AbstractShowBase &showFn = dynamic_cast<const FnCallValue<AbstractShowBase> &>((*fn.getHandle()->unproxy())).getFunction();
	Value resp = showFn.run(Document(args[0]), args[1]);
	return {"resp",resp};
}

Value QueryServer::commandList(const Value& fn, const Value& args, std::istream &input, std::ostream &output) {

	class ListCtx: public IListContext {
	public:
		std::istream &requests;
		std::ostream &responses;
		ListCtx(std::istream &requests,std::ostream &responses, Value viewHeader)
			: requests(requests), responses(responses)
			,viewHeader(viewHeader)
			,eof(false),headerSent(false) {
		}
		virtual Value getRow() {
			if (eof) return null;
			Value resp;
			if (!headerSent) {
				if (!headerObj.defined()) headerObj = json::object;
				resp = { "start" ,chunks ,headerObj};
				headerSent = true;
			} else {
				resp = {"chunks" ,chunks};
			}
			resp.toStream(responses);
			responses.put('\n');
			chunks.clear();
			Value req = Value::fromStream(requests);

			if (req[0].getString() == "list_row") {
				return req[1];
			} else if (req[0].getString() == "list_end") {
				eof = true;
				return null;
			} else {
				throw QueryServerError("protocol_error",{"Expects list_row or list_end, got:",req[0].getString()});
			}
		}

		Value finish() {
			//skip rows if we did not processed them yet
			while (!eof) {
				getRow();
			}
			return {"end",chunks};
		}

		virtual void send(StrViewA text) {
			chunks.add(text);
		}
		virtual void send(Value jsonValue) {
			chunks.add(jsonValue.stringify());
		}
		virtual Value getViewHeader() const {
			return viewHeader;
		}
		virtual void start(Value headerObj) {
			this->headerObj = headerObj;
		}

	protected:
		Array chunks;
		Value headerObj;
		Value viewHeader;
		bool eof;
		bool headerSent;
	};

	const json::IValue *v = fn.getHandle()->unproxy();
	AbstractListBase &listFn = dynamic_cast<const FnCallValue<AbstractListBase> &>(*v).getFunction();

	ListCtx listCtx(input,output,args[0]);
	listFn.run(listCtx,args[1]);
	return listCtx.finish();


}

Value QueryServer::commandUpdate(const Value& fn, const Value& args) {
	AbstractUpdateFnBase &updateFn = dynamic_cast<const FnCallValue<AbstractUpdateFnBase> &>(*fn.getHandle()->unproxy()).getFunction();
	Document doc(args[0]);
	Value response = updateFn.run(doc, args[1]);
	if (doc.dirty()) {
		return {"up" ,doc,response};
	} else {
		return {"up" ,nullptr ,response};
	}

}

Value QueryServer::commandView(const Value& fn,
		const Value& args) {
	AbstractViewBase &mapDocFn = dynamic_cast<const FnCallValue<AbstractViewBase> &>(*fn.getHandle()->unproxy()).getFunction();

	class FakeEmit: public IEmitFn {
	public:
		bool result;
		FakeEmit() {result = false;}
		virtual void operator()(const Value &, const Value &) {
			result = true;
		}
		virtual void operator()(const Value &) {
			result = true;
		}
		virtual void operator()() {
			result = true;
		}
	};

	Value docs= args[0];
	Array results;
	docs.forEach([&](const Value &v){
		FakeEmit emit;
		mapDocFn.map(Document(v), emit);
		results.add(emit.result);
		return true;
	});

	return {true,results};
}

Value QueryServer::commandFilter(const Value& fn, const Value& args) {
	AbstractFilterBase &filterFn = dynamic_cast<const FnCallValue<AbstractFilterBase> &>(*fn.getHandle()->unproxy()).getFunction();

	Value docs= args[0];
	Array results;
	docs.forEach([&](const Value &doc) {
		results.add(filterFn.run(Document(doc), args[1]));
		return true;
	});

	return {true,results};
}

void QueryServer::regView(String viewName, AbstractViewBase* impl) {
	views.insert(std::make_pair(StrKey(viewName),impl));
}

void QueryServer::regList(String listName, AbstractListBase* impl) {
	lists.insert(std::make_pair(StrKey(listName),impl));
}

void QueryServer::regShow(String showName, AbstractShowBase* impl) {
	shows.insert(std::make_pair(StrKey(showName),impl));
}

void QueryServer::regUpdateFn(String updateName, AbstractUpdateFnBase* impl) {
	updates.insert(std::make_pair(StrKey(updateName),impl));
}

void QueryServer::regFilter(String filterName, AbstractFilterBase* impl) {
	filters.insert(std::make_pair(StrKey(filterName),impl));
}

void QueryServer::setRestartRule(const RestartRule& rule) {
	rrule = rule;
}


Value QueryServer::createDesignDocument(Object &container, StrViewA fnName, StrViewA &suffix) {
	std::size_t pos = fnName.indexOf("/",0);
	StrViewA docName;

	if (pos== ((std::size_t)-1)) {
		docName = qserverName;
		suffix = fnName;
	} else {
		docName = fnName.substr(0,pos);
		suffix = fnName.substr(pos+1);
	}

	StrViewA strDocName(docName);
	//pick named object
	Value doc = container[strDocName];
	if (!doc.defined()) {
		Object obj;
		obj("_id",Value(String("_design/")+String(strDocName)))
			("language",qserverName);
		doc = obj;
		container.set(strDocName,doc);
		///pick name object
		doc = container[strDocName];
	}

	return doc;

}

Value createVersionedRef(StrViewA name, std::size_t ver) {
	return String(21+name.length,[&](char *c) {
		char *s = c;
		for (char x: name) *c++ = x;
		*c++='@';
		unsignedToString([&](char x){*c++=x;},ver,20,10);
		return c-s;
	});
}

Value QueryServer::generateDesignDocuments() {
	Object ddocs;
	for (auto &&kv : views) {
		StrViewA itemName;
		Value ddoc = createDesignDocument(ddocs,kv.first, itemName);
		Object ddocobj(ddoc);
		{
			auto sub1 = ddocobj.object("views");
			auto view = sub1.object(itemName);
			view("map",createVersionedRef(kv.first,kv.second->version()));
			switch (kv.second->reduceMode()) {
			case AbstractViewBase::rmNone:break;
			case AbstractViewBase::rmFunction:
				view("reduce", createVersionedRef(kv.first, kv.second->version()));break;
			case AbstractViewBase::rmSum:
				view("reduce","_sum");break;
			case AbstractViewBase::rmCount:
				view("reduce","_count");break;
			case AbstractViewBase::rmStats:
				view("reduce","_stats");break;
			}
		}
		ddocs.set(StrViewA(ddoc.getKey()), ddocobj);
	}

	for (auto &&kv : lists) {
		StrViewA itemName;
		Value ddoc = createDesignDocument(ddocs,kv.first,itemName);
		Object e(ddoc);
		e.object("lists")(itemName, createVersionedRef(kv.first,kv.second->version()));
		ddocs.set(ddoc.getKey(),e);
	}

	for (auto &&kv : shows) {
		StrViewA itemName;
		Value ddoc = createDesignDocument(ddocs,kv.first,itemName);
		Object e(ddoc);
		e.object("shows")(itemName, createVersionedRef(kv.first,kv.second->version()));
		ddocs.set(ddoc.getKey(),e);
	}

	for (auto &&kv : updates) {
		StrViewA itemName;
		Value ddoc = createDesignDocument(ddocs,kv.first,itemName);
		Object e(ddoc);
		e.object("updates")(itemName, createVersionedRef(kv.first,kv.second->version()));
		ddocs.set(ddoc.getKey(),e);
	}

	for (auto &&kv : filters) {
		StrViewA itemName;
		Value ddoc = createDesignDocument(ddocs,kv.first,itemName);
		Object e(ddoc);
		e.object("filters")(itemName, createVersionedRef(kv.first,kv.second->version()));
		ddocs.set(ddoc.getKey(),e);
	}
	Value ddocv = ddocs;
	Array output;

	ddocv.forEach([&](const Value doc) {
		output.add(doc);
		return true;

	});


	return output;
}


void QueryServer::syncDesignDocuments(Value designDocuments, CouchDB& couch, CouchDB::DesignDocUpdateRule updateRule) {

	Changeset chset = couch.createChangeset();

	designDocuments.forEach([&](Value doc) {
		couch.putDesignDocument(doc,updateRule);
		return false;
	});

}

String QueryServerError::getWhatMsg() const throw() {
	return String({"Query server error: ", type,": ",explain});
}

String VersionMistmatch::getWhatMsg() const throw() {
	return String("Version mistmatch.");
}


RestartRuleChangedFile::RestartRuleChangedFile(const String fname):file(fname),mtime(getMTime(fname)) {
}

bool RestartRuleChangedFile::operator ()() const {
	std::time_t m = getMTime(file);
	return m != mtime? 1:0;
}

std::time_t RestartRuleChangedFile::getMTime(const String& file) {
#ifdef _WIN32
	struct _stat st;
	st.st_mtime = 0;
	_stat(file.c_str(), &st);
	return st.st_mtime;

#else
	struct stat st;
	st.st_mtim.tv_sec = 0;
	stat(file.c_str(), &st);
	return st.st_mtim.tv_sec;
#endif
}

} /* namespace couchit */
