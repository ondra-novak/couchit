/*
 * queryServer.cpp
 *
 *  Created on: 4. 7. 2016
 *      Author: ondra
 */

#include "queryServer.h"

#include <imtjson/abstractValue.h>


#include "changeset.h"
#include "namedEnum.h"

namespace LightCouch {

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


int LightCouch::QueryServer::runDispatch(std::istream &in, std::ostream &out) {
	int r;

	while (!in.eof()) {
		Value req = Value::fromStream(in);

		try {

			Command cmd = commands[req[0].getString()];
			Value resp;
			switch (cmd) {
				case cmdReset:
					r = checkAppUpdate();
					if (r) return r;
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
			Value resp({"error",e.getType(),e.getExplain()});
			resp.toStream(out);
		} catch (VersionMistmatch &m) {
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
}

Value QueryServer::commandReset(const Value& ) {
	checkAppUpdate();
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
		StrViewA name = val.getString();
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
		StrViewA name = val.getString();

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

template<typename T>
Value createCompiledFnRef(T &fnRef) {
	typedef decltype(*fnRef) IT;
	return new FnCallValue<IT>(*fnRef);
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
			throw QueryServerError(THISLOCATION,"not_found",StringA(StrViewA("The document '")+(docid)+StrViewA("' is not cached at the query server")));
		Value fn = doc;
		Value path = req[2];
		for (std::size_t i = 0, cnt = path.size(); i < cnt; i++) {
			fn = fn[path[i].getString()];
			if (!fn.defined())
				throw QueryServerError(THISLOCATION,"not_found",StringA(StrViewA("Path not found'")+convStr(path.toString())+StrViewA("'")));
		}
		Value arguments = req[3];
		DDocCommand cmd = ddocCommands[convStr(path[0].getString())];
		Value resp;
		switch(cmd){
			case ddcmdShows: resp = commandShow(fn,arguments);break;
			case ddcmdLists: resp = commandList(fn,arguments, stream);break;
			case ddcmdFilters: resp = commandFilter(fn,arguments);break;
			case ddcmdUpdates: resp = commandUpdate(fn,arguments);break;
			case ddcmdViews: resp = commandView(fn,arguments);break;
		}
		return resp;
	}
}

Value QueryServer::commandShow(const Value& fn, const Value& args) {
	AbstractShowBase &showFn = dynamic_cast<const FnCallValue<AbstractShowBase> &>((*fn.getHandle())).getFunction();
	Value resp = showFn.run(Document(args[0]), args[1]);
	return {"resp",resp};
}

Value QueryServer::commandList(const Value& fn, const Value& args, const PInOutStream& stream) {

	class ListCtx: public IListContext {
	public:
		SeqFileInput requests;
		SeqFileOutput responses;
		ListCtx(const PInOutStream &stream, Value viewHeader)
			: requests(PInOutStream(stream)), responses(PInOutStream(stream))
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
			resp.serialize([&](char c){responses.write(c);});
			responses.write('\n');
			chunks.clear();
			Value req = Value::parse([&](){return requests.getNext();});
			if (req[0].getString() == "list_row") {
				return req[1];
			} else if (req[0].getString() == "list_end") {
				eof = true;
				return null;
			} else {
				throw QueryServerError(THISLOCATION,"protocol_error",StringA(StrViewA("Expects list_row or list_end, got:")+(convStr(req[0].getString()))));
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

	AbstractListBase &listFn = dynamic_cast<const FnCallValue<AbstractListBase> &>(*fn.getHandle()->unproxy()).getFunction();

	ListCtx listCtx(stream,args[0]);
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

void QueryServer::regView(StringA viewName, AbstractViewBase* impl) {
	views.insert(StrKey(viewName),impl);
}

void QueryServer::regList(StringA listName, AbstractListBase* impl) {
	lists.insert(StrKey(listName),impl);
}

void QueryServer::regShow(StringA showName, AbstractShowBase* impl) {
	shows.insert(StrKey(showName),impl);
}

void QueryServer::regUpdateFn(StringA updateName, AbstractUpdateFnBase* impl) {
	updates.insert(StrKey(updateName),impl);
}

void QueryServer::regFilter(StringA filterName, AbstractFilterBase* impl) {
	filters.insert(StrKey(filterName),impl);
}


Value QueryServer::createDesignDocument(Object &container, StrViewA fnName, StrViewA &suffix) {
	std::size_t pos = fnName.find('/');
	StrViewA docName;

	if (pos== ((std::size_t)-1)) {
		docName = qserverName;
		suffix = fnName;
	} else {
		docName = fnName.head(pos);
		suffix = fnName.offset(pos+1);
	}

	StrViewA strDocName(docName);
	//pick named object
	Value doc = container[strDocName];
	if (!doc.defined()) {
		Object obj;
		obj("_id",Value(String("_design/")+String(strDocName)))
			("language",convStr(qserverName));
		doc = obj;
		container.set(strDocName,doc);
		///pick name object
		doc = container[strDocName];
	}

	return doc;

}

Value createVersionedRef(StrViewA name, std::size_t ver) {
	TextFormatBuff<char, SmallAlloc<256 >> fmt;
	fmt("%1@%2") << name << ver;
	return Value(convStr(StrViewA(fmt.write())));
}

Value QueryServer::generateDesignDocuments() {
	Object ddocs;


	for(RegView::Iterator iter = views.getFwIter(); iter.hasItems();) {
		const RegView::KeyValue &kv = iter.getNext();
		StrViewA itemName;
		Value ddoc = createDesignDocument(ddocs,kv.key, itemName);
		Object ddocobj(ddoc);
		{
			auto sub1 = ddocobj.object("views");
			auto view = sub1.object(convStr(itemName));
			view("map",createVersionedRef(kv.key,kv.value->version()));
			switch (kv.value->reduceMode()) {
			case AbstractViewBase::rmNone:break;
			case AbstractViewBase::rmFunction:
				view("reduce",convStr(kv.key));break;
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

	for (RegListFn::Iterator iter = lists.getFwIter(); iter.hasItems();) {
		const RegListFn::KeyValue &kv = iter.getNext();
		StrViewA itemName;
		Value ddoc = createDesignDocument(ddocs,kv.key,itemName);
		Object e(ddoc);
		e.object("lists")(convStr(itemName), createVersionedRef(kv.key,kv.value->version()));
		ddocs.set(ddoc.getKey(),e);
	}

	for (RegShowFn::Iterator iter = shows.getFwIter(); iter.hasItems();) {
		const RegShowFn::KeyValue &kv = iter.getNext();
		StrViewA itemName;
		Value ddoc = createDesignDocument(ddocs,kv.key,itemName);
		Object e(ddoc);
		e.object("shows")(convStr(itemName), createVersionedRef(kv.key,kv.value->version()));
		ddocs.set(ddoc.getKey(),e);
	}

	for (RegUpdateFn::Iterator iter = updates.getFwIter(); iter.hasItems();) {
		const RegUpdateFn::KeyValue &kv = iter.getNext();
		StrViewA itemName;
		Value ddoc = createDesignDocument(ddocs,kv.key,itemName);
		Object e(ddoc);
		e.object("updates")(convStr(itemName), createVersionedRef(kv.key,kv.value->version()));
		ddocs.set(ddoc.getKey(),e);
	}

	for (RegFilterFn::Iterator iter = filters.getFwIter(); iter.hasItems();) {
		const RegFilterFn::KeyValue &kv = iter.getNext();
		StrViewA itemName;
		Value ddoc = createDesignDocument(ddocs,kv.key,itemName);
		Object e(ddoc);
		e.object("filters")(convStr(itemName), createVersionedRef(kv.key,kv.value->version()));
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
		couch.uploadDesignDocument(doc,updateRule);
		return false;
	});



}



QueryServerApp::QueryServerApp(StrViewA name, integer priority):LightSpeed::App(priority), QueryServer(name, ConstStrW()) {
}


void VersionMistmatch::message(ExceptionMsg& msg) const {
	msg("version mistmatch");
}

} /* namespace LightCouch */

