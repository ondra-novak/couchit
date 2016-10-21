/*
 * queryServer.cpp
 *
 *  Created on: 4. 7. 2016
 *      Author: ondra
 */

#include "queryServer.h"

#include <lightspeed/base/exceptions/httpStatusException.h>
#include <lightspeed/base/namedEnum.tcc>
#include <lightspeed/base/streams/fileiobuff.tcc>
#include <lightspeed/utils/md5iter.h>
#include <lightspeed/base/interface.tcc>
#include <lightspeed/base/containers/map.tcc>
#include <lightspeed/base/text/textFormat.tcc>
#include <immujson/abstractValue.h>


#include "changeset.h"
#include "lightspeed/mt/thread.h"
using LightSpeed::HashMD5;
using LightSpeed::HttpStatusException;
using LightSpeed::NamedEnumDef;
namespace LightCouch {

QueryServer::QueryServer(ConstStrA name, ConstStrW pathname)
		:qserverName(name),qserverPath(pathname) {
	IFileIOServices &svc = IFileIOServices::getIOServices();
	appUpdateTime = svc.getFileInfo(pathname)->getModifiedTime();
}


void QueryServer::checkAppUpdate() {
	IFileIOServices &svc = IFileIOServices::getIOServices();
	TimeStamp updated = svc.getFileInfo(qserverPath)->getModifiedTime();
	if (updated != appUpdateTime) {
		throw VersionMistmatch(THISLOCATION);
	}
}


const StringA& QueryServerError::getType() const {
	return type;
}

const StringA& QueryServerError::getExplain() const {
	return explain;
}

void QueryServerError::message(ExceptionMsg& msg) const {
	msg("QueryServer error: %1 - %2") << type << explain;
}
integer QueryServerApp::start(const Args& args) {
	integer init = initServer(args);
	if (init) return init;
	qserverPath = getAppPathname();
	return QueryServer::runDispatchStdIO();
}

integer QueryServer::runDispatchStdIO() {



	IFileIOServices &svc = IFileIOServices::getIOServices();
	PInOutStream stream = new IOBuffer<256*1024>(new SeqBidirStream(
			svc.openStdFile(IFileIOServices::stdInput).get(),
			svc.openStdFile(IFileIOServices::stdOutput).get()));
	runDispatch(stream);

	return 0;
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


void LightCouch::QueryServer::runDispatch(PInOutStream stream) {
	SeqFileInput requests(stream);
	SeqFileOutput responses(stream);
	auto reqInput = [&](){return requests.getNext();};
	auto resOutput = [&](char c){responses.write(c);};

	while (requests.hasItems()) {
		Value req = Value::parse(reqInput);

		try {

			Command cmd = commands[StringRef(req[0].getString())];
			Value resp;
			switch (cmd) {
				case cmdReset: resp=commandReset(req);break;
				case cmdAddLib: resp=commandAddLib(req);break;
				case cmdAddFun: resp=commandAddFun(req);break;
				case cmdMapDoc: resp=commandMapDoc(req);break;
				case cmdReduce: resp=commandReduce(req);break;
				case cmdReReduce: resp=commandReReduce(req);break;
				case cmdDDoc: resp=commandDDoc(req,stream);break;
			}
			resp.serialize(resOutput);
		} catch (QueryServerError &e) {
			Value out({"error",StringRef(e.getType()),StringRef(e.getExplain())});
			out.serialize(resOutput);
		} catch (VersionMistmatch &m) {
			Value out({"error","try_again","restarting query server, please try again"});
			out.serialize(resOutput);
			responses.write('\n');
			responses.flush();
			throw;
		} catch (std::exception &e) {
			Value out({"error","internal_error",e.what()});
			out.serialize(resOutput);
		}
		responses.write('\n');

	}
}

Value QueryServer::commandReset(const Value& ) {
	checkAppUpdate();
	preparedMaps.clear();
	return true;
}

Value QueryServer::commandAddLib(const Value& ) {
	throw QueryServerError(THISLOCATION,"unsupported","You cannot add lib to native C++ query server");
}

static natural extractVersion(ConstStrA &name) {
	natural versep = name.find('@');
	if (versep == naturalNull)
		throw QueryServerError(THISLOCATION,"no_version_defined", "View definition must contain version marker @version");
	ConstStrA ver = name.offset(versep+1);
	name = name.head(versep);
	natural verid;
	if (!parseUnsignedNumber(ver.getFwIter(), verid,10))
		throw QueryServerError(THISLOCATION,"invald version", "Version must be number");
	return verid;

}

Value QueryServer::commandAddFun(const Value& req) {
	ConstStrA name = StringRef(req[1].getString());
	natural verId = extractVersion(name);

	const AllocPointer<AbstractViewBase>  *fn = views.find(StrKey(name));
	if (fn == 0) {
		throw QueryServerError(THISLOCATION,"not_found",StringA(ConstStrA("Map Function '")+name+ConstStrA("' not found")));
	}
	if ((*fn)->version() != verId)
		throw VersionMistmatch(THISLOCATION);
	preparedMaps.add(PreparedMap(*(*fn)));
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

	for (natural i = 0; i < preparedMaps.length(); i++) {
		Array subres;
		Emit emit(subres);
		preparedMaps[i].fn.map(doc,emit);
		result.add(Value(subres));
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
		rowBuffer.add(AbstractViewBase::Row(docId,key,valPart));
	}

	for (auto &&val: fnlist) {
		ConstStrA name = StringRef(val.getString());
		const AllocPointer<AbstractViewBase>  *fn = views.find(StrKey(name));
		if (fn == 0 ) {
			throw QueryServerError(THISLOCATION,"not_found",StringA(ConstStrA("Reduce Function '")+name+ConstStrA("' not found")));
		}
		AbstractViewBase &view = *(*fn);
		if (view.reduceMode() != AbstractViewBase::rmFunction) {
			throw QueryServerError(THISLOCATION,"invalid_view",StringA(ConstStrA("Specified view '")+name+ConstStrA("' doesn't define reduce function")));
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
		valueBuffer.add(AbstractViewBase::ReducedRow(item));
	}

	for (auto &&val: fnlist) {
		ConstStrA name = StringRef(val.getString());

		const AllocPointer<AbstractViewBase>  *fn = views.find(StrKey(name));
		if (fn == 0 ) {
			throw QueryServerError(THISLOCATION,"not_found",StringA(ConstStrA("Reduce Function '")+name+ConstStrA("' not found")));
		}
		AbstractViewBase &view = *(*fn);
		if (view.reduceMode() != AbstractViewBase::rmFunction) {
			throw QueryServerError(THISLOCATION,"invalid_view",StringA(ConstStrA("Specified view '")+name+ConstStrA("' doesn't define reduce function")));
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

	if (vshows != null) {
		compiled.set("shows",compileDesignSection(this->shows,vshows,"shows"));
	}
	if (vlists != null) {
		compiled.set("lists",compileDesignSection(this->lists,vlists,"lists"));
	}
	if (vupdates != null) {
		compiled.set("updates",compileDesignSection(this->updates,vupdates,"updates"));
	}
	if (vfilters != null) {
		compiled.set("filters",compileDesignSection(this->filters,vfilters,"filters"));
	}
	if (vviews != null) {
		compiled.set("views",compileDesignSection(this->views,vviews,"views"));
	}
	return compiled;
}

template<typename T>
Value createCompiledFnRef(T &fnRef) {
	return new FnCallValue<typename T::ItemT>(*fnRef);
}

template<typename T>
Value QueryServer::compileDesignSection(T &reg, const Value &section, ConstStrA sectionName) {

	Object out;
	section.forEach([&](Value value){
		ConstStrA itemname = StringRef(value.getKey());
		bool inmap = false;
		if (value.type() == json::object) {
			value = value["map"];
			if (!value.defined()) return false;
			inmap = true;
		}
		ConstStrA name = StringRef(value.getString());
		natural verId = extractVersion(name);

		auto fnptr = reg.find(StrKey(name));
		if (fnptr == 0) {
			throw QueryServerError(THISLOCATION,"not_found",StringA(ConstStrA("Function '")+name+ConstStrA("' in section '")+sectionName+ConstStrA("' not found")));
		}

		if ((*fnptr)->version() != verId) {
			throw VersionMistmatch(THISLOCATION);
		}

		Value compiled = createCompiledFnRef(*fnptr);
		if (inmap) compiled = Object("map", compiled);
		out.set(StringRef(itemname), compiled);
		return false;
	});
	return out;

}


Value QueryServer::commandDDoc(const Value& req, const PInOutStream& stream) {
	StringRef docid = req[1].getString();
	if (docid == "new") {
		//cache new document

		StringRef docid = req[2].getString();
		Value ddoc = req[3];

		Value compiledDDoc = compileDesignDocument(ddoc);
		ddcache.set(docid, compiledDDoc);
		return true;
	} else {
		Value doc = ddcache[docid];
		if (!doc.defined())
			throw QueryServerError(THISLOCATION,"not_found",StringA(ConstStrA("The document '")+ConstStrA(docid)+ConstStrA("' is not cached at the query server")));
		Value fn = doc;
		Value path = req[2];
		for (natural i = 0, cnt = path.size(); i < cnt; i++) {
			fn = fn[path[i].getString()];
			if (!fn.defined())
				throw QueryServerError(THISLOCATION,"not_found",StringA(ConstStrA("Path not found'")+ConstStrA(StringRef(path.toString()))+ConstStrA("'")));
		}
		Value arguments = req[3];
		DDocCommand cmd = ddocCommands[StringRef(path[0].getString())];
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
				throw QueryServerError(THISLOCATION,"protocol_error",StringA(ConstStrA("Expects list_row or list_end, got:")+req[0].getString()));
			}
		}

		Value finish() {
			//skip rows if we did not processed them yet
			while (!eof) {
				getRow();
			}
			return {"end",chunks};
		}

		virtual void send(StringRef text) {
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

	AbstractListBase &listFn = dynamic_cast<const FnCallValue<AbstractListBase> &>(*fn.getHandle()).getFunction();

	ListCtx listCtx(stream,args[0]);
	listFn.run(listCtx,args[1]);
	return listCtx.finish();


}

Value QueryServer::commandUpdate(const Value& fn, const Value& args) {
	AbstractUpdateFnBase &updateFn = dynamic_cast<const FnCallValue<AbstractUpdateFnBase> &>(*fn.getHandle()).getFunction();
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
	AbstractViewBase &mapDocFn = dynamic_cast<const FnCallValue<AbstractViewBase> &>(*fn.getHandle()).getFunction();

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
	AbstractFilterBase &filterFn = dynamic_cast<const FnCallValue<AbstractFilterBase> &>(*fn.getHandle()).getFunction();

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


Value QueryServer::createDesignDocument(Object &container, ConstStrA fnName, ConstStrA &suffix) {
	natural pos = fnName.find('/');
	ConstStrA docName;

	if (pos== naturalNull) {
		docName = qserverName;
		suffix = fnName;
	} else {
		docName = fnName.head(pos);
		suffix = fnName.offset(pos+1);
	}

	StringRef strDocName(docName);
	//pick named object
	Value doc = container[strDocName];
	if (!doc.defined()) {
		Object obj;
		obj("_id",Value(String("_design/")+String(strDocName)))
			("language",StringRef(qserverName));
		doc = obj;
		container.set(strDocName,doc);
		///pick name object
		doc = container[strDocName];
	}

	return doc;

}

Value createVersionedRef(ConstStrA name, natural ver) {
	TextFormatBuff<char, SmallAlloc<256 >> fmt;
	fmt("%1@%2") << name << ver;
	return Value(StringRef(fmt.write()));
}

Value QueryServer::generateDesignDocuments() {
	Object ddocs;


	for(RegView::Iterator iter = views.getFwIter(); iter.hasItems();) {
		const RegView::KeyValue &kv = iter.getNext();
		ConstStrA itemName;
		Value ddoc = createDesignDocument(ddocs,kv.key, itemName);
		Object ddocobj(ddoc);
		{
			auto sub1 = ddocobj.object("views");
			auto view = sub1.object(StringRef(itemName));
			view("map",createVersionedRef(kv.key,kv.value->version()));
			switch (kv.value->reduceMode()) {
			case AbstractViewBase::rmNone:break;
			case AbstractViewBase::rmFunction:
				view("reduce",StringRef(kv.key));break;
			case AbstractViewBase::rmSum:
				view("reduce","_sum");break;
			case AbstractViewBase::rmCount:
				view("reduce","_count");break;
			case AbstractViewBase::rmStats:
				view("reduce","_stats");break;
			}
		}
		ddocs.set(StringRef(ddoc.getKey()), ddocobj);
	}

	for (RegListFn::Iterator iter = lists.getFwIter(); iter.hasItems();) {
		const RegListFn::KeyValue &kv = iter.getNext();
		ConstStrA itemName;
		Value ddoc = createDesignDocument(ddocs,kv.key,itemName);
		ddocs.set(StringRef(ddoc.getKey()),
				Object(ddoc).object("lists")
						(StringRef(itemName), createVersionedRef(kv.key,kv.value->version())));
	}

	for (RegShowFn::Iterator iter = shows.getFwIter(); iter.hasItems();) {
		const RegShowFn::KeyValue &kv = iter.getNext();
		ConstStrA itemName;
		Value ddoc = createDesignDocument(ddocs,kv.key,itemName);
		ddocs.set(StringRef(ddoc.getKey()),
				Object(ddoc).object("shows")
					(StringRef(itemName), createVersionedRef(kv.key,kv.value->version())));
	}

	for (RegUpdateFn::Iterator iter = updates.getFwIter(); iter.hasItems();) {
		const RegUpdateFn::KeyValue &kv = iter.getNext();
		ConstStrA itemName;
		Value ddoc = createDesignDocument(ddocs,kv.key,itemName);
		ddocs.set(StringRef(ddoc.getKey()),
				Object(ddoc).object("updates")
					(StringRef(itemName), createVersionedRef(kv.key,kv.value->version())));
	}

	for (RegFilterFn::Iterator iter = filters.getFwIter(); iter.hasItems();) {
		const RegFilterFn::KeyValue &kv = iter.getNext();
		ConstStrA itemName;
		Value ddoc = createDesignDocument(ddocs,kv.key,itemName);
		ddocs.set(StringRef(ddoc.getKey()),
				Object(ddoc).object("filters")
					(StringRef(itemName), createVersionedRef(kv.key,kv.value->version())));
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



QueryServerApp::QueryServerApp(ConstStrA name, integer priority):LightSpeed::App(priority), QueryServer(name, ConstStrW()) {
}


void VersionMistmatch::message(ExceptionMsg& msg) const {
	msg("version mistmatch");
}

} /* namespace LightCouch */

