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
#include <lightspeed/utils/json/jsonimpl.h>
#include <lightspeed/utils/json/jsonserializer.tcc>
#include <lightspeed/utils/md5iter.h>
#include <lightspeed/base/interface.tcc>
#include <lightspeed/base/containers/map.tcc>


#include "changeset.h"
using LightSpeed::HashMD5;
using LightSpeed::HttpStatusException;
using LightSpeed::JSON::Null;
using LightSpeed::NamedEnumDef;
namespace LightCouch {

QueryServer::QueryServer() {}
QueryServer::QueryServer(ConstStrA name):qserverName(name) {}



const StringA& QueryServer::Error::getType() const {
	return type;
}

const StringA& QueryServer::Error::getExplain() const {
	return explain;
}

void QueryServer::Error::message(ExceptionMsg& msg) const {
	msg("QueryServer error: %1 - %2") << type << explain;
}
integer QueryServerApp::start(const Args& args) {
	integer init = initServer(args);
	if (init) return init;
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
	JSON::PFactory factory = JSON::create();
	this->json = Json(factory);
	trueVal = json(true);

	while (requests.hasItems()) {
		JSON::Value req = factory->fromStream(requests);

		try {

			Command cmd = commands[req[0].getStringA()];
			ConstValue resp;
			switch (cmd) {
				case cmdReset: resp=commandReset(req);break;
				case cmdAddLib: resp=commandAddLib(req);break;
				case cmdAddFun: resp=commandAddFun(req);break;
				case cmdMapDoc: resp=commandMapDoc(req);break;
				case cmdReduce: resp=commandReduce(req);break;
				case cmdReReduce: resp=commandReReduce(req);break;
				case cmdDDoc: resp=commandDDoc(req,stream);break;
			}
			factory->toStream(*resp, responses);
		} catch (Error &e) {
			JSON::Value out = json << "error" << e.getType() << e.getExplain();
			factory->toStream(*out, responses);
		} catch (VersionMistmatch &m) {
			JSON::Value out = json << "error" << "version_mistmatch" << "restarting query server, please try again";
			factory->toStream(*out, responses);
			responses.flush();
			throw;
		} catch (std::exception &e) {
			JSON::Value out = json << "error" << "internal_error" << e.what();
			factory->toStream(*out,responses);
		}
		responses.write('\n');

	}
}

ConstValue QueryServer::commandReset(const ConstValue& ) {
	preparedMaps.clear();
	return trueVal;
}

ConstValue QueryServer::commandAddLib(const ConstValue& ) {
	throw Error(THISLOCATION,"unsupported","You cannot add lib to native C++ query server");
}

ConstValue QueryServer::commandAddFun(const ConstValue& req) {
	ConstStrA name = req[1].getStringA();
	natural versep = name.find('@');
	if (versep == naturalNull)
		throw Error(THISLOCATION,"no_version_defined", "View definition must contain version marker @version");
	ConstStrA ver = name.offset(versep+1);
	name = name.head(versep);
	natural verid;
	if (!parseUnsignedNumber(ver.getFwIter(), verid,10))
		throw Error(THISLOCATION,"invald version", "Version must be number");


	const AllocPointer<IMapDocFn>  *fn = regMap.find(StrKey(name));
	if (fn == 0) {
		throw Error(THISLOCATION,"not_found",StringA(ConstStrA("Map Function '")+name+ConstStrA("' not found")));
	}
	if ((*fn)->getVersion() != verid)
		throw VersionMistmatch(THISLOCATION);
	preparedMaps.add(PreparedMap(*(*fn)));
	return trueVal;
}

ConstValue QueryServer::commandMapDoc(const ConstValue& req) {

	class Emit: public IEmitFn {
	public:
		Container &container;
		Json &json;
		Emit(Container &container,Json &json):container(container),json(json) {}
		virtual void operator()(const ConstValue &key, const ConstValue &value) {
			if (key == null) {
				operator()(json(null),value);
			} else if (value == null) {
				operator()(key,json(null));
			} else {
				container.add(json << key << value);
			}
		}
	};

	Document doc = req[1];
	Container result = json.array();

	for (natural i = 0; i < preparedMaps.length(); i++) {
		Container subres = json.array();
		Emit emit(subres,json);
		preparedMaps[i].fn(doc,emit);
		result.add(subres);
	}

	return result;

}

ConstValue QueryServer::commandReduceGen(const ConstValue& req, RegReduceFn &regReduce) {
	ConstValue fnlist = req[1];
	ConstValue values = req[2];
	Container output = json.array();;
	for (natural i = 0, cnt = fnlist.length(); i < cnt; i++) {
		ConstStrA def = fnlist[i].getStringA();
		ConstStrA name, args;
		splitToNameAndArgs(def,name,args);
		const AllocPointer<IReduceFn>  *fn = regReduce.find(StrKey(name));
		if (fn == 0) {
			throw Error(THISLOCATION,"not_found",StringA(ConstStrA("Reduce Function '")+name+ConstStrA("' not found")));
		}
		IReduceFn &reduce = *(*fn);
		output.add(reduce(values));
	}
	return json << true << output;
}

ConstValue QueryServer::commandReduce(const ConstValue& req) {
	return commandReduceGen(req, regReduce);
}
ConstValue QueryServer::commandReReduce(const ConstValue& req) {
	return commandReduceGen(req, regReReduce);
}


void QueryServer::splitToNameAndArgs(ConstStrA cmd, ConstStrA& name,
		ConstStrA& args) {
	natural space = cmd.find(' ');
	if (space == naturalNull) {
		name= cmd;
		args = ConstStrA();
	}
	name = cmd.head(space);
	args = cmd.offset(space);
	while (!args.empty() && isspace(args[0])) args = args.crop(1,0);
	while (!args.empty() && isspace(args[args.length()-1])) args = args.crop(0,1);
}

template<typename Ifc>
class FnCallValue: public JSON::TextFieldA {
public:
	FnCallValue(Ifc &ifc, StringA args):JSON::TextFieldA(args),ifc(ifc) {}

	Ifc &getFunction() const {return ifc;}

protected:
	Ifc &ifc;

};


ConstValue QueryServer::compileDesignDocument(const ConstValue &document) {
	ConstValue shows = document["shows"];
	ConstValue lists = document["lists"];
	ConstValue updates = document["updates"];
	ConstValue filters = document["filters"];
	ConstValue views = document["views"];
	Container compiled = json.object();

	if (shows != null) {
		compiled.set("shows",compileDesignSection(regShow,shows,"shows"));
	}
	if (lists != null) {
		compiled.set("lists",compileDesignSection(regList,lists,"lists"));
	}
	if (updates != null) {
		compiled.set("updates",compileDesignSection(regUpdate,updates,"updates"));
	}
	if (filters != null) {
		compiled.set("filters",compileDesignSection(regFilter,filters,"filters"));
	}
	if (views != null) {
		compiled.set("views",compileDesignSection(regMap,views,"views"));
	}
	return compiled;
}

template<typename T>
ConstValue createCompiledFnRef(T &fnRef, StringA args) {
	return new FnCallValue<typename T::ItemT>(*fnRef, args);
}

template<typename T>
ConstValue QueryServer::compileDesignSection(T &reg, const ConstValue &section, ConstStrA sectionName) {

	Container out = json.object();
	section->enumEntries(JSON::IEntryEnum::lambda([&](const JSON::INode *value, ConstStrA itemname, natural){
		ConstStrA cmd = value->getStringUtf8();
		ConstStrA name, args;
		splitToNameAndArgs(cmd, name, args);
		auto fnptr = reg.find(StrKey(name));
		if (fnptr == 0) {
			throw Error(THISLOCATION,"not_found",StringA(ConstStrA("Function '")+name+ConstStrA("' in section '")+sectionName+ConstStrA("' not found")));
		}
		ConstValue compiled = createCompiledFnRef(*fnptr, args);
		out.set(itemname, compiled);
		return false;
	}));
	return out;

}


ConstValue QueryServer::commandDDoc(const ConstValue& req, const PInOutStream& stream) {
	ConstStrA docid = req[1].getStringA();
	if (docid == "new") {
		//cache new document

		if (ddcache == null) ddcache = json.object();

		docid = req[2].getStringA();
		ConstValue ddoc = req[3];

		ConstValue compiledDDoc = compileDesignDocument(ddoc);
		ddcache.set(docid, compiledDDoc);
		return trueVal;
	} else {
		ConstValue doc = ddcache["docid"];
		if (doc == null)
			throw Error(THISLOCATION,"not_found",StringA(ConstStrA("The document '")+docid+ConstStrA("' is not cached at the query server")));
		ConstValue fn = doc;
		ConstValue path = req[2];
		for (natural i = 0, cnt = path.length(); i < cnt; i++) {
			fn = fn[path[i].getStringA()];
			if (fn == null)
				throw Error(THISLOCATION,"not_found",StringA(ConstStrA("Path not found'")+json.factory->toString(*path)+ConstStrA("'")));
		}
		ConstValue arguments = req[3];
		DDocCommand cmd = ddocCommands[path[0].getStringA()];
		ConstValue resp;
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

ConstValue QueryServer::commandShow(const ConstValue& fn, const ConstValue& args) {
	IShowFunction &showFn = fn->getIfc<FnCallValue<IShowFunction> >().getFunction();
	ConstValue resp = showFn(Document(args[0]), args[1]);
	return json << "resp" << resp;
}

ConstValue QueryServer::commandList(const ConstValue& fn, const ConstValue& args, const PInOutStream& stream) {

	class ListCtx: public IListContext {
	public:
		SeqFileInput requests;
		SeqFileOutput responses;
		Json &json;
		ListCtx(const PInOutStream &stream, Json &json, ConstValue viewHeader)
			: requests(PInOutStream(stream)), responses(PInOutStream(stream)), json(json)
			,headerObj(json.object())
			,viewHeader(viewHeader)
			,eof(false) {
			chunks = json.array();
		}
		virtual ConstValue getRow() {
			if (eof) return null;
			Container resp;
			if (headerObj != null) {
				resp = json << "start" << chunks << headerObj;
				headerObj = null;
			} else {
				resp = json << "chunks" << chunks;
			}
			chunks.clear();
			json.factory->toStream(*resp,responses);
			responses.write('\n');
			JSON::Value req = json.factory->fromStream(requests);
			if (req[0].getStringA() == "list_row") {
				return Row(req[1]);
			} else if (req[0].getStringA() == "list_end") {
				eof = true;
				return null;
			} else {
				throw Error(THISLOCATION,"protocol_error",StringA(ConstStrA("Expects list_row or list_end, got:")+req[0].getStringA()));
			}
		}

		ConstValue finish() {
			return json << "end" << chunks;
		}

		virtual void send(ConstStrA text) {
			chunks.add(json(text));
		}
		virtual void send(ConstValue jsonValue) {
			chunks.add(json(json.factory->toString(*jsonValue)));
		}
		virtual ConstValue getViewHeader() const {
			return viewHeader;
		}

	protected:
		Container chunks;
		Container headerObj;
		ConstValue viewHeader;
		bool eof;
	};

	IListFunction &listFn = fn->getIfc<FnCallValue<IListFunction> >().getFunction();

	ListCtx listCtx(stream,json,args[0]);
	listFn(listCtx,args[1]);
	return listCtx.finish();


}

ConstValue QueryServer::commandUpdate(const ConstValue& fn, const ConstValue& args) {
	IUpdateFunction &updateFn = fn->getIfc<FnCallValue<IUpdateFunction> >().getFunction();
	Context ctx;
	ctx.args = fn.getStringA();
	ctx.request = args[1];
	ctx.response = json.object();
	Document doc(args[0]);
	updateFn(doc, args[1]);
	if (doc.dirty()) {
		return json << "up" << doc.getEditing() << ctx.response;
	} else {
		return json << "up" << null << ctx.response;
	}

}

ConstValue QueryServer::commandView(const ConstValue& fn,
		const ConstValue& args) {
	IMapDocFn &mapDocFn = fn->getIfc<FnCallValue<IMapDocFn> >().getFunction();

	class FakeEmit: public IEmitFn {
	public:
		bool result;
		FakeEmit() {result = false;}
		virtual void operator()(const ConstValue &, const ConstValue &) {
			result = true;
		}
	};

	ConstValue docs= args[0];
	Json::Array results = json.array();
	docs->enumEntries(JSON::IEntryEnum::lambda([&](const JSON::INode *doc, ConstStrA, natural){
		FakeEmit emit;
		mapDocFn(Document(doc), emit);
		results << emit.result;
		return false;
	}));

	return json << true << results;
}

ConstValue QueryServer::commandFilter(const ConstValue& fn, const ConstValue& args) {
	IFilterFunction &filterFn = fn->getIfc<FnCallValue<IFilterFunction> >().getFunction();

	ConstValue docs= args[0];
	Context ctx;
	Json::Array results = json.array();
	docs->enumEntries(JSON::IEntryEnum::lambda([&](const JSON::INode *doc, ConstStrA, natural){
		results << filterFn(Document(doc), args[1]);
		return false;
	}));

	return json << true << results;
}


Value QueryServer::createDesignDocument(Value container, ConstStrA fnName, ConstStrA &suffix) {
	natural pos = fnName.find('/');
	ConstStrA docName;

	if (pos== naturalNull) {
		docName = qserverName;
		suffix = fnName;
	} else {
		docName = fnName.head(pos);
		suffix = fnName.offset(pos+1);
	}


	Value doc = container[docName];
	if (doc == null) {
		doc = json("_id",StringA(ConstStrA("_design/")+docName))
				("language", qserverName);

		container.set(docName,doc);
	}
	return doc;

}

ConstValue QueryServer::generateDesignDocuments() {
	Value ddocs = json.object();


	for(RegMapFn::Iterator iter = regMap.getFwIter(); iter.hasItems();) {
		const RegMapFn::KeyValue &kv = iter.getNext();
		bool hasReduce = regReduce.find(kv.key) != 0;
		ConstStrA itemName;
		Json::Object ddoc = json.object(createDesignDocument(ddocs,kv.key, itemName));
		Json::Object view = (ddoc/"views"/itemName);
		view("map",StringA(ConstStrA(kv.key)+ConstStrA("@")+ToString<natural>(kv.value->getVersion())));
		if (hasReduce) {
			view("reduce",kv.key);
		}
	}

	for (RegListFn::Iterator iter = regList.getFwIter(); iter.hasItems();) {
		const RegListFn::KeyValue &kv = iter.getNext();
		ConstStrA itemName;
		Json::Object ddoc = json.object(createDesignDocument(ddocs,kv.key,itemName));
		(ddoc/"lists")(itemName, kv.key);
	}

	for (RegShowFn::Iterator iter = regShow.getFwIter(); iter.hasItems();) {
		const RegShowFn::KeyValue &kv = iter.getNext();
		ConstStrA itemName;
		Json::Object ddoc = json.object(createDesignDocument(ddocs,kv.key,itemName));
		(ddoc/"shows")(itemName, kv.key);
	}

	for (RegUpdateFn::Iterator iter = regUpdate.getFwIter(); iter.hasItems();) {
		const RegUpdateFn::KeyValue &kv = iter.getNext();
		ConstStrA itemName;
		Json::Object ddoc = json.object(createDesignDocument(ddocs,kv.key,itemName));
		(ddoc/"updates")(itemName, kv.key);
	}

	for (RegFilterFn::Iterator iter = regFilter.getFwIter(); iter.hasItems();) {
		const RegFilterFn::KeyValue &kv = iter.getNext();
		ConstStrA itemName;
		Json::Object ddoc = json.object(createDesignDocument(ddocs,kv.key,itemName));
		(ddoc/"filters")(itemName, kv.key);
	}

	Container output = json.array();
	ddocs->forEach([&](const JSON::INode *doc, ConstStrA, natural) {

		Json::CObject finDoc = json.object(ConstValue(doc));
		output.add(finDoc);
		return false;

	});


	return output;
}


void QueryServer::syncDesignDocuments(ConstValue designDocuments, CouchDB& couch, CouchDB::DesignDocUpdateRule updateRule) {

	Changeset chset = couch.createChangeset();

	designDocuments->forEach([&](ConstValue doc, ConstStrA, natural) {
		couch.uploadDesignDocument(doc,updateRule);
		return false;
	});



}



QueryServerApp::QueryServerApp(integer priority):LightSpeed::App(priority) {
}

QueryServerApp::QueryServerApp(ConstStrA name, integer priority):LightSpeed::App(priority), QueryServer(name) {
}


void QueryServer::VersionMistmatch::message(ExceptionMsg& msg) const {
	msg("version mistmatch");
}

} /* namespace LightCouch */

