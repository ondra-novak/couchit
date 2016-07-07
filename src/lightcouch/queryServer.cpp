/*
 * queryServer.cpp
 *
 *  Created on: 4. 7. 2016
 *      Author: ondra
 */

#include "queryServer.h"

#include <lightspeed/base/namedEnum.tcc>
#include <lightspeed/base/streams/fileiobuff.tcc>
#include <lightspeed/utils/json/jsonimpl.h>

using LightSpeed::JSON::Null;
using LightSpeed::NamedEnumDef;
namespace LightCouch {


const StringA& QueryServer::Error::getType() const {
	return type;
}

const StringA& QueryServer::Error::getExplain() const {
	return explain;
}

void QueryServer::Error::message(ExceptionMsg& msg) const {
	msg("QueryServer error: %1 - %2") << type << explain;
}

integer QueryServer::start(const Args& args) {

	integer init = initServer(args);
	if (init) return init;


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
	ConstStrA name, args;
	splitToNameAndArgs(req[1].getStringA(), name, args);
	const AllocPointer<IMapDocFn>  *fn = regMap.find(StrKey(name));
	if (fn == 0) {
		throw Error(THISLOCATION,"not_found",StringA(ConstStrA("Map Function '")+name+ConstStrA("' not found")));
	}
	preparedMaps.add(PreparedMap(*(*fn),args));
	return trueVal;
}

ConstValue QueryServer::commandMapDoc(const ConstValue& req) {

	class Emit: public IEmitFn {
	public:
		Container &container;
		Json &json;
		Emit(Container &container,Json &json):container(container),json(json) {}
		virtual void operator()(const ConstValue &key, const ConstValue &value) {
			container.add(json << key << value);
		}
	};

	Document doc = req[1];
	Container result = json.array();

	for (natural i = 0; i < preparedMaps.length(); i++) {
		Container subres = json.array();
		Emit emit(subres,json);
		preparedMaps[i].fn(doc,emit,preparedMaps[i].args);
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
		output.add(reduce(values, args));
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
	IShowFunction &showFn = fn->getIfc<const FnCallValue<IShowFunction> >().getFunction();
	Document doc = args[0];
	ConstValue req = args[1];
	Value outHeader = json.object();
	MemFile<> output;output.setStaticObj();



}

ConstValue QueryServer::commandList(const ConstValue& fn,
		const ConstValue& args, const PInOutStream& stream) {
}

ConstValue QueryServer::commandUpdate(const ConstValue& fn,
		const ConstValue& args) {
}

ConstValue QueryServer::commandView(const ConstValue& fn,
		const ConstValue& args) {
}

ConstValue QueryServer::commandFilter(const ConstValue& fn,
		const ConstValue& args) {
}


} /* namespace LightCouch */

