/*
 * queryServer.cpp
 *
 *  Created on: 4. 7. 2016
 *      Author: ondra
 */

#include "queryServer.h"

#include <lightspeed/base/namedEnum.tcc>
#include <lightspeed/base/streams/fileiobuff.tcc>

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

		} catch (Error &e) {
			JSON::Value out = json << "error" << e.getType() << e.getExplain();
			factory->toString(*out);
		} catch (std::exception &e) {
			JSON::Value out = json << "error" << "internal_error" << e.what();
			factory->toString(*out);

		}

	}
}

ConstValue QueryServer::commandReset(const ConstValue& req) {
	preparedMaps.clear();
	return trueVal;
}

ConstValue QueryServer::commandAddLib(const ConstValue& req) {
	throw Error(THISLOCATION,"unsupported","You cannot add lib to native C++ query server");
}

ConstValue QueryServer::commandAddFun(const ConstValue& req) {
	ConstStrA name, args;
	splitToNameAndArgs(req[1].getStringA(), name, args);
	const AllocPointer<IMapDocFn>  *fn = regMap.find(StrKey(name));
	if (fn == 0) {
		throw Error(THISLOCATION,"not_found",StringA(ConstStrA("Function '")+name+ConstStrA("' not found")));
	}
	preparedMaps.add(PreparedMap(*(*fn),args));
	return trueVal;
}

ConstValue QueryServer::commandMapDoc(const ConstValue& req) {
}

ConstValue QueryServer::commandReduce(const ConstValue& req) {
}

ConstValue QueryServer::commandReReduce(const ConstValue& req) {
}

ConstValue QueryServer::commandDDoc(const ConstValue& req,
		const PInOutStream& stream) {
}

} /* namespace LightCouch */

