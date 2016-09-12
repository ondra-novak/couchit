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
#include <lightspeed/base/text/textFormat.tcc>


#include "changeset.h"
#include "lightspeed/mt/thread.h"
using LightSpeed::HashMD5;
using LightSpeed::HttpStatusException;
using LightSpeed::JSON::Null;
using LightSpeed::NamedEnumDef;
namespace LightCouch {

QueryServer::QueryServer() {}
QueryServer::QueryServer(ConstStrA name):qserverName(name) {}



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
		} catch (QueryServerError &e) {
			JSON::Value out = json << "error" << e.getType() << e.getExplain();
			factory->toStream(*out, responses);
		} catch (VersionMistmatch &m) {
			JSON::Value out = json << "error" << "try_again" << "restarting query server, please try again";
			factory->toStream(*out, responses);
			responses.write('\n');
			responses.flush();
			//need sleep to give couchdb chance to emit error to the user
			//Thread::sleep(1000);
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
	return JSON::getConstant(JSON::constTrue);
}

ConstValue QueryServer::commandAddLib(const ConstValue& ) {
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

ConstValue QueryServer::commandAddFun(const ConstValue& req) {
	ConstStrA name = req[1].getStringA();
	natural verId = extractVersion(name);

	const AllocPointer<AbstractViewBase>  *fn = views.find(StrKey(name));
	if (fn == 0) {
		throw QueryServerError(THISLOCATION,"not_found",StringA(ConstStrA("Map Function '")+name+ConstStrA("' not found")));
	}
	if ((*fn)->version() != verId)
		throw VersionMistmatch(THISLOCATION);
	preparedMaps.add(PreparedMap(*(*fn)));
	return JSON::getConstant(JSON::constTrue);
}

ConstValue QueryServer::commandMapDoc(const ConstValue& req) {

	class Emit: public IEmitFn {
	public:
		Container &container;
		Json &json;
		Emit(Container &container,Json &json):container(container),json(json) {}

		virtual void operator()() override {
			container.add( (json << JSON::getConstant(JSON::constNull) ,JSON::getConstant(JSON::constNull) ) );
		}

		virtual void operator()(const ConstValue &key) override {
			container.add( (json << key,JSON::getConstant(JSON::constNull)) );
		}

		virtual void operator()(const ConstValue &key, const ConstValue &value) override {
			container.add( (json << key,value) );
		}
	};

	Document doc = req[1];
	Container result = json.array();

	for (natural i = 0; i < preparedMaps.length(); i++) {
		Container subres = json.array();
		Emit emit(subres,json);
		preparedMaps[i].fn.map(doc,emit);
		result.add(subres);
	}

	return result;

}


ConstValue QueryServer::commandReduce(const ConstValue& req) {
	ConstValue fnlist = req[1];
	ConstValue values = req[2];
	Container output = json.array();;
	rowBuffer.clear();

	for (natural i = 0, cnt = values.length(); i < cnt; ++i) {

		const JSON::INode *val = values->getPtr(i);
		const JSON::INode *keyPart = val->getPtr(0);
		const JSON::INode *key = keyPart->getPtr(0);
		const JSON::INode *docId = keyPart->getPtr(1);
		const JSON::INode *valPart = val->getPtr(1);
		rowBuffer.add(AbstractViewBase::Row(docId,key,valPart));
	}

	for (natural i = 0, cnt = fnlist.length(); i < cnt; i++) {
		ConstStrA name = fnlist[i].getStringA();
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
	return json << true << output;
}
ConstValue QueryServer::commandReReduce(const ConstValue& req) {
	ConstValue fnlist = req[1];
	ConstValue values = req[2];
	Container output = json.array();;
	valueBuffer.clear();

	for (natural i = 0, cnt = values.length(); i < cnt; ++i) {
		valueBuffer.add(values[i]);
	}

	for (natural i = 0, cnt = fnlist.length(); i < cnt; i++) {
		ConstStrA name = fnlist[i].getStringA();

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
	return json << true << output;
}



template<typename Ifc>
class FnCallValue: public JSON::Null {
public:
	FnCallValue(Ifc &ifc):ifc(ifc) {}

	Ifc &getFunction() const {return ifc;}

protected:
	Ifc &ifc;

};


ConstValue QueryServer::compileDesignDocument(const ConstValue &document) {
	ConstValue vshows = document["shows"];
	ConstValue vlists = document["lists"];
	ConstValue vupdates = document["updates"];
	ConstValue vfilters = document["filters"];
	ConstValue vviews = document["views"];
	Container compiled = json.object();

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
ConstValue createCompiledFnRef(T &fnRef) {
	return new FnCallValue<typename T::ItemT>(*fnRef);
}

template<typename T>
ConstValue QueryServer::compileDesignSection(T &reg, const ConstValue &section, ConstStrA sectionName) {

	Container out = json.object();
	section->enumEntries(JSON::IEntryEnum::lambda([&](const JSON::INode *value, ConstStrA itemname, natural){
		bool inmap = false;
		if (value->isObject()) {
			value = value->getPtr("map");
			if (value == 0) return false;
			inmap = true;
		}
		ConstStrA name = value->getStringUtf8();
		natural verId = extractVersion(name);

		auto fnptr = reg.find(StrKey(name));
		if (fnptr == 0) {
			throw QueryServerError(THISLOCATION,"not_found",StringA(ConstStrA("Function '")+name+ConstStrA("' in section '")+sectionName+ConstStrA("' not found")));
		}

		if ((*fnptr)->version() != verId) {
			throw VersionMistmatch(THISLOCATION);
		}

		ConstValue compiled = createCompiledFnRef(*fnptr);
		if (inmap) compiled = json("map", compiled);
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
		return JSON::getConstant(JSON::constTrue);;
	} else {
		ConstValue doc = ddcache[docid];
		if (doc == null)
			throw QueryServerError(THISLOCATION,"not_found",StringA(ConstStrA("The document '")+docid+ConstStrA("' is not cached at the query server")));
		ConstValue fn = doc;
		ConstValue path = req[2];
		for (natural i = 0, cnt = path.length(); i < cnt; i++) {
			fn = fn[path[i].getStringA()];
			if (fn == null)
				throw QueryServerError(THISLOCATION,"not_found",StringA(ConstStrA("Path not found'")+json.factory->toString(*path)+ConstStrA("'")));
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
	AbstractShowBase &showFn = fn->getIfc<FnCallValue<AbstractShowBase> >().getFunction();
	ConstValue resp = showFn.run(Document(args[0]), args[1]);
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
			json.factory->toStream(*resp,responses);
			responses.write('\n');
			chunks.clear();
			JSON::Value req = json.factory->fromStream(requests);
			if (req[0].getStringA() == "list_row") {
				return req[1];
			} else if (req[0].getStringA() == "list_end") {
				eof = true;
				return null;
			} else {
				throw QueryServerError(THISLOCATION,"protocol_error",StringA(ConstStrA("Expects list_row or list_end, got:")+req[0].getStringA()));
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
		virtual void start(ConstValue headerObj) {
			if (this->headerObj != null) this->headerObj.load(headerObj);
		}

	protected:
		Container chunks;
		Container headerObj;
		ConstValue viewHeader;
		bool eof;
	};

	AbstractListBase &listFn = fn->getIfc<FnCallValue<AbstractListBase> >().getFunction();

	ListCtx listCtx(stream,json,args[0]);
	listFn.run(listCtx,args[1]);
	return listCtx.finish();


}

ConstValue QueryServer::commandUpdate(const ConstValue& fn, const ConstValue& args) {
	AbstractUpdateFnBase &updateFn = fn->getIfc<FnCallValue<AbstractUpdateFnBase> >().getFunction();
	Document doc(args[0]);
	ConstValue response = updateFn.run(doc, args[1]);
	if (doc.dirty()) {
		return json << "up" << doc.getEditing() << response;
	} else {
		return json << "up" << null << response;
	}

}

ConstValue QueryServer::commandView(const ConstValue& fn,
		const ConstValue& args) {
	AbstractViewBase &mapDocFn = fn->getIfc<FnCallValue<AbstractViewBase> >().getFunction();

	class FakeEmit: public IEmitFn {
	public:
		bool result;
		FakeEmit() {result = false;}
		virtual void operator()(const ConstValue &, const ConstValue &) {
			result = true;
		}
		virtual void operator()(const ConstValue &) {
			result = true;
		}
		virtual void operator()() {
			result = true;
		}
	};

	ConstValue docs= args[0];
	Json::Array results = json.array();
	docs->enumEntries(JSON::IEntryEnum::lambda([&](const JSON::INode *doc, ConstStrA, natural){
		FakeEmit emit;
		mapDocFn.map(Document(doc), emit);
		results << emit.result;
		return false;
	}));

	return json << true << results;
}

ConstValue QueryServer::commandFilter(const ConstValue& fn, const ConstValue& args) {
	AbstractFilterBase &filterFn = fn->getIfc<FnCallValue<AbstractFilterBase> >().getFunction();

	ConstValue docs= args[0];
	Json::Array results = json.array();
	docs->enumEntries(JSON::IEntryEnum::lambda([&](const JSON::INode *doc, ConstStrA, natural){
		results << filterFn.run(Document(doc), args[1]);
		return false;
	}));

	return json << true << results;
}

void QueryServer::regView(StringA viewName, AbstractViewBase* impl) {
	impl->setJson(json);
	views.insert(StrKey(viewName),impl);
}

void QueryServer::regList(StringA listName, AbstractListBase* impl) {
	impl->setJson(json);
	lists.insert(StrKey(listName),impl);
}

void QueryServer::regShow(StringA showName, AbstractShowBase* impl) {
	impl->setJson(json);
	shows.insert(StrKey(showName),impl);
}

void QueryServer::regUpdateFn(StringA updateName, AbstractUpdateFnBase* impl) {
	impl->setJson(json);
	updates.insert(StrKey(updateName),impl);
}

void QueryServer::regFilter(StringA filterName, AbstractFilterBase* impl) {
	impl->setJson(json);
	filters.insert(StrKey(filterName),impl);
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

Value createVersionedRef(Json &json, ConstStrA name, natural ver) {
	TextFormatBuff<char, SmallAlloc<256 >> fmt;
	fmt("%1@%2") << name << ver;
	return json(ConstStrA(fmt.write()));
}

ConstValue QueryServer::generateDesignDocuments() {
	Value ddocs = json.object();


	for(RegView::Iterator iter = views.getFwIter(); iter.hasItems();) {
		const RegView::KeyValue &kv = iter.getNext();
		ConstStrA itemName;
		Json::Object ddoc = json.object(createDesignDocument(ddocs,kv.key, itemName));
		Json::Object view = (ddoc/"views"/itemName);
		view("map",createVersionedRef(json,kv.key,kv.value->version()));
		switch (kv.value->reduceMode()) {
		case AbstractViewBase::rmNone:break;
		case AbstractViewBase::rmFunction:
			view("reduce",kv.key);break;
		case AbstractViewBase::rmSum:
			view("reduce","_sum");break;
		case AbstractViewBase::rmCount:
			view("reduce","_count");break;
		case AbstractViewBase::rmStats:
			view("reduce","_stats");break;
		}
	}

	for (RegListFn::Iterator iter = lists.getFwIter(); iter.hasItems();) {
		const RegListFn::KeyValue &kv = iter.getNext();
		ConstStrA itemName;
		Json::Object ddoc = json.object(createDesignDocument(ddocs,kv.key,itemName));
		(ddoc/"lists")(itemName, createVersionedRef(json,kv.key,kv.value->version()));
	}

	for (RegShowFn::Iterator iter = shows.getFwIter(); iter.hasItems();) {
		const RegShowFn::KeyValue &kv = iter.getNext();
		ConstStrA itemName;
		Json::Object ddoc = json.object(createDesignDocument(ddocs,kv.key,itemName));
		(ddoc/"shows")(itemName, createVersionedRef(json,kv.key,kv.value->version()));
	}

	for (RegUpdateFn::Iterator iter = updates.getFwIter(); iter.hasItems();) {
		const RegUpdateFn::KeyValue &kv = iter.getNext();
		ConstStrA itemName;
		Json::Object ddoc = json.object(createDesignDocument(ddocs,kv.key,itemName));
		(ddoc/"updates")(itemName, createVersionedRef(json,kv.key,kv.value->version()));
	}

	for (RegFilterFn::Iterator iter = filters.getFwIter(); iter.hasItems();) {
		const RegFilterFn::KeyValue &kv = iter.getNext();
		ConstStrA itemName;
		Json::Object ddoc = json.object(createDesignDocument(ddocs,kv.key,itemName));
		(ddoc/"filters")(itemName, createVersionedRef(json,kv.key,kv.value->version()));
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


void VersionMistmatch::message(ExceptionMsg& msg) const {
	msg("version mistmatch");
}

} /* namespace LightCouch */

