/*
 * couchDB.cpp
 *
 *  Created on: 8. 3. 2016
 *      Author: ondra
 */

#include "ichangeNotify.h"
#include "changeset.h"
#include "couchDB.h"
#include <lightspeed/base/containers/convertString.h>
#include <lightspeed/base/text/textFormat.tcc>
#include <lightspeed/base/text/textOut.tcc>
#include <lightspeed/mt/atomic.h>
#include <lightspeed/utils/urlencode.h>
#include "lightspeed/base/streams/secureRandom.h"
#include "lightspeed/base/exceptions/errorMessageException.h"
#include "lightspeed/base/memory/poolalloc.h"
#include "lightspeed/base/streams/fileiobuff.tcc"
#include "lightspeed/base/containers/map.tcc"
#include "exception.h"
#include "uid.h"
#include "query.h"

#include "lightspeed/base/streams/netio_ifc.h"

#include "changedDoc.h"

#include "lightspeed/base/exceptions/netExceptions.h"

#include "queryCache.h"

#include "document.h"
using LightSpeed::INetworkServices;
using LightSpeed::lockInc;
namespace LightCouch {

const ConstStrA CouchDB::GET("GET");
const ConstStrA CouchDB::POST("POST");
const ConstStrA CouchDB::PUT("PUT");
const ConstStrA CouchDB::DELETE("DELETE");

const ConstStrA CouchDB::disableCache("disableCache");
const ConstStrA CouchDB::refreshCache("refreshCache");
const ConstStrA CouchDB::storeHeaders("storeHeaders");



static JSON::PFactory createFactory(JSON::PFactory jfact) {
	if (jfact != null) return jfact;
	else return JSON::create();
}

CouchDB::CouchDB(const Config& cfg)
	:json(createFactory(cfg.factory)),connSource(cfg.connSource), pathPrefix(cfg.pathPrefix),factory(json.factory)
	,cache(cfg.cache),seqNumSlot(0)
{
	if (!cfg.databaseName.empty()) use(cfg.databaseName);
	listenExitFlag = false;
}

HttpResponse &CouchDB::rawRequest(ConstStrA method, ConstStrA path, ConstStrA postData, JSON::Value headers) {
	try {
		return rawRequest_noErrorRetry(method,path,postData,headers);
	} catch (std::exception &e) {
		lastConnectError = e.what();
		stream = nil;
		return rawRequest_noErrorRetry(method,path,postData,headers);
	}

}

HttpResponse &CouchDB::rawRequest_noErrorRetry(ConstStrA method, ConstStrA path, ConstStrA postData, JSON::Value headers) {
	if (stream == nil) {
		stream = Constructor1<NStream, PNetworkStream>(connSource.getNext());
	} else {
		if (response != nil) {
			response->skipRemainBody();
		}
	}

	StringA wholePath = pathPrefix+path;
	HttpRequest rq(stream->getOutput(),wholePath,method);

	bool hasCtxType = false;

	ConstStrA ctxLenStr =rq.getHeaderFieldName(HttpRequest::fldContentLength);
	ConstStrA ctxTypeStr =rq.getHeaderFieldName(HttpRequest::fldContentType);

	if (headers!= null && headers->getType() == JSON::ndObject) {
		for (JSON::Iterator iter = headers->getFwIter(); iter.hasItems();) {
			const JSON::KeyValue &kv = iter.getNext();
			ConstStrA key = kv.key;
			if (key != ctxLenStr) {
				rq.setHeader(key, kv->getStringUtf8());
				if (key == ctxTypeStr) hasCtxType = true;
			}
		}
	}

	if (method == "POST" || method == "PUT") {
		rq.setContentLength(postData.length());
		if (!hasCtxType)
			rq.setHeader(HttpRequest::fldContentType,"application/json");
		rq.writeAll(postData.data(),postData.length());
	}
	rq.closeOutput();


	response = Constructor1<HttpResponse, IInputStream *>(stream->getInput());
	response->setStaticObj();
	response->readHeaders();
	return response;

}


JSON::Value CouchDB::requestJson(ConstStrA method, ConstStrA path, JSON::Value postData, JSON::Value headers) {


	if (headers != null && headers->getType() != JSON::ndObject) {
		throw InvalidParamException(THISLOCATION,4,"Argument headers must be either null or an JSON object");
	}

	bool hasheaders = headers != null;
	bool storehdrs = false;
	bool usecache = true;
	bool useseqnums = true;
	JValue headersToSend = json.object();

	ConstStrA cachePath;


	if (hasheaders) {
		for (JSON::Iterator iter = headers->getFwIter(); iter.hasItems();) {
			const JSON::KeyValue &kv = iter.getNext();
			ConstStrA str = kv->getStringUtf8();
			if (str == disableCache) {
				if (kv->getBool()) usecache = false;
			} else if (str == refreshCache) {
				if (kv->getBool()) useseqnums = false;
			} else if (str == storeHeaders) {
				if (kv->getBool()) storehdrs = false;
			} else {
				headersToSend->add(kv);
			}
		}
	}

	ConstStrA postDataStr;

	StringA tmp;

	//path starting with slash is not relative to database, otherwise set prefix
	if (path.head(1) != ConstStrA("/")) {
		if (database.empty()) throw ErrorMessageException(THISLOCATION,"No database selected");
		cachePath = path;
		tmp = StringA(database+path);
		path = tmp;
	} else {
		//disable caching for absolute path
		usecache = false;
	}


	//there will be stored cached item
	Optional<QueryCache::CachedItem> cachedItem;

	//use cache only for method GET, if cache is defined and postData are null
	usecache = usecache && (method == GET) && cache;

	if (usecache) {
		cachedItem = cache->find(cachePath);
		if (cachedItem->isDefined()) {
			if (seqNumSlot && *seqNumSlot == cachedItem->seqNum && useseqnums)
				return cachedItem->value;
			headersToSend->add("If-None-Match",factory(cachedItem->etag));
		}
		postData = null;
	} else if (method == GET) {
		postData = null;
	}

	if (postData != null) postDataStr = factory->toString(*postData);

	HttpReq response(*this,method,path, postDataStr,headersToSend);
	SeqFileInput in(response.getBody());
	if (response.getStatus() == 304 && cachedItem != null) {
		return cachedItem->value;
	}
	if (response.getStatus()/100 != 2) {

		JSON::Value errorVal;
		try{
			errorVal = factory->fromStream(in);
		} catch (...) {

		}
		throw RequestError(THISLOCATION,response.getStatus(), response.getStatusMessage(), errorVal);
	}
	lastStatus = response.getStatus();

	JSON::Value v = factory->fromStream(in);;

	if (storehdrs) {
		headers.clear();
		response.enumHeaders([&](ConstStrA key, ConstStrA value) {
			headers->add(key,factory->newValue(value));
			return true;
		});
	}

	if (usecache){
		BredyHttpSrv::HeaderValue fld = response.getHeaderField(response.fldETag);
		if (fld.defined) {
			atomicValue useq = seqNumSlot?*seqNumSlot:0;
			cache->set(cachePath, QueryCache::CachedItem(fld,useq, v));
		}
	}


	return v;

}


UIDIterator CouchDB::genUID(natural count) {

	TextFormatBuff<char, StaticAlloc<32> > fmt;

	fmt("/_uuids?count=%1") << count;
	JSON::Value uidlist = requestJson(GET,fmt.write());
	return UIDIterator(uidlist["uuids"]);

}

static lnatural getRandom() {
	SecureRandom srand;
	natural out;
	srand.blockRead(&out,sizeof(out));
	return out;
}

static atomic counter = 0;

LocalUID CouchDB::genUIDFast() {

	static lnatural randomNumber = getRandom();

	TimeStamp t = TimeStamp::now();
	TextFormatBuff<char, StaticAlloc<256> > fmt;
	fmt.setBase(62);
	atomicValue v = lockInc(counter);
	fmt("%{03}1%%{05}2%%{06}3%%{011}4%")
		<< t.getDay()
		<< t.getTime()
		<< (Bin::natural32)(v & 0x3FFFFFFF)
		<< randomNumber;
	return LocalUID(fmt.write().head(25));


}

void CouchDB::use(ConstStrA database) {
	this->database = ConstStrA("/") + database+ConstStrA("/");
}

ConstStrA CouchDB::getCurrentDB() const {
	return database.crop(1,1);
}

StringA CouchDB::urlencode(ConstStrA text) {
	return convertString(UrlEncoder(), text);
}

void CouchDB::createDatabase() {
	requestJson(PUT,ConstStrA(),null);
}

void CouchDB::deleteDatabase() {
	requestJson(DELETE,ConstStrA(),null);
}

CouchDB::~CouchDB() {
}

enum ListenExceptionStop {listenExceptionStop};

natural CouchDB::listenChangesInternal(IChangeNotify &cb, natural fromSeq, const Filter &filter, ListenMode lm) {

	AutoArrayStream<char, SmallAlloc<1024> > gline;
	StringA hlp;

	JSON::Value hdrs = json(refreshCache,true);


	class WHandle: public INetworkResource::WaitHandler {
	public:
		bool &listenExitFlag;;

		virtual natural wait(const INetworkResource *resource, natural waitFor, natural ) const {
			while (!listenExitFlag) {
				if (limitTm.expired())
					return 0;
				//each 150 ms check exit flag.
				//Find better solution later
				natural r = INetworkResource::WaitHandler::wait(resource,waitFor,150);
				if (r) {
					limitTm = Timeout(100000);
					return r;
				}
			}
			throw listenExceptionStop;
		}

		WHandle(bool &listenExitFlag):listenExitFlag(listenExitFlag),limitTm(100000) {}
		mutable Timeout limitTm;

	};

	WHandle whandle(listenExitFlag);

	if (stream == nil) {
		stream = Constructor1<NStream, PNetworkStream>(connSource.getNext());
	}



	if (lm != lmNoWait)
		stream->getHandle()->setWaitHandler(&whandle);

	try {

		bool rep = true;
		do {

			if (listenExitFlag) break;

			gline.clear();
			TextOut<AutoArrayStream<char, SmallAlloc<1024> > &, StaticAlloc<256> > fmt(gline);
			fmt("%1_changes?since=%2") << database << fromSeq;
			if (!filter.viewPath.empty()) {
				if (filter.flags & Filter::isView) {
					fmt("&filter=_view&view=%1") << (hlp=urlencode(filter.viewPath));
				}else{
					fmt("&filter=%1") << (hlp=urlencode(filter.viewPath));
				}
							}
			if(filter.flags & Filter::allConflicts) {
				fmt("&style=all_docs");
			}
			if (filter.flags & Filter::includeDocs) {
				fmt("&include_docs=true");
				if (filter.flags & Filter::attachments) {
					fmt("&attachments=true");
				}
				if (filter.flags & Filter::conflicts) {
					fmt("&conflicts=true");
				}
				if (filter.flags & Filter::attEncodingInfo) {
					fmt("&att_encoding_info=true");
				}
			}
			if (filter.flags & Filter::reverseOrder) {
				fmt("&descending=true");
			}
			if (lm  != lmNoWait) fmt("&feed=longpoll");

			for (natural i = 0; i<filter.args.length(); i++) {
				fmt("&%1=%2") << filter.args[i].key << (hlp=urlencode(filter.args[i].value));
			}

			JSON::Value v = requestJson(GET,gline.getArray(),null,hdrs);
			JSON::Value results=v["results"];

			natural lastSeq = v["last_seq"]->getUInt();
			if (seqNumSlot) *seqNumSlot = lastSeq;

			bool cbok;
			for (JSON::Iterator iter = results->getFwIter(); iter.hasItems();) {
				ChangedDoc doc(iter.getNext());
				fromSeq = doc.seqId;
				cbok = cb.onChange(doc);
				if (!cbok) break;
			}

			fromSeq =  lastSeq;

			rep = lm == lmForever && listenExitFlag == false && cbok;
		}
		while (rep);
		stream->getHandle()->setWaitHandler(0);
		listenExitFlag = false;
	} catch (ListenExceptionStop &) {
		listenExitFlag = false;
		stream=nil;
		return fromSeq;
	} catch (...) {
		if (stream != nil) stream->getHandle()->setWaitHandler(0);
		listenExitFlag = false;
		throw;
	}

	return fromSeq;
}

Query CouchDB::createQuery(const View &view) {
	return Query(*this, view);
}

Changeset CouchDB::createChangeset() {
	return Changeset(*this);
}

JSON::IFactory &CouchDB::getJsonFactory() {
	return *factory;
}


CouchDB::HttpReq::HttpReq(CouchDB& db, ConstStrA method, ConstStrA path, ConstStrA body, JSON::Value headers)
	:lock(db.lock)
	,response(db.rawRequest(method,path,body,headers))
	,owner(db)
{

}

natural CouchDB::HttpReq::getStatus() const {
	return response.getStatus();
}

ConstStrA CouchDB::HttpReq::getStatusMessage() const {
	return response.getStatusMessage();
}

BredyHttpSrv::HeaderValue CouchDB::HttpReq::getHeaderField(Field field) const {
	return response.getHeaderField(field);
}

BredyHttpSrv::HeaderValue CouchDB::HttpReq::getHeaderField(ConstStrA field) const {
	return response.getHeaderField(field);
}

natural CouchDB::HttpReq::getContentLength() const {
	return response.getContentLength();
}

SeqFileInput CouchDB::HttpReq::getBody() {
	SeqFileInBuff<512> buffered(&response);
	return buffered;
}

void CouchDB::stopListenChanges() {
	listenExitFlag = true;
}

CouchDB::HttpReq::~HttpReq() {
	try {
		response.skipRemainBody();
	} catch (...) {
		owner.stream = nil;
	}
}

natural CouchDB::getLastSeqNumber() {
	Filter::ListArg arg;
	arg.key = ConstStrA("limit");
	arg.value = ConstStrA("1");
	return listenChanges(0,Filter(StringA(),Filter::reverseOrder,Filter::ListArgs(&arg,1))
			,lmNoWait,[](const ChangedDoc &){return true;});
}

atomicValue& CouchDB::trackSeqNumbers() {
	if (seqNumSlot) return *seqNumSlot;
	if (cache == 0) throw ErrorMessageException(THISLOCATION,"Caching is disabled");
	if (database.empty()) throw ErrorMessageException(THISLOCATION,"No current database");
	atomicValue &v = cache->trackSeqNumbers(database);
	if (v == 0) lockCompareExchange(v,0,getLastSeqNumber());
	seqNumSlot = &v;
	return v;
}

Conflicts CouchDB::loadConflicts(const Document &confDoc) {
	TextFormatBuff<char, StaticAlloc<256> > fmt;
	StringA docId = urlencode(confDoc.id);
	StringA revList;
	JSON::Value allRevs = factory->array();
	allRevs->add(confDoc.allData["_rev"]);
	allRevs->copy(const_cast<JSON::INode *>(confDoc.conflicts));
	revList= urlencode(factory->toString(*allRevs));
	fmt("%1?open_revs=%2") << docId << revList;
	JSON::Value res = requestJson(GET,fmt.write());
	Conflicts c;
	for (JSON::Iterator iter = res->getFwIter(); iter.hasItems();) {
		const JSON::KeyValue &kv = iter.getNext();
		JSON::Value doc = kv->getPtr("ok");
		if (doc != null) c.add(Document(doc));
	}
	return c;
}
Query CouchDB::createQuery(natural viewFlags) {
	View v("_all_docs", viewFlags);
	return createQuery(v);
}

JSON::Value CouchDB::retrieveLocalDocument(ConstStrA localId) {
	TextFormatBuff<char, StaticAlloc<256> > fmt;
	StringA encdoc = urlencode(localId);
	fmt("_local/%1") << encdoc;
	return requestJson(GET,fmt.write(),null,json(refreshCache,true));

}

CouchDB::UpdateFnResult CouchDB::callUpdateFn(ConstStrA updateFnPath,
		ConstStrA documentId, JSON::Value arguments) {
	AutoArrayStream<char, SmallAlloc<1024> > urlline;
	TextOut<AutoArrayStream<char, SmallAlloc<1024> > &, SmallAlloc<256> > fmt(urlline);
	String tmp;
	fmt("%1/%2") << updateFnPath << (tmp=urlencode(documentId));

	if (arguments != null) {
		char c = '?';
		for (JSON::Iterator iter = arguments->getFwIter(); iter.hasItems();) {
			const JSON::KeyValue &kv = iter.getNext();
			fmt("%1%2=") << c << (tmp=urlencode(kv.getStringKey()));
			if (kv->getType() == JSON::ndString) {
				tmp = urlencode(kv->getStringUtf8());
			} else {
				tmp = urlencode(factory->toString(*kv));
			}
			fmt("%1") <<tmp;
			c = '&';
		}
	}

	JSON::Value h = json(storeHeaders,true);
	JSON::Value v = requestJson(PUT,urlline.getArray(), null, h);

	UpdateFnResult r;
	const JSON::INode *n = h->getPtr("X-Couch-Update-NewRev");
	if (n) {
		r.newRevID = n->getStringUtf8();
	}
	r.response = v;
	return r;


}


} /* namespace assetex */

