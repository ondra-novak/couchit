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

CouchDB::HttpConfig::HttpConfig() {
	this->keepAlive = true;
	this->useHTTP10 = false;
	this->userAgent = ConstStrA("LightCouch/1.0 (+https://github.com/ondra-novak/lightcouch)");
}

CouchDB::HttpConfig CouchDB::httpConfig;

static JSON::PFactory createFactory(JSON::PFactory jfact) {
	if (jfact != null) return jfact;
	else return JSON::create();
}

CouchDB::CouchDB(const Config& cfg)
	:json(createFactory(cfg.factory)),baseUrl(cfg.baseUrl),http(httpConfig),factory(json.factory)
	,cache(cfg.cache),seqNumSlot(0)
{
	if (!cfg.databaseName.empty()) use(cfg.databaseName);
	listenExitFlag = false;
}


JSON::ConstValue CouchDB::jsonGET(ConstStrA path, JSON::Value headers, natural flags) {
	if (headers != null && headers->getType() != JSON::ndObject) {
		throw InvalidParamException(THISLOCATION,4,"Argument headers must be either null or an JSON object");
	}

	AutoArray<char, SmallAlloc<4096> > requestUrl;
	reqPathToFullPath(path,requestUrl);

	bool usecache = (flags & disableCache) == 0;
	if (path.head(1) == ConstStrA('/')) usecache = false;
	if (!cache) usecache = false;

	//there will be stored cached item
	Optional<QueryCache::CachedItem> cachedItem;

	if (usecache) {
		cachedItem = cache->find(path);
		if (cachedItem->isDefined()) {
			if (seqNumSlot && *seqNumSlot == cachedItem->seqNum && (flags & refreshCache) == 0)
				return cachedItem->value;
		}
	}

	Synchronized<FastLock> _(lock);
	http.open(HttpClient::mGET, requestUrl);
	http.setHeader(HttpClient::fldAccept,"application/json");
	if (cachedItem != nil) {
		http.setHeader(HttpClient::fldIfNoneMatch, cachedItem->etag);
	}
	headers->enumEntries(JSON::IEntryEnum::lambda([this](const JSON::INode *nd, ConstStrA key, natural ){
		this->http.setHeader(key,nd->getStringUtf8());
		return false;
	}));

	SeqFileInput response = http.send();
	if (http.getStatus() == 304 && cachedItem != null) {
		http.close();
		return cachedItem->value;
	}
	if (http.getStatus()/100 != 2) {

		JSON::Value errorVal;
		try{
			errorVal = factory->fromStream(response);
		} catch (...) {

		}
		http.close();
		throw RequestError(THISLOCATION,http.getStatus(), http.getStatusMessage(), errorVal);
	} else {
		JSON::Value v = factory->fromStream(response);
		if (usecache) {
			BredyHttpSrv::HeaderValue fld = http.getHeader(HttpClient::fldETag);
			if (fld.defined) {
				atomicValue useq = seqNumSlot?*seqNumSlot:0;
				cache->set(path, QueryCache::CachedItem(fld,useq, v));
			}
		}
		if (flags & storeHeaders && headers != null) {
			headers->clear();
			http.enumHeaders([&](ConstStrA key, ConstStrA value) {
				headers->add(key, this->factory->newValue(value));
				return false;
			});
		}
		http.close();
		return v;
	}
}


JSON::ConstValue CouchDB::jsonDELETE(ConstStrA path, JSON::Value headers, natural flags) {
	if (headers != null && headers->getType() != JSON::ndObject) {
		throw InvalidParamException(THISLOCATION,4,"Argument headers must be either null or an JSON object");
	}

	AutoArray<char, SmallAlloc<4096> > requestUrl;
	reqPathToFullPath(path,requestUrl);

	Synchronized<FastLock> _(lock);
	http.open(HttpClient::mDELETE, requestUrl);
	http.setHeader(HttpClient::fldAccept,"application/json");
	headers->enumEntries(JSON::IEntryEnum::lambda([this](const JSON::INode *nd, ConstStrA key, natural ){
		this->http.setHeader(key,nd->getStringUtf8());
		return false;
	}));

	SeqFileInput response = http.send();
	if (http.getStatus()/100 != 2) {

		JSON::Value errorVal;
		try{
			errorVal = factory->fromStream(response);
		} catch (...) {

		}
		http.close();
		throw RequestError(THISLOCATION,http.getStatus(), http.getStatusMessage(), errorVal);
	} else {
		JSON::Value v = factory->fromStream(response);
		if (flags & storeHeaders && headers != null) {
			headers->clear();
			http.enumHeaders([&](ConstStrA key, ConstStrA value) {
				headers->add(key, this->factory->newValue(value));
				return false;
			});
		}
		http.close();
		return v;
	}
}

JSON::ConstValue CouchDB::jsonPUTPOST(HttpClient::Method method, ConstStrA path, JSON::ConstValue data, JSON::Container headers, natural flags) {
	if (headers != null && headers->getType() != JSON::ndObject) {
		throw InvalidParamException(THISLOCATION,4,"Argument headers must be either null or an JSON object");
	}

	AutoArray<char, SmallAlloc<4096> > requestUrl;
	reqPathToFullPath(path,requestUrl);

	Synchronized<FastLock> _(lock);
	http.open(method, requestUrl);
	http.setHeader(HttpClient::fldAccept,"application/json");
	http.setHeader(HttpClient::fldContentType,"application/json");
	headers->enumEntries(JSON::IEntryEnum::lambda([this](const JSON::INode *nd, ConstStrA key, natural ){
		this->http.setHeader(key,nd->getStringUtf8());
		return false;
	}));

	SeqFileOutput out = http.beginBody(HttpClient::psoDefault);
	if (data != null) factory->toStream(*data, out);
	SeqFileInput response = http.send();
	if (http.getStatus()/100 != 2) {

		JSON::Value errorVal;
		try{
			errorVal = factory->fromStream(response);
		} catch (...) {

		}
		http.close();
		throw RequestError(THISLOCATION,http.getStatus(), http.getStatusMessage(), errorVal);
	} else {
		JSON::Value v = factory->fromStream(response);
		if (flags & storeHeaders && headers != null) {
			headers.clear();
			auto hb = json.object(headers);
			http.enumHeaders([&](ConstStrA key, ConstStrA value) {
				hb(key, value);
				return false;
			});
		}
		http.close();
		return v;
	}
}


JSON::ConstValue CouchDB::jsonPUT(ConstStrA path, JSON::ConstValue postData, JSON::Container headers, natural flags) {
	return jsonPUTPOST(HttpClient::mPUT,path,postData,headers,flags);
}
JSON::ConstValue CouchDB::jsonPOST(ConstStrA path, JSON::ConstValue postData, JSON::Container headers, natural flags) {
	return jsonPUTPOST(HttpClient::mPOST,path,postData,headers,flags);
}


UIDIterator CouchDB::genUID(natural count) {

	TextFormatBuff<char, StaticAlloc<32> > fmt;

	fmt("/_uuids?count=%1") << count;
	JSON::ConstValue uidlist = jsonGET(fmt.write());
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
	this->database = database;
}

ConstStrA CouchDB::getCurrentDB() const {
	return database;
}

StringA CouchDB::urlencode(ConstStrA text) {
	return convertString(UrlEncoder(), text);
}

void CouchDB::createDatabase() {
	jsonPUT(ConstStrA(),null);
}

void CouchDB::deleteDatabase() {
	jsonDELETE(ConstStrA(),null);
}

CouchDB::~CouchDB() {
}

enum ListenExceptionStop {listenExceptionStop};

natural CouchDB::listenChangesInternal(IChangeNotify &cb, natural fromSeq, const Filter &filter, ListenMode lm) {

	AutoArrayStream<char, SmallAlloc<4096> > gline;
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




	bool rep = true;
	do {

		if (listenExitFlag) break;

		gline.clear();
		TextOut<AutoArrayStream<char, SmallAlloc<4096> > &, StaticAlloc<256> > fmt(gline);
		fmt("%1/%2/_changes?since=%2") << baseUrl << database << fromSeq;
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

		JSON::Value v;
		{
			Synchronized<FastLock> _(lock);
			WHandle whandle(listenExitFlag);
			http.open(HttpClient::mGET,gline.getArray());
			http.setHeader(HttpClient::fldAccept,"application/json");
			SeqFileInput in = http.send();
			PNetworkStream conn = http.getConnection();

			if (lm != lmNoWait)
				conn->setWaitHandler(&whandle);

			try {
				JSON::Value v = factory->fromStream(in);
			} catch (ListenExceptionStop &) {
				listenExitFlag = false;
				http.closeConnection();
				return fromSeq;
			} catch (...) {
				listenExitFlag = false;
				http.closeConnection();
				throw;
			}

			if (lm != lmNoWait)
				conn->setWaitHandler(0);
			http.close();
		}


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



void CouchDB::stopListenChanges() {
	listenExitFlag = true;
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
	JSON::ConstValue res = jsonGET(fmt.write());
	Conflicts c;
	for (JSON::ConstIterator iter = res->getFwIter(); iter.hasItems();) {
		const JSON::ConstKeyValue &kv = iter.getNext();
		JSON::ConstValue doc = kv["ok"];
		if (doc != null) c.add(Document(doc));
	}
	return c;
}
Query CouchDB::createQuery(natural viewFlags) {
	View v("_all_docs", viewFlags);
	return createQuery(v);
}

JSON::ConstValue CouchDB::retrieveLocalDocument(ConstStrA localId) {
	TextFormatBuff<char, StaticAlloc<256> > fmt;
	StringA encdoc = urlencode(localId);
	fmt("_local/%1") << encdoc;
	return jsonGET(fmt.write(),null,refreshCache);

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

	JSON::Value h = json.object();
	JSON::Value v = jsonPUT(urlline.getArray(), null, h, storeHeaders);

	UpdateFnResult r;
	const JSON::INode *n = h->getPtr("X-Couch-Update-NewRev");
	if (n) {
		r.newRevID = n->getStringUtf8();
	}
	r.response = v;
	return r;


}


} /* namespace assetex */

