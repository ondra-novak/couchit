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
ConstStrA CouchDB::fldTimestamp("!timestamp");
ConstStrA CouchDB::fldPrevRevision("!prevRev");

typedef AutoArrayStream<char, SmallAlloc<256> > UrlLine;

static JSON::PFactory createFactory(JSON::PFactory jfact) {
	if (jfact != null) return jfact;
	else return JSON::create();
}

static lnatural getRandom() {
	SecureRandom srand;
	natural out;
	srand.blockRead(&out,sizeof(out));
	return out;
}


CouchDB::CouchDB(const Config& cfg)
	:json(createFactory(cfg.factory)),baseUrl(cfg.baseUrl),serverid(cfg.serverid),http(httpConfig),factory(json.factory)
	,cache(cfg.cache),seqNumSlot(0)
{
	if (!cfg.databaseName.empty()) use(cfg.databaseName);
	listenExitFlag = false;
	if (serverid.empty()) {
		//generate server id
		lnatural rnd = getRandom();
		TextFormatBuff<char, SmallAlloc<256> > fmt;
		fmt("%1") << setBase(62) << rnd;
		serverid = fmt.write();
	}
}


template<typename C>
void CouchDB::reqPathToFullPath(ConstStrA reqPath, C &output) {
	output.append(baseUrl);
	if (reqPath.head(1) == ConstStrA('/')) {
		output.append(reqPath.offset(1));
	} else {
		if (database.empty()) throw ErrorMessageException(THISLOCATION,"No database selected");

//			output.append(ConstStrA('/'));
		output.append(database);
		output.append(ConstStrA('/'));
		output.append(reqPath);
	}
}



JSON::ConstValue CouchDB::requestGET(ConstStrA path, JSON::Value headers, natural flags) {
	if (headers != null && headers->getType() != JSON::ndObject) {
		throw InvalidParamException(THISLOCATION,4,"Argument headers must be either null or an JSON object");
	}

	AutoArray<char, SmallAlloc<4096> > requestUrl;
	reqPathToFullPath(path,requestUrl);

	bool usecache = (flags & flgDisableCache) == 0;
	if (path.head(1) == ConstStrA('/')) usecache = false;
	if (!cache) usecache = false;

	//there will be stored cached item
	Optional<QueryCache::CachedItem> cachedItem;

	if (usecache) {
		cachedItem = cache->find(path);
		if (cachedItem->isDefined()) {
			if (seqNumSlot && *seqNumSlot == cachedItem->seqNum && (flags & flgRefreshCache) == 0)
				return cachedItem->value;
		}
	}

	Synchronized<FastLock> _(lock);
	http.open(HttpClient::mGET, requestUrl);
	http.setHeader(HttpClient::fldAccept,"application/json");
	if (cachedItem != nil) {
		http.setHeader(HttpClient::fldIfNoneMatch, cachedItem->etag);
	}
	if (headers!= null) headers->enumEntries(JSON::IEntryEnum::lambda([this](const JSON::INode *nd, ConstStrA key, natural ){
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
		if (flags & flgStoreHeaders && headers != null) {
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


JSON::ConstValue CouchDB::requestDELETE(ConstStrA path, JSON::Value headers, natural flags) {
	if (headers != null && headers->getType() != JSON::ndObject) {
		throw InvalidParamException(THISLOCATION,4,"Argument headers must be either null or an JSON object");
	}

	AutoArray<char, SmallAlloc<4096> > requestUrl;
	reqPathToFullPath(path,requestUrl);

	Synchronized<FastLock> _(lock);
	http.open(HttpClient::mDELETE, requestUrl);
	http.setHeader(HttpClient::fldAccept,"application/json");
	if (headers) headers->enumEntries(JSON::IEntryEnum::lambda([this](const JSON::INode *nd, ConstStrA key, natural ){
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
		if (flags & flgStoreHeaders && headers != null) {
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
	if (headers != null) headers->enumEntries(JSON::IEntryEnum::lambda([this](const JSON::INode *nd, ConstStrA key, natural ){
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
		if (flags & flgStoreHeaders && headers != null) {
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


JSON::ConstValue CouchDB::requestPUT(ConstStrA path, JSON::ConstValue postData, JSON::Container headers, natural flags) {
	return jsonPUTPOST(HttpClient::mPUT,path,postData,headers,flags);
}
JSON::ConstValue CouchDB::requestPOST(ConstStrA path, JSON::ConstValue postData, JSON::Container headers, natural flags) {
	return jsonPUTPOST(HttpClient::mPOST,path,postData,headers,flags);
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
	requestPUT(ConstStrA(),null);
}

void CouchDB::deleteDatabase() {
	requestDELETE(ConstStrA(),null);
}

CouchDB::~CouchDB() {
}

enum ListenExceptionStop {listenExceptionStop};

natural CouchDB::listenChangesInternal(IChangeNotify &cb, natural fromSeq, const Filter &filter, ListenMode lm) {

	AutoArrayStream<char, SmallAlloc<4096> > gline;
	StringA hlp;


	class WHandle: public INetworkResource::WaitHandler {
	public:
		bool &listenExitFlag;;

		virtual natural wait(const INetworkResource *resource, natural waitFor, natural ) const {
			while (!listenExitFlag) {
				if (limitTm.expired())
					return 0;
				//each 150 ms check exit flag.
				//TODO Find better solution later
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
		fmt("%1%%2/_changes?since=%3") << baseUrl << database << fromSeq;
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

			if (http.getStatus()/100 != 2) {

				JSON::Value errorVal;
				try{
					errorVal = factory->fromStream(in);
				} catch (...) {

				}
				http.close();
				throw RequestError(THISLOCATION,http.getStatus(), http.getStatusMessage(), errorVal);
			} else {


				PNetworkStream conn = http.getConnection();

				if (lm != lmNoWait)
					conn->setWaitHandler(&whandle);

				try {
					v = factory->fromStream(in);
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
/*
Conflicts CouchDB::loadConflicts(const Document &confDoc) {
	TextFormatBuff<char, StaticAlloc<256> > fmt;
	StringA docId = urlencode(confDoc["_id"].getStringA());
	StringA revList;
	JSON::Container allRevs = factory->array();
	allRevs.add(confDoc["_rev"]);
	allRevs.load(confDoc["_conflicts"]);
	revList= urlencode(factory->toString(*allRevs));
	fmt("%1?open_revs=%2") << docId << revList;
	JSON::ConstValue res = requestGET(fmt.write());
	Conflicts c;
	for (JSON::ConstIterator iter = res->getFwIter(); iter.hasItems();) {
		const JSON::ConstKeyValue &kv = iter.getNext();
		JSON::ConstValue doc = kv["ok"];
		if (doc != null) c.add(Document(doc));
	}
	return c;
}
*/
Query CouchDB::createQuery(natural viewFlags) {
	View v("_all_docs", viewFlags);
	return createQuery(v);
}

JSON::ConstValue CouchDB::retrieveLocalDocument(ConstStrA localId, natural flags) {
	TextFormatBuff<char, StaticAlloc<256> > fmt;
	StringA encdoc = urlencode(localId);
	fmt("_local/%1") << encdoc;
	return requestGET(fmt.write(),null,flgRefreshCache | (flags & flgDisableCache));

}
/*
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

	JSON::Container h = json.object();
	JSON::ConstValue v = requestPUT(urlline.getArray(), null, h, flgStoreHeaders);

	UpdateFnResult r;
	const JSON::INode *n = h->getPtr("X-Couch-Update-NewRev");
	if (n) {
		r.newRevID = n->getStringUtf8();
	}
	r.response = v;
	return r;


}

*/

UID CouchDB::getUID() {
	return UID(serverid,ConstStrA());
}

UID CouchDB::getUID(ConstStrA suffix) {
	return UID(serverid,suffix);
}

ConstValue CouchDB::retrieveDocument(ConstStrA docId, ConstStrA revId, natural flags) {
	UrlLine urlLine;
	TextOut<UrlLine &, SmallAlloc<256> > urlfmt(urlLine);
	FilterRead<ConstStrA::Iterator, UrlEncoder> docIdEnc(docId.getFwIter()), revIdEnc(revId.getFwIter());
	urlfmt("%1?rev=%2") << &docIdEnc << &revIdEnc;
	return requestGET(urlLine.getArray(),null, flags & (flgDisableCache|flgRefreshCache));
}

ConstValue CouchDB::retrieveDocument(ConstStrA docId, natural flags) {
	UrlLine urlLine;
	TextOut<UrlLine &, SmallAlloc<256> > urlfmt(urlLine);
	FilterRead<ConstStrA::Iterator, UrlEncoder> docIdEnc(docId.getFwIter());

	urlfmt("%1") << &docIdEnc;

	char c = '?';
		char d = '&';

		if (flags & flgAttachments) {
			urlfmt("%1attachments=true") << c;c=d;
		}
		if (flags & flgAttEncodingInfo) {
			urlfmt("%1att_encoding_info=true") << c;c=d;
		}
		if (flags & flgConflicts) {
			urlfmt("%1conflicts=true") << c;c=d;
		}
		if (flags & flgDeletedConflicts) {
			urlfmt("%1deleted_conflicts=true") << c;c=d;
		}
		if (flags & flgSeqNumber) {
			urlfmt("%1local_seq=true") << c;c=d;
		}
		if (flags & flgRevisions) {
			urlfmt("%1revs=true") << c;c=d;
		}
		if (flags & flgRevisionsInfo) {
			urlfmt("%1revs_info=true") << c;
		}

	return requestGET(urlLine.getArray(),null,flags & (flgDisableCache|flgRefreshCache));

}

CouchDB::UpdateResult CouchDB::updateDoc(ConstStrA updateHandlerPath, ConstStrA documentId,
		JSON::ConstValue arguments) {

	UrlLine urlline;
	TextOut<UrlLine &, SmallAlloc<256> > urlfmt(urlline);
	FilterRead<ConstStrA::Iterator, UrlEncoder> docIdEnc(documentId.getFwIter());
	urlfmt("%1/%2") << updateHandlerPath << &docIdEnc;
	if (arguments != null) {
		char c = '?';
		for (JSON::ConstIterator iter = arguments->getFwIter(); iter.hasItems();) {
			const JSON::ConstKeyValue &kv = iter.getNext();
			FilterRead<ConstStrA::Iterator, UrlEncoder> keyEnc(kv.getStringKey().getFwIter());
			ConstStrA valStr;
			if (kv->isString()) {
				valStr = kv.getStringA();
			} else {
				valStr = json.factory->toString(*kv);
			}
			FilterRead<ConstStrA::Iterator, UrlEncoder> valEnc(valStr.getFwIter());
			urlfmt("%1%%2=%3") << c << &keyEnc << &valEnc;
			c = '&';
		}
	}
	JSON::Container h = json.object();
	JSON::ConstValue v = requestPUT(urlline.getArray(), null, h, flgStoreHeaders);

	StringA newRev;
	const JSON::INode *n = h->getPtr("X-Couch-Update-NewRev");
	if (n) {
		newRev = n->getStringUtf8();
	}

	return UpdateResult(v,newRev);

}

ConstValue CouchDB::showDoc(ConstStrA showHandlerPath, ConstStrA documentId,
		JSON::ConstValue arguments, natural flags) {

	UrlLine urlline;
	TextOut<UrlLine &, SmallAlloc<256> > urlfmt(urlline);
	FilterRead<ConstStrA::Iterator, UrlEncoder> docIdEnc(documentId.getFwIter());
	urlfmt("%1/%2") << showHandlerPath << &docIdEnc;
	if (arguments != null) {
		char c = '?';
		for (JSON::ConstIterator iter = arguments->getFwIter(); iter.hasItems();) {
			const JSON::ConstKeyValue &kv = iter.getNext();
			FilterRead<ConstStrA::Iterator, UrlEncoder> keyEnc(kv.getStringKey().getFwIter());
			ConstStrA valStr;
			if (kv->isString()) {
				valStr = kv.getStringA();
			} else {
				valStr = json.factory->toString(*kv);
			}
			FilterRead<ConstStrA::Iterator, UrlEncoder> valEnc(valStr.getFwIter());
			urlfmt("%1%%2=%3") << c << &keyEnc << &valEnc;
			c = '&';
		}
	}
	JSON::ConstValue v = requestGET(urlline.getArray(),null,flags);

	return v;

}


StringA CouchDB::uploadAttachment(Document& document, ConstStrA attachmentName,ConstStrA contentType, const UploadFn& updateFn) {
	Synchronized<FastLock> _(lock);
	ConstStrA documentId = document.getID();
	ConstStrA revId = document.getRev();
	UrlLine urlline;
	TextOut<UrlLine &, SmallAlloc<256> > urlfmt(urlline);
	FilterRead<ConstStrA::Iterator, UrlEncoder> docIdEnc(documentId.getFwIter()),attNameEnc(attachmentName.getFwIter());
	urlfmt("%1/%2/%3/%4") << baseUrl << database << &docIdEnc << &attNameEnc;
	if (!revId.empty()) {
		FilterRead<ConstStrA::Iterator, UrlEncoder> revIdEnc(revId.getFwIter());
		urlfmt("?rev=%1") << &revIdEnc;
	}
	http.open(HttpClient::mPUT, urlline.getArray());
	http.setHeader(HttpClient::fldContentType, contentType);
	SeqFileOutput out = http.beginBody(HttpClient::psoDefault);
	updateFn(out);
	SeqFileInput in = http.send();
	if (http.getStatus() != 201) {

		JSON::Value errorVal;
		try{
			errorVal = factory->fromStream(in);
		} catch (...) {

		}
		http.close();
		throw RequestError(THISLOCATION,http.getStatus(), http.getStatusMessage(), errorVal);
	} else {
		JSON::Value v = factory->fromStream(in);
		http.close();
		return v["rev"].getStringA();
	}
}

void CouchDB::downloadAttachment(Document& document, ConstStrA attachmentName, const DownloadFn &downloadFn) {
	Synchronized<FastLock> _(lock);
	ConstStrA documentId = document.getID();
	ConstStrA revId = document.getRev();
	UrlLine urlline;
	TextOut<UrlLine &, SmallAlloc<256> > urlfmt(urlline);
	FilterRead<ConstStrA::Iterator, UrlEncoder> docIdEnc(documentId.getFwIter()),attNameEnc(attachmentName.getFwIter());
	urlfmt("%1/%2/%3/%4") << baseUrl << database << &docIdEnc << &attNameEnc;
	if (!revId.empty()) {
		FilterRead<ConstStrA::Iterator, UrlEncoder> revIdEnc(revId.getFwIter());
		urlfmt("?rev=%1") << &revIdEnc;
	}
	http.open(HttpClient::mGET, urlline.getArray());
	SeqFileInput in = http.send();
	if (http.getStatus() != 200) {

		JSON::Value errorVal;
		try{
			errorVal = factory->fromStream(in);
		} catch (...) {

		}
		http.close();
		throw RequestError(THISLOCATION,http.getStatus(), http.getStatusMessage(), errorVal);
	} else {
		HttpClient::HeaderValue ctx = http.getHeader(HttpClient::fldContentType);
		HttpClient::HeaderValue len = http.getHeader(HttpClient::fldContentLength);
		natural llen = naturalNull;
		if (len.defined) parseUnsignedNumber(len.getFwIter(), llen, 10);
		downloadFn(DownloadFile(in,ctx,llen));
	}
}

Value CouchDB::getUIDValue() {
	return json(ConstStrA(getUID()));
}

Value CouchDB::getUIDValue(ConstStrA suffix) {
	return json(ConstStrA(getUID(suffix)));
}

StringA CouchDB::uploadAttachment(Document& document, ConstStrA attachmentName,
		const AttachmentDataRef &data) {

	return uploadAttachment(document,attachmentName,data.contentType,UploadFn([&](SeqFileOutput out) {
		out.blockWrite(data,true);
	}));

}

AttachmentData CouchDB::downloadAttachment(Document& document,
		ConstStrA attachmentName) {

	StringB data;
	StringA contentType;
	downloadAttachment(document, attachmentName, [&](const DownloadFile &dw) {
		if (dw.contentLength == naturalNull) throw ErrorMessageException(THISLOCATION,"No content-length");
		StringB::WriteIterator wr= data.createBufferIter(dw.contentLength);
		wr.copy(dw,dw.contentLength);
		contentType = dw.contentType;
	});
	return AttachmentData(data,contentType);

}


Value CouchDB::newDocument() {
	return json("_id",ConstStrA(getUID()));
}

Value CouchDB::newDocument(ConstStrA suffix) {
	return json("_id",ConstStrA(getUID(suffix)));
}

} /* namespace assetex */
