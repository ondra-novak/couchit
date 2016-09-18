/*
 * couchDB.cpp
 *
 *  Created on: 8. 3. 2016
 *      Author: ondra
 */


#include "changeset.h"
#include "couchDB.h"
#include <lightspeed/base/containers/convertString.h>
#include <lightspeed/base/containers/stack.tcc>
#include <lightspeed/base/text/textFormat.tcc>
#include <lightspeed/base/text/textOut.tcc>
#include <lightspeed/mt/atomic.h>
#include <lightspeed/utils/json/jsonparser.h>
#include <lightspeed/utils/json/jsonserializer.tcc>
#include <lightspeed/utils/urlencode.h>
#include "lightspeed/base/streams/secureRandom.h"
#include "lightspeed/base/exceptions/errorMessageException.h"
#include "lightspeed/base/memory/poolalloc.h"
#include "lightspeed/base/streams/fileiobuff.tcc"
#include "lightspeed/base/containers/map.tcc"
#include "exception.h"
#include "query.h"

#include "lightspeed/base/streams/netio_ifc.h"

#include "changes.h"

#include "lightspeed/base/exceptions/netExceptions.h"

#include "conflictResolver.h"
#include "defaultUIDGen.h"
#include "queryCache.h"

#include "document.h"
using LightSpeed::INetworkServices;
using LightSpeed::JSON::serialize;
using LightSpeed::lockInc;
using LightSpeed::Stack;
namespace LightCouch {

CouchDB::HttpConfig::HttpConfig(const Config &cfg) {
	this->keepAlive = true;
	this->useHTTP10 = false;
	this->userAgent = ConstStrA("LightCouch/1.0 (+https://github.com/ondra-novak/lightcouch)");
	this->httpsProvider = cfg.httpsProvider;
	this->proxyProvider = cfg.proxyProvider;
	if (cfg.iotimeout != null) this->iotimeout = cfg.iotimeout;
}

ConstStrA CouchDB::fldTimestamp("!timestamp");
ConstStrA CouchDB::fldPrevRevision("!prevRev");

typedef AutoArrayStream<char, SmallAlloc<1024> > UrlLine;

static JSON::PFactory createFactory(JSON::PFactory jfact) {
	if (jfact != null) return jfact;
	else return JSON::create();
}

CouchDB::CouchDB(const Config& cfg)
	:json(createFactory(cfg.factory)),baseUrl(cfg.baseUrl),factory(json.factory)
	,cache(cfg.cache),seqNumSlot(0)
	,uidGen(cfg.uidgen == null?DefaultUIDGen::getInstance():*cfg.uidgen)
	,httpConfig(cfg),http(httpConfig)
{
	if (!cfg.databaseName.empty()) use(cfg.databaseName);
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
	bool redirectRetry = false;
	SeqFileInput response(NULL);
    do {
    	redirectRetry = false;
		http.setHeader(HttpClient::fldAccept,"application/json");
		if (cachedItem != nil) {
			http.setHeader(HttpClient::fldIfNoneMatch, cachedItem->etag);
		}
		if (headers!= null) headers->enumEntries(JSON::IEntryEnum::lambda([this](const JSON::INode *nd, ConstStrA key, natural ){
			this->http.setHeader(key,nd->getStringUtf8());
			return false;
		}));

		response = http.send();
		if (http.getStatus() == 304 && cachedItem != null) {
			http.close();
			return cachedItem->value;
		}
		if (http.getStatus() == 301 || http.getStatus() == 302 || http.getStatus() == 303 || http.getStatus() == 307) {
			HttpClient::HeaderValue val = http.getHeader(http.fldLocation);
			if (!val.defined) throw RequestError(THISLOCATION,requestUrl,http.getStatus(),http.getStatusMessage(), factory->newValue("Redirect without Location"));
			http.close();
			http.open(HttpClient::mGET, val);
			redirectRetry = true;
		}
    }
	while (redirectRetry);

	if (http.getStatus()/100 != 2) {

		JSON::Value errorVal;
		try{
			errorVal = factory->fromStream(response);
			//LightCouch's query server will return try_again in case that version mistmatch
			//This allows to start fresh version of query server. However we need to repeat the request
			//There is limit to repeat max 31x, then return error
			if (errorVal["error"].getStringA() == "try_again" && (flags & flgTryAgainCounterMask) != flgTryAgainCounterMask) {
				SyncReleased<FastLock> _(lock);
				return requestGET(path, headers, flags + flgTryAgainCounterStep);
			}
		} catch (...) {

		}
		http.close();
		throw RequestError(THISLOCATION,requestUrl,http.getStatus(), http.getStatusMessage(), errorVal);
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
		throw RequestError(THISLOCATION,requestUrl,http.getStatus(), http.getStatusMessage(), errorVal);
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
	if (data != null) {
		SeqTextOutA textout(out);
		JSON::serialize(data,textout,true);
	}
	SeqFileInput response = http.send();
	if (http.getStatus()/100 != 2) {

		JSON::Value errorVal;
		try{
			errorVal = factory->fromStream(response);
			//LightCouch's query server will return try_again in case that version mistmatch
			//This allows to start fresh version of query server. However we need to repeat the request
			//There is limit to repeat max 31x, then return error
			if (errorVal["error"].getStringA() == "try_again" && (flags & flgTryAgainCounterMask) != flgTryAgainCounterMask) {
				SyncReleased<FastLock> _(lock);
				return jsonPUTPOST(method,path, data,headers, flags + flgTryAgainCounterStep);
			}
		} catch (...) {

		}
		http.close();
		throw RequestError(THISLOCATION,requestUrl,http.getStatus(), http.getStatusMessage(), errorVal);
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


Query CouchDB::createQuery(const View &view) {
	return Query(*this, view);
}

Changeset CouchDB::createChangeset() {
	return Changeset(*this);
}




ConstValue CouchDB::getLastSeqNumber() {
	ChangesSink chsink = createChangesSink();
	Changes chgs = chsink.setFilterFlags(Filter::reverseOrder).limit(1).exec();
	if (chgs.hasItems()) return ChangedDoc(chgs.getNext()).seqId;
	else return null;
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

ConstStrA CouchDB::genUID() {
	return uidGen(uidBuffer,ConstStrA());
}

ConstStrA CouchDB::genUID(ConstStrA prefix) {
	return uidGen(uidBuffer,prefix);
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
		throw RequestError(THISLOCATION,urlline.getArray(),http.getStatus(), http.getStatusMessage(), errorVal);
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
		throw RequestError(THISLOCATION,urlline.getArray(),http.getStatus(), http.getStatusMessage(), errorVal);
	} else {
		HttpClient::HeaderValue ctx = http.getHeader(HttpClient::fldContentType);
		HttpClient::HeaderValue len = http.getHeader(HttpClient::fldContentLength);
		natural llen = naturalNull;
		if (len.defined) parseUnsignedNumber(len.getFwIter(), llen, 10);
		downloadFn(DownloadFile(in,ctx,llen));
	}
}

Value CouchDB::genUIDValue() {
	Synchronized<FastLock> _(lock);
	return json(genUID());
}

Value CouchDB::genUIDValue(ConstStrA prefix) {
	Synchronized<FastLock> _(lock);
	return json(genUID(prefix));
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
	Synchronized<FastLock> _(lock);
	return json("_id",genUID());
}

Value CouchDB::newDocument(ConstStrA prefix) {
	Synchronized<FastLock> _(lock);
	return json("_id",genUID(prefix));
}

template<typename T>
class DesignDocumentParse: public JSON::Parser<T> {
public:
	AutoArray<char> functionBuff;

	typedef JSON::Parser<T> Super;

	DesignDocumentParse(IIterator<char, T> &iter, JSON::PFactory factory)
		:Super(iter,factory) {}


	virtual Value parse() {
		const char *functionkw = "function";
		char x = Super::iter.peek();
		while (isspace(x)) {
			Super::iter.skip();
			x = Super::iter.peek();
		}
		if (x == 'f') {
			Super::iter.skip();
			x = Super::iter.getNext();
			if (x == 'a') {
				Super::parseCheck(JSON::strFalse+2);
				return JSON::getConstant(JSON::constFalse);
			} else if (x == 'u') {
				Super::parseCheck(functionkw+2);
				functionBuff.clear();
				functionBuff.append(ConstStrA(functionkw));

				AutoArray<char>::WriteIter wr = functionBuff.getWriteIterator();

				Stack<char, SmallAlloc<256> > levelStack;
				while(true) {
					char c = Super::iter.getNext();
					wr.write(c);
					if (c == '(' || c == '[' || c == '{') {
						levelStack.push(c);
					} else if (c == ')' || c == ']' || c == '}') {
						if (levelStack.empty() ||
								(c == ')' && levelStack.top() != '(') ||
								(c == '}' && levelStack.top() != '{') ||
								(c == ']' && levelStack.top() != '['))
							throw JSON::ParseError_t(THISLOCATION,ConstStrA(functionBuff));
						levelStack.pop();
						if (levelStack.empty() && c == '}') break;
					} else if (c == '"') {
						Super::parseRawString();
						for (natural i = 0; i < Super::strBuff.length(); i++) {
							JSON::Serializer<AutoArray<char>::WriteIter>::writeChar(Super::strBuff[i],wr);
						}
						wr.write(c);
					}
				}
				return Super::factory->newValue(ConstStrA(functionBuff));
			} else {
				throw JSON::ParseError_t(THISLOCATION,"expected 'false' or 'function' ");
			}
		} else {
			return Super::parse();
		}
	}
};

static const ConstStrA _designSlash("_design/");



bool CouchDB::uploadDesignDocument(ConstValue content, DesignDocUpdateRule updateRule, ConstStrA name) {


	class DDResolver: public ConflictResolver {
	public:
		DDResolver(CouchDB &db, bool attachments, DesignDocUpdateRule rule):
			ConflictResolver(db,attachments),rule(rule) {}

		virtual ConstValue resolveConflict(Document &doc, const Path &path,
				const ConstValue &leftValue, const ConstValue &rightValue) {

			switch (rule) {
				case ddurMergeSkip: return leftValue;
				case ddurMergeOverwrite: return rightValue;
				default: return ConflictResolver::resolveConflict(doc,path,leftValue,rightValue);
			}
		}
		virtual bool isEqual(const ConstValue &leftValue, const ConstValue &rightValue) {
			Document doc(leftValue);
			ConstValue res = ConflictResolver::makeDiffObject(doc,Path::root, leftValue, rightValue);
			return res == null;

		}

	protected:
		DesignDocUpdateRule rule;
	};

	if (name.empty()) {
		name = content["_id"].getStringA();
	} else if (name.head(8) != _designSlash) {
		AutoArray<char, SmallAlloc<256> > newName;
		newName.append(_designSlash);
		newName.append(name);
		return uploadDesignDocument(content,updateRule,name);
	}

	Changeset chset = createChangeset();

	try {
		DDResolver resolver(*this,true,updateRule);

		Document curddoc = this->retrieveDocument(name,0);

		///design document already exists, skip uploading
		if (updateRule == ddurSkipExisting) return false;

		///no change in design document, skip uploading
		if (resolver.isEqual(curddoc, content)) return false;

		if (updateRule == ddurCheck) {
			UpdateException::ErrorItem errItem;
			errItem.document = content;
			errItem.errorDetails = json.object();
			errItem.errorType = "conflict";
			errItem.reason = "ddurCheck in effect, cannot update design document";
			throw UpdateException(THISLOCATION,ConstStringT<UpdateException::ErrorItem>(&errItem,1));
		}

		if (updateRule == ddurOverwrite) {
			curddoc.setContent(json, content,1);
			chset.update(curddoc);
		} else {
			Value v = resolver.merge3w(curddoc,content,json.object());
			curddoc.setRev(v);
			chset.update(curddoc);
		}



	} catch (HttpStatusException &e) {
		if (e.getStatus() == 404) {
			Document ddoc(content);
			ddoc.edit(json);
			ddoc.setID(json(name));
			chset.update(ddoc);
		} else {
			throw;
		}
	}


	try {
		chset.commit(false);
	} catch (UpdateException &e) {
		if (e.getErrors()[0].errorType == "conflict") {
			return uploadDesignDocument(content,updateRule,name);
		}
	}
	return true;

}

template<typename T>
JSON::Value parseDesignDocument(IIterator<char, T> &stream, JSON::PFactory factory) {
	DesignDocumentParse<T> parser(stream, factory);
	return parser.parse();
}

bool CouchDB::uploadDesignDocument(ConstStrW pathname, DesignDocUpdateRule updateRule, ConstStrA name) {

	SeqFileInBuff<> infile(pathname,0);
	SeqTextInA intextfile(infile);
	return uploadDesignDocument(parseDesignDocument(intextfile,json.factory), updateRule, name);

}

bool  CouchDB::uploadDesignDocument(const char* content,
		natural contentLen, DesignDocUpdateRule updateRule, ConstStrA name) {

	ConstStrA ctt(content, contentLen);
	ConstStrA::Iterator iter = ctt.getFwIter();
	return uploadDesignDocument(parseDesignDocument(iter,json.factory), updateRule, name);

}

Changes CouchDB::receiveChanges(ChangesSink& sink) {
	UrlLine reqline;
	reqPathToFullPath("_changes",reqline.getArray());
	TextOut<UrlLine &, SmallAlloc<256> > urlfmt(reqline);

	natural qpos = reqline.length();

	if (sink.seqNumber != null) {
		ConvertReadIter<UrlEncodeConvert, ConstStrA::Iterator> seqNumb(sink.seqNumber.getStringA().getFwIter());
		urlfmt("&since=%1") << &seqNumb;
	}
	if (sink.outlimit != naturalNull) {
		urlfmt("&limit=%1") << sink.outlimit;
	}
	if (sink.timeout > 0) {
		urlfmt("&feed=longpoll");
		if (sink.timeout == naturalNull) {
			urlfmt("&heartbeat=true");
		} else {
			urlfmt("&timeout=%1") << sink.timeout;
		}
	}
	if (sink.filter != null) {
		const Filter &flt = sink.filter;
		if (!flt.viewPath.empty()) {
			ConstStrA fltpath = flt.viewPath;
			ConstStrA ddocName;
			ConstStrA filterName;
			bool isView = false;
			ConstStrA::SplitIterator iter = fltpath.split('/');
			ConstStrA x = iter.getNext();
			if (x == "_design") x = iter.getNext();
			ddocName = x;
			x = iter.getNext();
			if (x == "_view") {
				isView = true;
				x = iter.getNext();
			}
			filterName = x;



			if (isView) {
				urlfmt("&filter=_view&view=%1/%2") << ddocName << filterName;
			} else {
				urlfmt("&filter=%1/%2") << ddocName << filterName;
			}
		}
		if (flt.flags & Filter::allRevs) urlfmt("&style=all_docs");
		if (flt.flags & Filter::includeDocs) {
			urlfmt("&include_docs=true");
			if (flt.flags & Filter::attachments) {
				urlfmt("&attachments=true");
			}
			if (flt.flags & Filter::conflicts) {
				urlfmt("&conflicts=true");
			}
			if (flt.flags & Filter::attEncodingInfo) {
				urlfmt("&att_encoding_info=true");
			}
		}
		if (flt.flags & Filter::reverseOrder) {
			urlfmt("&descending=true");
		}
		for (natural i = 0; i<flt.args.length(); i++) {
			ConvertReadIter<UrlEncodeConvert,StringA::Iterator> cnv(flt.args[i].value.getFwIter());
			urlfmt("&%1=%2") << flt.args[i].key << &cnv;
		}
	}
	if (sink.filterArgs != null) {
		sink.filterArgs->forEach([&](ConstValue v, ConstStrA key, natural) {
			if (v->isString()) {
				ConvertReadIter<UrlEncodeConvert,ConstStrA::Iterator> cnv(v.getStringA().getFwIter());
				urlfmt("&%1=%2") << key << &cnv;
			} else {
				urlfmt("&%1=") << key;
				JSON::serialize(v, reqline, true);
			}
			return false;
		});
	}

	if (lockCompareExchange(sink.cancelState,1,0)) {
		throw CanceledException(THISLOCATION);
	}

	class WHandle: public INetworkResource::WaitHandler {
	public:
		atomic &cancelState;

		virtual natural wait(const INetworkResource *resource, natural waitFor, natural ) const {
			for(;;) {
				if (lockCompareExchange(cancelState,1,0)) {
					throw CanceledException(THISLOCATION);
				}
				if (limitTm.expired())
					return 0;
				//each 200 ms check exit flag.
				//TODO Find better solution later
				natural r = INetworkResource::WaitHandler::wait(resource,waitFor,200);
				if (r) {
					limitTm = Timeout(100000);
					return r;
				}
			}
		}

		WHandle(atomic &cancelState):cancelState(cancelState),limitTm(100000) {}
		mutable Timeout limitTm;
	};

	if (qpos < reqline.length()) reqline.getArray().set(qpos,'?');

	Synchronized<FastLock> _(lock);
	WHandle whandle(sink.cancelState);
	http.open(HttpClient::mGET,reqline.getArray());
	http.setHeader(HttpClient::fldAccept,"application/json");
	SeqFileInput in = http.send();

	ConstValue v;
	if (http.getStatus()/100 != 2) {

		JSON::Value errorVal;
		try{
			errorVal = factory->fromStream(in);
		} catch (...) {

		}
		http.close();
		throw RequestError(THISLOCATION,reqline.getArray(),http.getStatus(), http.getStatusMessage(), errorVal);
	} else {


		PNetworkStream conn = http.getConnection();

		if (sink.timeout)
			conn->setWaitHandler(&whandle);

		try {
			v = factory->fromStream(in);
		} catch (JSON::ParseError_t &e) {
			//terminate http connection
			http.closeConnection();
			//canceled exception can be stored in reason of ParseError_t exception
			const Exception *r = e.getReason();
			//process all reasons
			while (r) {
				//test the reason
				const CanceledException *ce = dynamic_cast<const CanceledException *>(r);
				//throw reason only
				if (ce) throw *ce;
				//continue reading reasons
				r = r->getReason();
			}

			//in case not found, throw what you have
			throw;

		} catch (...) {
			//any exception
			//terminate connection
			http.closeConnection();
			//throw it
			throw;
		}

		if (sink.timeout)
			conn->setWaitHandler(0);
		http.close();
	}


	ConstValue results=v["results"];
	sink.seqNumber = v["last_seq"];
	if (seqNumSlot) *seqNumSlot = sink.seqNumber;

	return results;
}

ChangesSink CouchDB::createChangesSink() {
	return ChangesSink(*this);
}

} /* namespace assetex */

