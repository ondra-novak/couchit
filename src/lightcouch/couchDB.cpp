/*
 * couchDB.cpp
 *
 *  Created on: 8. 3. 2016
 *      Author: ondra
 */


#include "changeset.h"
#include "couchDB.h"
#include <immujson/json.h>
#include <lightspeed/base/containers/convertString.h>
#include <lightspeed/base/containers/stack.tcc>
#include <lightspeed/base/exceptions/throws.tcc>
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
#include "query.h"

#include "lightspeed/base/streams/netio_ifc.h"

#include "changes.h"

#include "lightspeed/base/exceptions/netExceptions.h"

#include "conflictResolver.h_"
#include "defaultUIDGen.h"
#include "queryCache.h"

#include "document.h"

using LightSpeed::lockCompareExchange;
namespace LightCouch {



CouchDB::HttpConfig::HttpConfig(const Config &cfg) {
	this->keepAlive = true;
	this->useHTTP10 = false;
	this->userAgent = StringRef("LightCouch/1.0 (+https://github.com/ondra-novak/lightcouch)");
	this->httpsProvider = cfg.httpsProvider;
	this->proxyProvider = cfg.proxyProvider;
	if (cfg.iotimeout != null) this->iotimeout = cfg.iotimeout;
}

StringRef CouchDB::fldTimestamp("~timestamp");
StringRef CouchDB::fldPrevRevision("~prevRev");
natural CouchDB::maxSerializedKeysSizeForGETRequest = 1024;




CouchDB::CouchDB(const Config& cfg)
	:baseUrl(cfg.baseUrl)
	,cache(cfg.cache)
	,uidGen(cfg.uidgen == null?DefaultUIDGen::getInstance():*cfg.uidgen)
	,httpConfig(cfg),http(httpConfig)
	,queryable(*this)
{
	if (!cfg.databaseName.empty()) use(cfg.databaseName);
}


static bool isRelativePath(const StringRef &path) {
	return path.substr(0,7) != "http://" && path.substr(0,8) != "https://";
}

Value CouchDB::requestGET(const StringRef &path, Value *headers, natural flags) {

	if (isRelativePath(path)) return requestGET(*getUrlBuilder(path),headers,flags);

	if (headers != nullptr && headers->type() != json::object) {
		throw InvalidParamException(THISLOCATION,4,"Argument headers must be either null or an JSON object");
	}

	Synchronized<FastLock> _(lock);

	std::size_t baseUrlLen = baseUrl.length();
	std::size_t databaseLen = database.length();
	std::size_t prefixLen = baseUrlLen+databaseLen+1;

	bool usecache = (flags & flgDisableCache) == 0;
	if (usecache && (!cache
		|| path.substr(0,baseUrlLen) != baseUrl
		|| path.substr(baseUrlLen, databaseLen) != database)) {
		usecache = false;
	}

	StringRef cacheKey = path.substr(prefixLen);

	//there will be stored cached item
	Optional<QueryCache::CachedItem> cachedItem;

	if (usecache) {
		cachedItem = cache->find(cacheKey);
	}

	http.open(HttpClient::mGET, path);
	bool redirectRetry = false;
	SeqFileInput response(NULL);
    do {
    	redirectRetry = false;
		http.setHeader(HttpClient::fldAccept,"application/json");
		if (cachedItem != nil && cachedItem->isDefined()) {
			http.setHeader(HttpClient::fldIfNoneMatch, cachedItem->etag);
		}
		if (headers!= nullptr)
			headers->forEach([this](Value v){
				this->http.setHeader(StringRef(v.getKey()),StringRef(v.getString()));
				return true;
			});

		response = http.send();
		if (http.getStatus() == 304 && cachedItem != null) {
			http.close();
			return cachedItem->value;
		}
		if (http.getStatus() == 301 || http.getStatus() == 302 || http.getStatus() == 303 || http.getStatus() == 307) {
			HttpClient::HeaderValue val = http.getHeader(http.fldLocation);
			if (!val.defined) throw RequestError(THISLOCATION,ConstStrA(path),http.getStatus(),http.getStatusMessage(), Value("Redirect without Location"));
			http.close();
			http.open(HttpClient::mGET, val);
			redirectRetry = true;
		}
    }
	while (redirectRetry);

    auto readStreamFn = [&response](){return response.getNext();};

	if (http.getStatus()/100 != 2) {

		Value errorVal;
		try{
			errorVal = Value::parse(readStreamFn);
		} catch (...) {

		}
		http.close();
		throw RequestError(THISLOCATION,ConstStrA(path),http.getStatus(), http.getStatusMessage(), errorVal);
	} else {
		Value v = Value::parse(readStreamFn);
		if (usecache) {
			BredyHttpSrv::HeaderValue fld = http.getHeader(HttpClient::fldETag);
			if (fld.defined) {
				cache->set(cacheKey, QueryCache::CachedItem(StringRef(fld), v));
			}
		}
		if (flags & flgStoreHeaders && headers != nullptr) {
			Object obj;
			http.enumHeaders([&](StringRef key, StringRef value) {
				obj(key,value);
				return false;
			});
			*headers = obj;
		}
		http.close();
		return v;
	}
}

Value CouchDB::requestDELETE(const StringRef &path, Value *headers, natural flags) {

	if (isRelativePath(path)) return requestDELETE(*getUrlBuilder(path),headers,flags);

	if (headers != nullptr && headers->type() != json::object) {
		throw InvalidParamException(THISLOCATION,4,"Argument headers must be either null or an JSON object");
	}

	Synchronized<FastLock> _(lock);
	http.open(HttpClient::mDELETE, path);
	http.setHeader(HttpClient::fldAccept,"application/json");
	if (headers) headers->forEach([this](Value v){
		this->http.setHeader(StringRef(v.getKey()),StringRef(v.getString()));
		return true;
	});


	SeqFileInput response = http.send();
    auto readStreamFn = [&response](){return response.getNext();};
	if (http.getStatus()/100 != 2) {

		Value errorVal;
		try{
			errorVal = Value::parse(readStreamFn);
		} catch (...) {

		}
		http.close();
		throw RequestError(THISLOCATION,ConstStrA(path),http.getStatus(), http.getStatusMessage(), errorVal);
	} else {
		Value v = Value::parse(readStreamFn);
		if (flags & flgStoreHeaders && headers != nullptr) {
			Object obj;
			http.enumHeaders([&](StringRef key, StringRef value) {
				obj(key, value);
				return false;
			});
			*headers = obj;
		}
		http.close();
		return v;
	}
}

Value CouchDB::jsonPUTPOST(HttpClient::Method method, const StringRef &path, Value data, Value *headers, natural flags) {

	if (isRelativePath(path)) return jsonPUTPOST(method,*getUrlBuilder(path),data,headers,flags);

	if (headers != nullptr && headers->type() != json::object) {
		throw InvalidParamException(THISLOCATION,4,"Argument headers must be either null or an JSON object");
	}


	Synchronized<FastLock> _(lock);
	http.open(method, path);
	http.setHeader(HttpClient::fldAccept,"application/json");
	http.setHeader(HttpClient::fldContentType,"application/json");
	if (headers != nullptr)
		headers->forEach([this](Value v){
		this->http.setHeader(StringRef(v.getKey()),StringRef(v.getString()));
		return true;
	});

	SeqFileOutput out = http.beginBody(HttpClient::psoDefault);
	if (data.type() != json::undefined) {
		data.serialize([&out](char c) {out.write(c);});
	}
	SeqFileInput response = http.send();

    auto readStreamFn = [&response](){return response.getNext();};


	if (http.getStatus()/100 != 2) {

		Value errorVal;
		try{
			errorVal = Value::parse(readStreamFn);
		} catch (...) {

		}
		http.close();
		throw RequestError(THISLOCATION,ConstStrA(path),http.getStatus(), http.getStatusMessage(), errorVal);
	} else {
		Value v = Value::parse(readStreamFn);
		if (flags & flgStoreHeaders && headers != nullptr) {
			Object obj;
			http.enumHeaders([&](StringRef key, StringRef value) {
				obj(key, value);
				return false;
			});
			*headers = obj;
		}
		http.close();
		return v;
	}
}


Value CouchDB::requestPUT(const StringRef &path, const Value &postData, Value *headers, natural flags) {
	return jsonPUTPOST(HttpClient::mPUT,path,postData,headers,flags);
}
Value CouchDB::requestPOST(const StringRef &path, const Value &postData, Value *headers, natural flags) {
	return jsonPUTPOST(HttpClient::mPOST,path,postData,headers,flags);
}




void CouchDB::use(String database) {
	this->database = database;
}

String CouchDB::getCurrentDB() const {
	return database;
}


void CouchDB::createDatabase() {
	PUrlBuilder url = getUrlBuilder("");
	requestPUT(url->toString(),Value());
}

void CouchDB::deleteDatabase() {
	PUrlBuilder url = getUrlBuilder("");
	requestDELETE(url->toString(),nullptr);
}

CouchDB::~CouchDB() {
}

enum ListenExceptionStop {listenExceptionStop};


Query CouchDB::createQuery(const View &view) {
	return Query(view, queryable);
}

Changeset CouchDB::createChangeset() {
	return Changeset(*this);
}




Value CouchDB::getLastSeqNumber() {
	ChangesSink chsink = createChangesSink();
	Changes chgs = chsink.setFilterFlags(Filter::reverseOrder).limit(1).exec();
	if (chgs.hasItems()) return ChangedDoc(chgs.getNext()).seqId;
	else return nullptr;
}



Query CouchDB::createQuery(natural viewFlags) {
	View v("_all_docs", viewFlags);
	return createQuery(v);
}

Value CouchDB::retrieveLocalDocument(const StringRef &localId, natural flags) {
	PUrlBuilder url = getUrlBuilder("_local");
	url->add(localId);
	return requestGET(url->toString(),nullptr, flags & (flgDisableCache|flgRefreshCache));
}

StringRef CouchDB::genUID() {
	return uidGen(uidBuffer,StringRef());
}

StringRef CouchDB::genUID(StringRef prefix) {
	return uidGen(uidBuffer,prefix);
}

Value CouchDB::retrieveDocument(const StringRef &docId, const StringRef & revId, natural flags) {
	PUrlBuilder url = getUrlBuilder("");
	url->add(docId).add("rev",revId);
	return requestGET(*url,nullptr, flags & (flgDisableCache|flgRefreshCache));
}

Value CouchDB::retrieveDocument(const StringRef &docId, natural flags) {
	PUrlBuilder url = getUrlBuilder("");
	url->add(docId);

	if (flags & flgAttachments) url->add("attachments","true");
	if (flags & flgAttEncodingInfo) url->add("att_encoding_info","true");
	if (flags & flgConflicts) url->add("conflicts","true");
	if (flags & flgDeletedConflicts) url->add("deleted_conflicts","true");
	if (flags & flgSeqNumber) url->add("local_seq","true");
	if (flags & flgRevisions) url->add("revs","true");
	if (flags & flgRevisionsInfo) url->add("revs_info","true");

	return requestGET(*url,nullptr,flags & (flgDisableCache|flgRefreshCache));

}

CouchDB::UpdateResult CouchDB::updateDoc(StringRef updateHandlerPath, StringRef documentId,
		Value arguments) {
	PUrlBuilder url = getUrlBuilder(updateHandlerPath);
	url->add(documentId);
	for (auto &&v:arguments) {
		StringRef key = v.getKey();
		String vstr = v.toString();
		url->add(key,vstr);
	}
	Value h;
	Value v = requestPUT(*url, nullptr, &h, flgStoreHeaders);
	return UpdateResult(v,h["X-Couch-Update-NewRev"]);

}

Value CouchDB::showDoc(const StringRef &showHandlerPath, const StringRef &documentId,
		const Value &arguments, natural flags) {

	PUrlBuilder url = getUrlBuilder(showHandlerPath);
	url->add(documentId);
	for (auto &&v:arguments) {
		StringRef key = v.getKey();
		String vstr = v.toString();
		url->add(key,vstr);
	}
	Value v = requestGET(*url,nullptr,flags);
	return v;

}


Upload CouchDB::uploadAttachment(const Value &document, const StringRef &attachmentName, const StringRef &contentType) {

	StringRef documentId = document["_id"].getString();
	StringRef revId = document["_rev"].getString();
	PUrlBuilder url = getUrlBuilder("");
	url->add(documentId);
	url->add(attachmentName);
	url->add("rev",revId);

	//the lock is unlocked in the UploadClass
	lock.lock();

	class UploadClass: public Upload::Target {
	public:
		UploadClass(FastLock &lock, HttpClient &http, const PUrlBuilder &urlline)
			:lock(lock)
			,http(http)
			,out(http.beginBody(HttpClient::psoDefault))
			,urlline(urlline)
			,finished(false) {
		}

		~UploadClass() noexcept(false) {
			//if not finished, finish now
			if (!finished) finish();
		}

		virtual void operator()(const void *buffer, std::size_t size) {
			out.blockWrite(buffer,size,true);
		}

		String finish() {
			try {
				if (finished) return response;
				finished = true;

				SeqFileInput in = http.send();
				auto responseData = [&in](){return in.getNext();};
				if (http.getStatus() != 201) {


					Value errorVal;
					try{
						errorVal = Value::parse(responseData);
					} catch (...) {

					}
					http.close();
					StringRef url(*urlline);
					throw RequestError(THISLOCATION,ConstStrA(url),http.getStatus(), http.getStatusMessage(), errorVal);
				} else {
					Value v = Value::parse(responseData);
					http.close();
					response = v["rev"];
					lock.unlock();
					return response;
				}
			} catch (...) {
				lock.unlock();
				throw;
			}
		}

	protected:
		FastLock &lock;
		HttpClient &http;
		SeqFileOutput out;
		Value response;
		PUrlBuilder urlline;
		bool finished;
	};


	try {
		//open request
		http.open(HttpClient::mPUT, StringRef(*url));
		//send header
		http.setHeader(HttpClient::fldContentType, contentType);
		//create upload object
		return Upload(new UploadClass(lock,http,url));
	} catch (...) {
		//anywhere can exception happen, then unlock here and throw exception
		lock.unlock();
		throw;
	}


}

String CouchDB::uploadAttachment(const Value &document, const StringRef &attachmentName, const AttachmentDataRef &attachmentData) {

	Upload upld = uploadAttachment(document,attachmentName,attachmentData.contentType);
	upld.write(attachmentData.data(), attachmentData.length());
	return upld.finish();
}


Value CouchDB::genUIDValue() {
	Synchronized<FastLock> _(lock);
	return StringRef(genUID());
}

Value CouchDB::genUIDValue(StringRef prefix) {
	Synchronized<FastLock> _(lock);
	return StringRef(genUID(prefix));
}


Document CouchDB::newDocument() {
	Synchronized<FastLock> _(lock);
	return Document(genUID(),StringRef());
}

Document CouchDB::newDocument(const StringRef &prefix) {
	Synchronized<FastLock> _(lock);
	return Document(genUID(),StringRef(prefix));
}

template<typename Fn>
class DesignDocumentParse: public json::Parser<Fn> {
public:
	AutoArray<char> functionBuff;

	typedef json::Parser<Fn> Super;

	DesignDocumentParse(const Fn &source)
		:Super(source) {}


	virtual json::Value parse() {
		const char *functionkw = "function";
		const char *falsekw = "false";
		char x = Super::rd.next();
		if (x == 'f') {
			Super::rd.commit();
			x = Super::rd.next();
			if (x == 'a') {
				Super::checkString(falsekw+1);
				return Value(false);
			} else if (x == 'u') {
				Super::checkString(functionkw+1);

				functionBuff.clear();
				functionBuff.append(ConstStrA(functionkw));

				AutoArray<char>::WriteIter wr = functionBuff.getWriteIterator();

				Stack<char, SmallAlloc<256> > levelStack;
				while(true) {
					char c = Super::rd.nextCommit();
					wr.write(c);
					if (c == '(' || c == '[' || c == '{') {
						levelStack.push(c);
					} else if (c == ')' || c == ']' || c == '}') {
						if (levelStack.empty() ||
								(c == ')' && levelStack.top() != '(') ||
								(c == '}' && levelStack.top() != '{') ||
								(c == ']' && levelStack.top() != '['))
							throw json::ParseError(std::string(functionBuff.data(),functionBuff.length()));
						levelStack.pop();
						if (levelStack.empty() && c == '}') break;
					} else if (c == '"') {
						json::Value v = Super::parseString();
						v.serialize([&](char c) {wr.write(c);});
					}
				}
				return json::Value(StringRef(functionBuff));
			} else {
				throw json::ParseError("Unexpected token");
			}
		} else {
			return Super::parse();
		}
	}
};

static const StringRef _designSlash("_design/");



bool CouchDB::uploadDesignDocument(const Value &content, DesignDocUpdateRule updateRule) {

/*
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
	*/

	Changeset chset = createChangeset();

	Document newddoc = content;
	try {
		Document curddoc = this->retrieveDocument(content["_id"].getString(),0);
		newddoc.setRev(curddoc.getRev());

		///design document already exists, skip uploading
		if (updateRule == ddurSkipExisting) return false;

		///no change in design document, skip uploading
		if (Value(curddoc) == Value(newddoc)) return false;

		if (updateRule == ddurCheck) {
			UpdateException::ErrorItem errItem;
			errItem.document = content;
			errItem.errorDetails = Object();
			errItem.errorType = "conflict";
			errItem.reason = "ddurCheck in effect, cannot update design document";
			throw UpdateException(THISLOCATION,ConstStringT<UpdateException::ErrorItem>(&errItem,1));
		}

		if (updateRule == ddurOverwrite) {
			chset.update(newddoc);
		} else {
			throwUnsupportedFeature(THISLOCATION,this,"Merge design document");
		}



	} catch (HttpStatusException &e) {
		if (e.getStatus() == 404) {
			chset.update(content);
		} else {
			throw;
		}
	}


	try {
		chset.commit(false);
	} catch (UpdateException &e) {
		if (e.getErrors()[0].errorType == "conflict") {
			return uploadDesignDocument(content,updateRule);
		}
	}
	return true;

}

template<typename T>
Value parseDesignDocument(IIterator<char, T> &stream) {
	auto reader = [&](){return stream.getNext();};
	DesignDocumentParse<decltype(reader)> parser(reader);
	return parser.parse();
}

bool CouchDB::uploadDesignDocument(ConstStrW pathname, DesignDocUpdateRule updateRule) {

	SeqFileInBuff<> infile(pathname,0);
	SeqTextInA intextfile(infile);
	return uploadDesignDocument(parseDesignDocument(intextfile), updateRule);

}

bool  CouchDB::uploadDesignDocument(const char* content,
		natural contentLen, DesignDocUpdateRule updateRule) {

	StringRef ctt(content, contentLen);
	StringRef::Iterator iter = ctt.getFwIter();
	return uploadDesignDocument(parseDesignDocument(iter), updateRule);

}

Changes CouchDB::receiveChanges(ChangesSink& sink) {

	PUrlBuilder url = getUrlBuilder("_changes");

	if (sink.seqNumber.defined()) {
		url->add("since", sink.seqNumber.toString());
	}
	if (sink.outlimit != naturalNull) {
		url->add("limit",ToString<natural>(sink.outlimit));
	}
	if (sink.timeout > 0) {
		url->add("feed","longpoll");
		if (sink.timeout == naturalNull) {
			url->add("heartbeat","true");
		} else {
			url->add("timeout",ToString<natural>(sink.timeout));
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

			String fpath({StringRef(ddocName),"/",StringRef(filterName)});

			if (isView) {
				url->add("filter","_view");
				url->add("view",fpath);
			} else {
				url->add("filter",fpath);
			}
		}
		if (flt.flags & Filter::allRevs) url->add("style","all_docs");
		if (flt.flags & Filter::includeDocs) {
			url->add("include_docs","true");
			if (flt.flags & Filter::attachments) {
				url->add("attachments","true");
			}
			if (flt.flags & Filter::conflicts) {
				url->add("conflicts","true");
			}
			if (flt.flags & Filter::attEncodingInfo) {
				url->add("att_encoding_info","true");
			}
		}
		if (flt.flags & Filter::reverseOrder) {
			url->add("descending","true");
		}
		for (auto &&itm: flt.args) {
			if (!sink.filterArgs[itm.getKey()].defined()) {
				url->add(StringRef(itm.getKey()),StringRef(itm.toString()));
			}
		}
	}
	for (auto &&v : sink.filterArgs) {
			String val = v.toString();
			StringRef key = v.getKey();
			url->add(key,val);
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

	Synchronized<FastLock> _(lock);
	WHandle whandle(sink.cancelState);
	http.open(HttpClient::mGET,*url);
	http.setHeader(HttpClient::fldAccept,"application/json");
	SeqFileInput in = http.send();

	auto response = [&](){return in.getNext();};

	Value v;
	if (http.getStatus()/100 != 2) {

		Value errorVal;
		try{
			errorVal = Value::parse(response);
		} catch (...) {

		}
		http.close();
		throw RequestError(THISLOCATION,ConstStrA(*url),http.getStatus(), http.getStatusMessage(), errorVal);
	} else {


		PNetworkStream conn = http.getConnection();

		if (sink.timeout)
			conn->setWaitHandler(&whandle);

		try {

			v = Value::parse(response);

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


	Value results=v["results"];
	sink.seqNumber = v["last_seq"];

	return results;
}

ChangesSink CouchDB::createChangesSink() {
	return ChangesSink(*this);
}

class EmptyDownload: public Download::Source {
public:
	virtual natural operator()(void *, std::size_t ) {return 0;}
};

class StreamDownload: public Download::Source {
public:
	StreamDownload(const PInputStream &stream, FastLock &lock, HttpClient &http):stream(stream),lock(lock),http(http) {}
	virtual natural operator()(void *buffer, std::size_t size) {
		return stream->read(buffer,size);
	}
	~StreamDownload() {
		http.close();
		lock.unlock();
	}

protected:
	PInputStream stream;
	FastLock &lock;
	HttpClient &http;
};


Download CouchDB::downloadAttachmentCont(PUrlBuilder urlline, const StringRef &etag) {

	lock.lock();
	try {
		http.open(HttpClient::mGET, *urlline);
		if (!etag.empty()) http.setHeader(http.fldIfNoneMatch,etag);
		SeqFileInput in = http.send();

		auto readStreamFn = [&in](){return in.getNext();};

		natural status = http.getStatus();
		if (status != 200 && status != 304) {

			Value errorVal;
			try{
				errorVal = Value::parse(readStreamFn);
			} catch (...) {

			}
			http.close();
			throw RequestError(THISLOCATION,ConstStrA(*urlline),http.getStatus(), http.getStatusMessage(), errorVal);
		} else {
			HttpClient::HeaderValue ctx = http.getHeader(HttpClient::fldContentType);
			HttpClient::HeaderValue len = http.getHeader(HttpClient::fldContentLength);
			HttpClient::HeaderValue etag = http.getHeader(HttpClient::fldETag);
			natural llen = naturalNull;
			if (len.defined) parseUnsignedNumber(len.getFwIter(), llen, 10);
			if (status == 304) return Download(new EmptyDownload,StringRef(ctx),StringRef(etag),llen,true);
			else return Download(new StreamDownload(in.getStream(),lock,http),StringRef(ctx),StringRef(etag),llen,false);
		}
	} catch (...) {
		lock.unlock();
		throw;
	}


}

Download CouchDB::downloadAttachment(const Document &document, const StringRef &attachmentName,  const StringRef &etag) {

	StringRef documentId = document["_id"].getString();
	StringRef revId = document["_rev"].getString();

	if (revId.empty()) return downloadAttachment(documentId, attachmentName, etag);

	PUrlBuilder url = getUrlBuilder("");
	url->add(documentId);
	url->add(attachmentName);
	url->add("_rev",revId);

	return downloadAttachmentCont(url,etag);


}

Download CouchDB::downloadAttachment(const Value &document, const StringRef &attachmentName,  const StringRef &etag) {

	if (document.type() == json::string) return downloadAttachment((StringRef)document.getString(), attachmentName, etag);

	StringRef documentId = document["_id"].getString();
	StringRef revId = document["_rev"].getString();

	if (revId.empty()) return downloadAttachment(documentId, attachmentName, etag);

	PUrlBuilder url = getUrlBuilder("");
	url->add(documentId);
	url->add(attachmentName);
	url->add("_rev",revId);

	return downloadAttachmentCont(url,etag);

}

Download CouchDB::downloadAttachment(const StringRef &docId, const StringRef &attachmentName,  const StringRef &etag) {

	PUrlBuilder url = getUrlBuilder("");
	url->add(docId);
	url->add(attachmentName);

	return downloadAttachmentCont(url,etag);
}


CouchDB::Queryable::Queryable(CouchDB& owner):owner(owner) {
}

Value CouchDB::Queryable::executeQuery(const QueryRequest& r) {

	PUrlBuilder url = owner.getUrlBuilder(r.view.viewPath);

	ConstStrA startKey = "startkey";
	ConstStrA endKey = "endkey";
	ConstStrA startKeyDocId = "startkey_docid";
	ConstStrA endKeyDocId = "endkey_docid";
	bool useCache;

	bool desc = (r.view.flags & View::reverseOrder) != 0;
	if (r.reversedOrder) desc = !desc;
	if (desc) url->add("descending","true");
	Value postBody;
	if (desc) {
		std::swap(startKey,endKey);
		std::swap(startKeyDocId,endKeyDocId);
	}

	useCache = (r.view.flags & View::noCache) == 0 && !r.nocache;

	switch (r.mode) {
		case qmAllItems: break;
		case qmKeyList: if (r.keys.size() == 1) {
							url->addJson("key",r.keys[0]);
						}else if (r.keys.size() > 1){
							if (useCache) {
								String ser = Value(r.keys).stringify();
								if (ser.length() > maxSerializedKeysSizeForGETRequest) {
									postBody = r.keys;
								} else {
									url->add("keys",ser);
								}
							} else {
								postBody = Object("keys",r.keys);
							}
						}
						break;
		case qmKeyRange: {
							Value start = r.keys[0];
							Value end = r.keys[1];
							url->addJson(startKey,start);
							url->addJson(endKey,end);
							if (r.docIdFromGetKey) {
								StringRef startDocId = start.getKey();
								if (!startDocId.empty()) {
									url->add(startKeyDocId, startDocId);
								}
								StringRef endDocId = end.getKey();
								if (!endDocId.empty()) {
									url->add(endKeyDocId, endDocId);
								}
							}
						}
						break;
		case qmKeyPrefix:
					url->addJson(startKey,Value(r.keys[0]).addToArray(Query::minKey));
					url->addJson(endKey,Value(r.keys[0]).addToArray(Query::maxKey));
				break;
		case qmStringPrefix:
				url->addJson(startKey,Value(r.keys[0]).addSuffix(Query::minString));
				url->addJson(endKey,Value(r.keys[0]).addSuffix(Query::maxString));
				break;
		}


	switch (r.reduceMode) {
	case rmDefault:
		if ((r.view.flags & View::reduce) == 0) url->add("reduce","false");
		else {
			natural level = (r.view.flags & View::groupLevelMask) / View::groupLevel;
			if (r.mode == qmKeyList) {
				url->add("group",level?"true":"false");
			} else {
				url->add("groupLevel",ToString<natural>(level));
			}
		}
			break;
	case rmGroup:
		url->add("group","true");
		break;
	case rmGroupLevel:
		url->add("group_level",ToString<natural>(r.groupLevel));
		break;
	case rmNoReduce:
		url->add("reduce","false");
		break;
	case rmReduce:
		break;
	}

	if (r.offset) url->add("skip",ToString<natural>(r.offset));
	if (r.limit != naturalNull) url->add("limit",ToString<natural>(r.limit));
	if (r.nosort) url->add("sorted","false");
	if (r.view.flags & View::includeDocs) {
		url->add("include_docs","true");
		if (r.view.flags & View::conflicts) url->add("conflicts","true");
		if (r.view.flags & View::attachments) url->add("attachments","true");
		if (r.view.flags & View::attEncodingInfo) url->add("att_encoding_info","true");
	}
	if (r.exclude_end) url->add("inclusive_end","false");
	if (r.view.flags & View::updateSeq) url->add("update_seq","false");
	if (r.view.flags & View::updateAfter) url->add("stale","update_after");
	else if (r.view.flags & View::stale) url->add("stale","ok");


	if (r.view.args.type() == json::object) {
		for(auto &&item: r.view.args) {
			url->add(StringRef(item.getKey()),StringRef(item.toString()));
		}
	} else if (r.view.args.defined()) {
		url->add("args",StringRef(r.view.args.toString()));
	}

	Value result;
	if (!postBody.defined()) {
		result = owner.requestGET(*url,0,0);
	} else {
		result = owner.requestPOST(*url,postBody,0,0);
	}
	if (r.view.postprocess) result = r.view.postprocess(&owner, r.ppargs, result);
	return result;

}


CouchDB::UrlBldPool::UrlBldPool():AbstractResourcePool(3,naturalNull,naturalNull) {
}

AbstractResource* CouchDB::UrlBldPool::createResource() {
	return new UrlBld;
}

const char* CouchDB::UrlBldPool::getResourceName() const {
	return "Url buffer";
}

CouchDB::PUrlBuilder CouchDB::getUrlBuilder(ConstStrA resourcePath) {
	if (database.empty() && resourcePath.head(1) != ConstStrA("/"))
		throw ErrorMessageException(THISLOCATION,"No database selected");

	PUrlBuilder b(urlBldPool);
	b->init(baseUrl,database,resourcePath);
	return b;
}

Value CouchDB::bulkUpload(const Value docs, bool all_or_nothing) {
	PUrlBuilder b = getUrlBuilder("_bulk_docs");

	Object wholeRequest;
	wholeRequest.set("docs", docs);
	if (all_or_nothing)
		wholeRequest.set("all_or_nothing",true);

	return requestPOST(*b,wholeRequest,0,0);
}


} /* namespace assetex */

