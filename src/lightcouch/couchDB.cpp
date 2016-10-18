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

typedef AutoArrayStream<char, SmallAlloc<1024> > UrlLine;


CouchDB::CouchDB(const Config& cfg)
	:baseUrl(cfg.baseUrl)
	,cache(cfg.cache)
	,uidGen(cfg.uidgen == null?DefaultUIDGen::getInstance():*cfg.uidgen)
	,httpConfig(cfg),http(httpConfig)
{
	if (!cfg.databaseName.empty()) use(cfg.databaseName);
}


template<typename C>
void CouchDB::reqPathToFullPath(StringRef reqPath, C &output) {
	output.append(baseUrl);
	if (reqPath.substr(0,1) == "/") {
		output.append(reqPath.substr(0,1));
	} else {
		if (database.empty()) throw ErrorMessageException(THISLOCATION,"No database selected");

//			output.append(StringRef('/'));
		output.append(database);
		output.append(StringRef("/"));
		output.append(reqPath);
	}
}



Value CouchDB::requestGET(const StringRef &path, Value *headers, natural flags) {
	if (headers != nullptr && headers->type() != json::object) {
		throw InvalidParamException(THISLOCATION,4,"Argument headers must be either null or an JSON object");
	}

	AutoArray<char, SmallAlloc<4096> > requestUrl;
	reqPathToFullPath(path,requestUrl);

	bool usecache = (flags & flgDisableCache) == 0;
	if (path.substr(0,1) == StringRef("/")) usecache = false;
	if (!cache) usecache = false;

	//there will be stored cached item
	Optional<QueryCache::CachedItem> cachedItem;

	if (usecache) {
		cachedItem = cache->find(path);
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
			if (!val.defined) throw RequestError(THISLOCATION,requestUrl,http.getStatus(),http.getStatusMessage(), Value("Redirect without Location"));
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
		throw RequestError(THISLOCATION,requestUrl,http.getStatus(), http.getStatusMessage(), errorVal);
	} else {
		Value v = Value::parse(readStreamFn);
		if (usecache) {
			BredyHttpSrv::HeaderValue fld = http.getHeader(HttpClient::fldETag);
			if (fld.defined) {
				cache->set(path, QueryCache::CachedItem(fld, v));
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
	if (headers != nullptr && headers->type() != json::object) {
		throw InvalidParamException(THISLOCATION,4,"Argument headers must be either null or an JSON object");
	}

	AutoArray<char, SmallAlloc<4096> > requestUrl;
	reqPathToFullPath(path,requestUrl);

	Synchronized<FastLock> _(lock);
	http.open(HttpClient::mDELETE, requestUrl);
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
		throw RequestError(THISLOCATION,requestUrl,http.getStatus(), http.getStatusMessage(), errorVal);
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

	if (headers != nullptr && headers->type() != json::object) {
		throw InvalidParamException(THISLOCATION,4,"Argument headers must be either null or an JSON object");
	}

	AutoArray<char, SmallAlloc<4096> > requestUrl;
	reqPathToFullPath(path,requestUrl);

	Synchronized<FastLock> _(lock);
	http.open(method, requestUrl);
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
		throw RequestError(THISLOCATION,requestUrl,http.getStatus(), http.getStatusMessage(), errorVal);
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
	requestPUT(StringRef(),Value());
}

void CouchDB::deleteDatabase() {
	requestDELETE(StringRef(),nullptr);
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
	UrlLine urlLine;
	TextOut<UrlLine &, SmallAlloc<256> > urlfmt(urlLine);
	ConvertReadIter<StringRef::Iterator, UrlEncodeConvert> docIdEnc(localId.getFwIter());
	urlfmt("_local/%1") << &docIdEnc;
	return requestGET(urlLine.getArray(),nullptr, flags & (flgDisableCache|flgRefreshCache));
}

StringRef CouchDB::genUID() {
	return uidGen(uidBuffer,StringRef());
}

StringRef CouchDB::genUID(StringRef prefix) {
	return uidGen(uidBuffer,prefix);
}

Value CouchDB::retrieveDocument(const StringRef &docId, const StringRef & revId, natural flags) {
	UrlLine urlLine;
	TextOut<UrlLine &, SmallAlloc<256> > urlfmt(urlLine);
	FilterRead<StringRef::Iterator, UrlEncoder> docIdEnc(docId.getFwIter()), revIdEnc(revId.getFwIter());
	urlfmt("%1?rev=%2") << &docIdEnc << &revIdEnc;
	return requestGET(urlLine.getArray(),nullptr, flags & (flgDisableCache|flgRefreshCache));
}

Value CouchDB::retrieveDocument(const StringRef &docId, natural flags) {
	UrlLine urlLine;
	TextOut<UrlLine &, SmallAlloc<256> > urlfmt(urlLine);
	FilterRead<StringRef::Iterator, UrlEncoder> docIdEnc(docId.getFwIter());

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

	return requestGET(urlLine.getArray(),nullptr,flags & (flgDisableCache|flgRefreshCache));

}

CouchDB::UpdateResult CouchDB::updateDoc(StringRef updateHandlerPath, StringRef documentId,
		Value arguments) {

	UrlLine urlline;
	TextOut<UrlLine &, SmallAlloc<256> > urlfmt(urlline);
	FilterRead<StringRef::Iterator, UrlEncoder> docIdEnc(documentId.getFwIter());
	urlfmt("%1/%2") << updateHandlerPath << &docIdEnc;
	if (arguments.defined()) {
		char c = '?';
		arguments.forEach([&](const Value &v) {
			StringRef key = v.getKey();
			std::string vstr = v.toString();
			StringRef val(vstr);
			ConvertReadIter<UrlEncodeConvert, StringRef::Iterator> keyEnc(key.getFwIter()),
																   valEnc(val.getFwIter());
			urlfmt("%1%%2=%3") << c << &keyEnc << &valEnc;
			c = '&';
		});
	}
	Value h;
	Value v = requestPUT(urlline.getArray(), nullptr, &h, flgStoreHeaders);
	return UpdateResult(v,h["X-Couch-Update-NewRev"]);

}

Value CouchDB::showDoc(const StringRef &showHandlerPath, const StringRef &documentId,
		const Value &arguments, natural flags) {

	UrlLine urlline;
	TextOut<UrlLine &, SmallAlloc<256> > urlfmt(urlline);
	FilterRead<StringRef::Iterator, UrlEncoder> docIdEnc(documentId.getFwIter());
	urlfmt("%1/%2") << showHandlerPath << &docIdEnc;
	if (arguments.defined()) {
		char c = '?';
		arguments.forEach([&](const Value &v) {
			StringRef key = v.getKey();
			std::string vstr = v.toString();
			StringRef val(vstr);
			ConvertReadIter<UrlEncodeConvert, StringRef::Iterator> keyEnc(key.getFwIter()),
																   valEnc(val.getFwIter());
			urlfmt("%1%%2=%3") << c << &keyEnc << &valEnc;
			c = '&';
		});
	}
	Value v = requestGET(urlline.getArray(),nullptr,flags);

	return v;

}


String CouchDB::uploadAttachment(const Value &document, const StringRef &attachmentName, const StringRef &contentType, const UploadFn &uploadFn) {
	Synchronized<FastLock> _(lock);
	StringRef documentId = document["_id"].getString();
	StringRef revId = document["_rev"].getString();
	UrlLine urlline;
	TextOut<UrlLine &, SmallAlloc<256> > urlfmt(urlline);
	ConvertReadIter<UrlEncodeConvert,StringRef::Iterator> docIdEnc(documentId.getFwIter()),attNameEnc(attachmentName.getFwIter()),revIdEnc(revId.getFwIter());;
	urlfmt("%1/%2/%3/%4?rev=%5") << baseUrl << database << &docIdEnc << &attNameEnc << &revIdEnc;

	http.open(HttpClient::mPUT, urlline.getArray());
	http.setHeader(HttpClient::fldContentType, contentType);
	SeqFileOutput out = http.beginBody(HttpClient::psoDefault);
	unsigned char buffer[4096];
	std::size_t sbuff;
	do {
		sbuff = uploadFn(buffer,sbuff);
		if (sbuff) out.blockWrite(buffer,sbuff,true);
	} while (sbuff != 0);

	SeqFileInput in = http.send();
	auto responseData = [&in](){return in.getNext();};
	if (http.getStatus() != 201) {


		Value errorVal;
		try{
			errorVal = Value::parse(responseData);
		} catch (...) {

		}
		http.close();
		throw RequestError(THISLOCATION,urlline.getArray(),http.getStatus(), http.getStatusMessage(), errorVal);
	} else {
		Value v = Value::parse(responseData);
		http.close();
		return v["rev"];
	}
}


Value CouchDB::genUIDValue() {
	Synchronized<FastLock> _(lock);
	return StringRef(genUID());
}

Value CouchDB::genUIDValue(StringRef prefix) {
	Synchronized<FastLock> _(lock);
	return StringRef(genUID(prefix));
}

String CouchDB::uploadAttachment(const Value &document, const StringRef &attachmentName, const AttachmentDataRef &attachmentData) {

	ConstBin::Iterator iter = attachmentData.getFwIter();
	return uploadAttachment(document,attachmentName,attachmentData.contentType,
			[&](unsigned char *buffer, std::size_t length) -> std::size_t {
		ArrayRef<unsigned char> ref(buffer,length);
		return iter.blockRead(ref.ref(),true);
	});

}

Value CouchDB::newDocument() {
	Synchronized<FastLock> _(lock);
	return Object("_id",genUID());
}

Value CouchDB::newDocument(const StringRef &prefix) {
	Synchronized<FastLock> _(lock);
	return Object("_id",genUID(prefix));
}

template<typename Fn>
class DesignDocumentParse: public json::Parser<Fn> {
public:
	AutoArray<char> functionBuff;

	typedef json::Parser<Fn> Super;

	DesignDocumentParse(const Fn &source)
		:Super(source) {}


	virtual Value parse() {
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
				functionBuff.append(StringRef(functionkw));

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
							throw json::ParseError(StringRef(functionBuff));
						levelStack.pop();
						if (levelStack.empty() && c == '}') break;
					} else if (c == '"') {
						Value v = Super::parseString();
						v.serialize([&](char c) {wr.write(c);});
					}
				}
				return Value(StringRef(functionBuff));
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
			chset.update(newddoc);
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
	UrlLine reqline;
	reqPathToFullPath("_changes",reqline.getArray());
	TextOut<UrlLine &, SmallAlloc<256> > urlfmt(reqline);

	natural qpos = reqline.length();

	if (sink.seqNumber != null) {
		ConvertReadIter<UrlEncodeConvert, StringRef::Iterator> seqNumb(sink.seqNumber.getStringA().getFwIter());
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
			StringRef fltpath = flt.viewPath;
			StringRef ddocName;
			StringRef filterName;
			bool isView = false;
			StringRef::SplitIterator iter = fltpath.split('/');
			StringRef x = iter.getNext();
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
		sink.filterArgs->forEach([&](ConstValue v, StringRef key, natural) {
			if (v->isString()) {
				ConvertReadIter<UrlEncodeConvert,StringRef::Iterator> cnv(v.getStringA().getFwIter());
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

		Value errorVal;
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

Value CouchDB::emptyDocument(StringRef id) {
	return json("_id",id);
}

void CouchDB::downloadAttachment(const StringRef& documentId, const StringRef& revId,
		const StringRef& attachmentName, const DownloadFn& downloadFn,
		StringRef etag) {

	Synchronized<FastLock> _(lock);
	UrlLine urlline;
	TextOut<UrlLine &, SmallAlloc<256> > urlfmt(urlline);
	FilterRead<StringRef::Iterator, UrlEncoder> docIdEnc(documentId.getFwIter()),attNameEnc(attachmentName.getFwIter());
	urlfmt("%1/%2/%3/%4") << baseUrl << database << &docIdEnc << &attNameEnc;
	if (!revId.empty()) {
		FilterRead<StringRef::Iterator, UrlEncoder> revIdEnc(revId.getFwIter());
		urlfmt("?rev=%1") << &revIdEnc;
	}
	http.open(HttpClient::mGET, urlline.getArray());
	if (!etag.empty()) http.setHeader(http.fldIfNoneMatch,etag);
	SeqFileInput in = http.send();
	natural status = http.getStatus();
	if (status != 200 && status != 304) {

		Value errorVal;
		try{
			errorVal = factory->fromStream(in);
		} catch (...) {

		}
		http.close();
		throw RequestError(THISLOCATION,urlline.getArray(),http.getStatus(), http.getStatusMessage(), errorVal);
	} else {
		HttpClient::HeaderValue ctx = http.getHeader(HttpClient::fldContentType);
		HttpClient::HeaderValue len = http.getHeader(HttpClient::fldContentLength);
		HttpClient::HeaderValue etag = http.getHeader(HttpClient::fldETag);
		natural llen = naturalNull;
		if (len.defined) parseUnsignedNumber(len.getFwIter(), llen, 10);
		downloadFn(DownloadFile(in,ctx,etag,llen,status == 304));
	}

}

AttachmentData CouchDB::downloadAttachment(const StringRef& docId,
		const StringRef& revId, const StringRef& attachmentName) {
	StringB data;
	StringA contentType;
	downloadAttachment(docId, revId,attachmentName, [&](const DownloadFile &dw) {
		if (dw.contentLength == naturalNull) throw ErrorMessageException(THISLOCATION,"No content-length");
		StringB::WriteIterator wr= data.createBufferIter(dw.contentLength);
		wr.copy(dw,dw.contentLength);
		contentType = dw.contentType;
	});
	return AttachmentData(data,contentType);

}


} /* namespace assetex */
