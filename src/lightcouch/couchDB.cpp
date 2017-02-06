/*
 * couchDB.cpp
 *
 *  Created on: 8. 3. 2016
 *      Author: ondra
 */


#include "changeset.h"
#include "couchDB.h"
#include <imtjson/json.h>
#include "exception.h"
#include "query.h"


#include "changes.h"


#include "conflictResolver.h_"
#include "defaultUIDGen.h"
#include "queryCache.h"

#include "document.h"
#include "minihttp/buffered.h"


namespace LightCouch {



StrViewA CouchDB::fldTimestamp("~timestamp");
StrViewA CouchDB::fldPrevRevision("~prevRev");
std::size_t CouchDB::maxSerializedKeysSizeForGETRequest = 1024;





CouchDB::CouchDB(const Config& cfg)
	:baseUrl(cfg.baseUrl)
	,cache(cfg.cache)
	,uidGen(cfg.uidgen == nullptr?DefaultUIDGen::getInstance():*cfg.uidgen)
	,http("LightCouch mini http")
	,queryable(*this)
{
	if (!cfg.databaseName.empty()) use(cfg.databaseName);
}


static bool isRelativePath(const StrViewA &path) {
	return path.substr(0,7) != "http://" && path.substr(0,8) != "https://";
}

Value CouchDB::requestGET(const StrViewA &path, Value *headers, std::size_t flags) {

	if (isRelativePath(path)) return requestGET(*getUrlBuilder(path),headers,flags);

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

	StrViewA cacheKey = path.substr(prefixLen);

	//there will be stored cached item
	Optional<QueryCache::CachedItem> cachedItem;

	if (usecache) {
		cachedItem = cache->find(cacheKey);
	}

	http.open(path,"GET",true);
	bool redirectRetry = false;

	int status;

    do {
    	redirectRetry = false;
    	Object hdr(headers?*headers:Value());
		hdr("Accept","application/json");
		if (cachedItem != nil && cachedItem->isDefined()) {
			hdr("If-None-Match", cachedItem->etag);
		}

		status = http.setHeaders(hdr).send();
		if (status == 304 && cachedItem != null) {
			http.close();
			return cachedItem->value;
		}
		if (status == 301 || status == 302 || status == 303 || status == 307) {
			json::Value val = http.getHeaders()["Location"];
			if (!val.defined()) {
				http.abort();
				throw RequestError(THISLOCATION,path,http.getStatus(),StrViewA(http.getStatusMessage()), Value("Redirect without Location"));
			}
			http.close();
			http.open(val.getString(), "GET",true);
			redirectRetry = true;
		}
    }
	while (redirectRetry);


	if (status/100 != 2) {
		handleUnexpectedStatus(path);
		throw;
	} else {
		Value v = parseResponse();
		if (usecache) {
			Value fld = http.getHeaders()["ETag"];
			if (fld.defined()) {
				cache->set(cacheKey, QueryCache::CachedItem(fld.getString(), v));
			}
		}
		if (flags & flgStoreHeaders && headers != nullptr) {
			*headers = http.getHeaders();
		}
		http.close();
		return v;
	}
}

void CouchDB::handleUnexpectedStatus(StrViewA path) {

	Value errorVal;
	try{
		errorVal = Value::parse(BufferedRead<InputStream>(http.getResponse()));
	} catch (...) {

	}
	http.close();
	throw RequestError(THISLOCATION,path,http.getStatus(), StrViewA(http.getStatusMessage()), errorVal);
}

Value CouchDB::parseResponse() {
	return Value::parse(BufferedRead<InputStream>(http.getResponse()));
}


Value CouchDB::requestDELETE(const StrViewA &path, Value *headers, std::size_t flags) {

	if (isRelativePath(path)) return requestDELETE(*getUrlBuilder(path),headers,flags);

	Synchronized<FastLock> _(lock);
	http.open(path,"DELETE",true);
	Object hdr(headers?*headers:Value());
	hdr("Accept","application/json");

	int status = http.setHeaders(hdr).send();
	if (status/100 != 2) {
		handleUnexpectedStatus(path);
		throw;
	} else {
		Value v = parseResponse();
		if (flags & flgStoreHeaders && headers != nullptr) {
			*headers = http.getHeaders();
		}
		http.close();
		return v;
	}
}

Value CouchDB::jsonPUTPOST(bool postMethod, const StrViewA &path, Value data, Value *headers, std::size_t flags) {

	if (isRelativePath(path)) return jsonPUTPOST(postMethod,*getUrlBuilder(path),data,headers,flags);


	Synchronized<FastLock> _(lock);
	http.open(path,postMethod?"POST":"PUT",true);
	Object hdr(headers?*headers:Value());
	hdr("Accept","application/json");
	hdr("Content-Type","application/json");
	http.setHeaders(hdr);

	int status;
	if (data.type() == json::undefined) {
		status = http.send(StrViewA());
	} else {
		bool hdrSent = false;
		byte buffer[4096];
		int bpos = 0;
		OutputStream outstr(nullptr);


		data.serialize([&](int i) {
			buffer[bpos++] = (byte)i;
			if (bpos == sizeof(buffer)) {
				if (!hdrSent) {
					outstr = http.beginBody();
					hdrSent = true;
				}
				outstr(buffer,bpos,0);
				bpos = 0;
			}
		});
		if (bpos && hdrSent) {
			outstr(buffer,bpos);
		}
		outstr = nullptr;
		status = http.send(buffer,bpos);
	}




	if (status/100 != 2) {
		handleUnexpectedStatus(path);
		throw;
	} else {
		Value v = parseResponse();
		if (flags & flgStoreHeaders && headers != nullptr) {
			*headers = http.getHeaders();
		}
		http.close();
		return v;
	}
}


Value CouchDB::requestPUT(const StrViewA &path, const Value &postData, Value *headers, std::size_t flags) {
	return jsonPUTPOST(false,path,postData,headers,flags);
}
Value CouchDB::requestPOST(const StrViewA &path, const Value &postData, Value *headers, std::size_t flags) {
	return jsonPUTPOST(true,path,postData,headers,flags);
}




void CouchDB::use(String database) {
	this->database = database;
}

String CouchDB::getCurrentDB() const {
	return database;
}


void CouchDB::createDatabase() {
	PUrlBuilder url = getUrlBuilder("");
	requestPUT(*url,Value());
}

void CouchDB::deleteDatabase() {
	PUrlBuilder url = getUrlBuilder("");
	requestDELETE(*url,nullptr);
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



Query CouchDB::createQuery(std::size_t viewFlags) {
	View v("_all_docs", viewFlags);
	return createQuery(v);
}

Value CouchDB::retrieveLocalDocument(const StrViewA &localId, std::size_t flags) {
	PUrlBuilder url = getUrlBuilder("_local");
	url->add(localId);
	try {
		return requestGET(*url,nullptr, flags & (flgDisableCache|flgRefreshCache));
	} catch (const RequestError &e) {
		if (e.getStatus() == 404 && (flags & flgCreateNew)) {
			return Object("_id",String({"_local/",localId}));
		}
		else throw;
	}
}

StrViewA CouchDB::genUID() {
	return uidGen(uidBuffer,StrViewA());
}

StrViewA CouchDB::genUID(StrViewA prefix) {
	return uidGen(uidBuffer,prefix);
}

Value CouchDB::retrieveDocument(const StrViewA &docId, const StrViewA & revId, std::size_t flags) {
	PUrlBuilder url = getUrlBuilder("");
	url->add(docId).add("rev",revId);
	return requestGET(*url,nullptr, flags & (flgDisableCache|flgRefreshCache));
}

Value CouchDB::retrieveDocument(const StrViewA &docId, std::size_t flags) {
	PUrlBuilder url = getUrlBuilder("");
	url->add(docId);

	if (flags & flgAttachments) url->add("attachments","true");
	if (flags & flgAttEncodingInfo) url->add("att_encoding_info","true");
	if (flags & flgConflicts) url->add("conflicts","true");
	if (flags & flgDeletedConflicts) url->add("deleted_conflicts","true");
	if (flags & flgSeqNumber) url->add("local_seq","true");
	if (flags & flgRevisions) url->add("revs","true");
	if (flags & flgRevisionsInfo) url->add("revs_info","true");

	try {
		return requestGET(*url,nullptr,flags & (flgDisableCache|flgRefreshCache));
	} catch (const RequestError &e) {
		if (e.getStatus() == 404 && (flags & flgCreateNew)) {
			return Object("_id",docId);
		}
		else throw;
	}

}

CouchDB::UpdateResult CouchDB::updateDoc(StrViewA updateHandlerPath, StrViewA documentId,
		Value arguments) {
	PUrlBuilder url = getUrlBuilder(updateHandlerPath);
	url->add(documentId);
	for (auto &&v:arguments) {
		StrViewA key = v.getKey();
		String vstr = v.toString();
		url->add(key,vstr);
	}
	Value h;
	Value v = requestPUT(*url, nullptr, &h, flgStoreHeaders);
	return UpdateResult(v,String(h["X-Couch-Update-NewRev"]));

}

Value CouchDB::showDoc(const StrViewA &showHandlerPath, const StrViewA &documentId,
		const Value &arguments, std::size_t flags) {

	PUrlBuilder url = getUrlBuilder(showHandlerPath);
	url->add(documentId);
	for (auto &&v:arguments) {
		StrViewA key = v.getKey();
		String vstr = v.toString();
		url->add(key,vstr);
	}
	Value v = requestGET(*url,nullptr,flags);
	return v;

}


Upload CouchDB::uploadAttachment(const Value &document, const StrViewA &attachmentName, const StrViewA &contentType) {

	StrViewA documentId = document["_id"].getString();
	StrViewA revId = document["_rev"].getString();
	PUrlBuilder url = getUrlBuilder("");
	url->add(documentId);
	url->add(attachmentName);
	if (!revId.empty()) url->add("rev",revId);

	//the lock is unlocked in the UploadClass
	lock.lock();

	class UploadClass: public Upload::Target {
	public:
		UploadClass(FastLock &lock, HttpClient &http, const PUrlBuilder &urlline)
			:lock(lock)
			,http(http)
			,out(http.beginBody())
			,urlline(urlline)
			,finished(false) {
		}

		~UploadClass() noexcept(false) {
			//if not finished, finish now
			if (!finished) finish();
		}

		virtual void operator()(const void *buffer, std::size_t size) {
			out(reinterpret_cast<const unsigned char *>(buffer),size,0);
		}

		String finish() {
			try {
				if (finished) return String(response);
				finished = true;

				out = OutputStream(nullptr);
				int status = http.send();
				if (status != 201) {


					Value errorVal;
					try{
						errorVal = Value::parse(BufferedRead<InputStream>(http.getResponse()));
					} catch (...) {

					}
					http.close();
					StrViewA url(*urlline);
					throw RequestError(THISLOCATION,url,status, StrViewA(http.getStatusMessage()), errorVal);
				} else {
					Value v = Value::parse(BufferedRead<InputStream>(http.getResponse()));
					http.close();
					response = v["rev"];
					lock.unlock();
					return String(response);
				}
			} catch (...) {
				lock.unlock();
				throw;
			}
		}

	protected:
		FastLock &lock;
		HttpClient &http;
		OutputStream out;
		Value response;
		PUrlBuilder urlline;
		bool finished;
	};


	try {
		//open request
		http.open((*url),"PUT",true);
		//send header
		http.setHeaders(Object("Content-Type",contentType));
		//create upload object
		return Upload(new UploadClass(lock,http,url));
	} catch (...) {
		//anywhere can exception happen, then unlock here and throw exception
		lock.unlock();
		throw;
	}


}

String CouchDB::uploadAttachment(const Value &document, const StrViewA &attachmentName, const AttachmentDataRef &attachmentData) {

	Upload upld = uploadAttachment(document,attachmentName,attachmentData.contentType);
	upld.write(attachmentData.data(), attachmentData.length());
	return upld.finish();
}


Value CouchDB::genUIDValue() {
	Synchronized<FastLock> _(lock);
	return StrViewA(genUID());
}

Value CouchDB::genUIDValue(StrViewA prefix) {
	Synchronized<FastLock> _(lock);
	return StrViewA(genUID(prefix));
}


Document CouchDB::newDocument() {
	Synchronized<FastLock> _(lock);
	return Document(genUID(),StrViewA());
}

Document CouchDB::newDocument(const StrViewA &prefix) {
	Synchronized<FastLock> _(lock);
	return Document(genUID(StrViewA(prefix)),StrViewA());
}

template<typename Fn>
class DesignDocumentParse: public json::Parser<Fn> {
public:
	std::string functionBuff;

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
				functionBuff.append(functionkw);


				Stack<char, SmallAlloc<256> > levelStack;
				while(true) {
					char c = Super::rd.nextCommit();
					functionBuff.push_back(c);
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
						v.serialize([&](char c) {functionBuff.push_back(c);});
					}
				}
				return json::Value(functionBuff);
			} else {
				throw json::ParseError("Unexpected token");
			}
		} else {
			return Super::parse();
		}
	}
};

static const StrViewA _designSlash("_design/");



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
		std::size_t contentLen, DesignDocUpdateRule updateRule) {

	ConstStrA ctt(content, contentLen);
	ConstStrA::Iterator iter = ctt.getFwIter();
	return uploadDesignDocument(parseDesignDocument(iter), updateRule);

}

Changes CouchDB::receiveChanges(ChangesSink& sink) {

	PUrlBuilder url = getUrlBuilder("_changes");

	if (sink.seqNumber.defined()) {
		url->add("since", sink.seqNumber.toString());
	}
	if (sink.outlimit != ((std::size_t)-1)) {
		url->add("limit",ToString<std::size_t>(sink.outlimit));
	}
	if (sink.timeout > 0) {
		url->add("feed","longpoll");
		if (sink.timeout == ((std::size_t)-1)) {
			url->add("heartbeat","true");
		} else {
			url->add("timeout",ToString<std::size_t>(sink.timeout));
		}
	}
	if (sink.filter != null) {
		const Filter &flt = sink.filter;
		if (!flt.viewPath.empty()) {
			ConstStrA fltpath = convStr(flt.viewPath);
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

			String fpath({StrViewA(ddocName),"/",StrViewA(filterName)});

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
				url->add(StrViewA(itm.getKey()),StrViewA(itm.toString()));
			}
		}
	}
	for (auto &&v : sink.filterArgs) {
			String val = v.toString();
			StrViewA key = v.getKey();
			url->add(key,val);
	}


	if (sink.timeout) {
		sink.initCancelFunction();
	}

	Synchronized<FastLock> _(lock);

	if (!sink.cancelFunction) {
		sink.cancelFunction = http.initCancelFunction();
	}

	http.open(*url,"GET",true);
	http.setTimeout(120000);
	if (sink.timeout) {
		http.setCancelFunction(sink.cancelFunction);
	}
	http.setHeaders(Object("Accept","application/json"));

	int status = http.send();


	Value v;
	if (status/100 != 2) {
		if (sink.canceled) {
			sink.canceled = false;
			throw CanceledException(THISLOCATION);
		}
		handleUnexpectedStatus(*url);
	} else {

		InputStream stream = http.getResponse();
		try {

			v = Value::parse(BufferedRead<InputStream>(stream));

		} catch (...) {

			if (sink.timeout) {
				http.setCancelFunction(CancelFunction());
			}
			//any exception
			//terminate connection
			http.abort();
			if (sink.canceled) {
				sink.canceled = false;
				throw CanceledException(THISLOCATION);
			}
			//throw it
			throw;
		}

		http.close();

		if (sink.timeout) {
			http.setCancelFunction(CancelFunction());
		}
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
	virtual std::size_t operator()(void *, std::size_t ) {return 0;}
	virtual const unsigned char *operator()(std::size_t 	, std::size_t *ready) {
		if (ready) {
			*ready = 0;
		}
		return reinterpret_cast<const unsigned  char *>(this);
	}
};

class StreamDownload: public Download::Source {
public:
	StreamDownload(const InputStream &stream, FastLock &lock, HttpClient &http):stream(stream),lock(lock),http(http) {}
	virtual std::size_t operator()(void *buffer, std::size_t size) {
		std::size_t rd;
		const unsigned char *c = stream(0,&rd);
		if (size > rd) size = rd;
		std::memcpy(buffer,c,size);
		stream(size,0);
		return size;
	}
	virtual const unsigned char *operator()(std::size_t processed, std::size_t *ready) {
		return stream(processed,ready);
	}

	~StreamDownload() {
		http.close();
		lock.unlock();
	}

protected:
	InputStream stream;
	FastLock &lock;
	HttpClient &http;
};


Download CouchDB::downloadAttachmentCont(PUrlBuilder urlline, const StrViewA &etag) {

	lock.lock();
	try {
		http.open(*urlline, "GET", true);
		if (!etag.empty()) http.setHeaders(Object("If-None-Match",etag));
		int status = http.send();

		if (status != 200 && status != 304) {

			handleUnexpectedStatus(*urlline);
			throw;
		} else {
			Value hdrs = http.getHeaders();
			Value ctx = hdrs["Content-Type"];
			Value len = hdrs["Content-Length"];
			Value etag = hdrs["ETag"];
			std::size_t llen = ((std::size_t)-1);
			if (len.defined()) llen = len.getUInt();
			if (status == 304) return Download(new EmptyDownload,ctx.getString(),etag.getString(),llen,true);
			else return Download(new StreamDownload(http.getResponse(),lock,http),ctx.getString(),etag.getString(),llen,false);
		}
	} catch (...) {
		lock.unlock();
		throw;
	}


}

Download CouchDB::downloadAttachment(const Document &document, const StrViewA &attachmentName,  const StrViewA &etag) {

	StrViewA documentId = document["_id"].getString();
	StrViewA revId = document["_rev"].getString();

	if (revId.empty()) return downloadAttachment(documentId, attachmentName, etag);

	PUrlBuilder url = getUrlBuilder("");
	url->add(documentId);
	url->add(attachmentName);
	url->add("_rev",revId);

	return downloadAttachmentCont(url,etag);


}

Download CouchDB::downloadAttachment(const Value &document, const StrViewA &attachmentName,  const StrViewA &etag) {

	if (document.type() == json::string) return downloadAttachment((StrViewA)document.getString(), attachmentName, etag);

	StrViewA documentId = document["_id"].getString();
	StrViewA revId = document["_rev"].getString();

	if (revId.empty()) return downloadAttachment(documentId, attachmentName, etag);

	PUrlBuilder url = getUrlBuilder("");
	url->add(documentId);
	url->add(attachmentName);
	url->add("_rev",revId);

	return downloadAttachmentCont(url,etag);

}

Download CouchDB::downloadAttachment(const StrViewA &docId, const StrViewA &attachmentName,  const StrViewA &etag) {

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
								StrViewA startDocId = start.getKey();
								if (!startDocId.empty()) {
									url->add(startKeyDocId, startDocId);
								}
								StrViewA endDocId = end.getKey();
								if (!endDocId.empty()) {
									url->add(endKeyDocId, endDocId);
								}
							}
						}
						break;
		case qmKeyPrefix:
					url->addJson(startKey,addToArray(r.keys[0],Query::minKey));
					url->addJson(endKey,addToArray(r.keys[0],Query::maxKey));
				break;
		case qmStringPrefix:
				url->addJson(startKey,addSuffix(r.keys[0],Query::minString));
				url->addJson(endKey,addSuffix(r.keys[0],Query::maxString));
				break;
		}


	switch (r.reduceMode) {
	case rmDefault:
		if ((r.view.flags & View::reduce) == 0) url->add("reduce","false");
		else {
			std::size_t level = (r.view.flags & View::groupLevelMask) / View::groupLevel;
			if (r.mode == qmKeyList) {
				url->add("group",level?"true":"false");
			} else {
				url->add("groupLevel",ToString<std::size_t>(level));
			}
		}
			break;
	case rmGroup:
		url->add("group","true");
		break;
	case rmGroupLevel:
		url->add("group_level",ToString<std::size_t>(r.groupLevel));
		break;
	case rmNoReduce:
		url->add("reduce","false");
		break;
	case rmReduce:
		break;
	}

	if (r.offset) url->add("skip",ToString<std::size_t>(r.offset));
	if (r.limit != ((std::size_t)-1)) url->add("limit",ToString<std::size_t>(r.limit));
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
			url->add(StrViewA(item.getKey()),StrViewA(item.toString()));
		}
	} else if (r.view.args.defined()) {
		url->add("args",StrViewA(r.view.args.toString()));
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


CouchDB::UrlBldPool::UrlBldPool():AbstractResourcePool(3,((std::size_t)-1),((std::size_t)-1)) {
}

AbstractResource* CouchDB::UrlBldPool::createResource() {
	return new UrlBld;
}

const char* CouchDB::UrlBldPool::getResourceName() const {
	return "Url buffer";
}

CouchDB::PUrlBuilder CouchDB::getUrlBuilder(StrViewA resourcePath) {
	if (database.empty() && resourcePath.substr(0,1) != StrViewA("/"))
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

void CouchDB::updateDocument(Document& doc) {
	Changeset chset = createChangeset();
	chset.update(doc);
	chset.commit(false);
	doc.set("_rev", chset.getCommitRev(doc));
}


} /* namespace assetex */

