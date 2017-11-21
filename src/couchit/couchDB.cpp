/*
 * couchDB.cpp
 *
 *  Created on: 8. 3. 2016
 *      Author: ondra
 */

#include <fstream>
#include <assert.h>
#include <mutex>


#include "changeset.h"
#include "couchDB.h"
#include <imtjson/json.h>
#include <imtjson/value.h>
#include <imtjson/binjson.tcc>

#include "exception.h"
#include "query.h"

#include "changes.h"


#include "defaultUIDGen.h"
#include "queryCache.h"

#include "document.h"
#include "showProc.h"
#include "updateProc.h"


namespace couchit {



StrViewA CouchDB::fldTimestamp("~timestamp");
StrViewA CouchDB::fldPrevRevision("~prevRev");
std::size_t CouchDB::maxSerializedKeysSizeForGETRequest = 1024;





CouchDB::CouchDB(const Config& cfg)
	:cfg(cfg)
	,curConnections(0)
	,uidGen(cfg.uidgen == nullptr?DefaultUIDGen::getInstance():*cfg.uidgen)
	,queryable(*this)

{
	if (!cfg.authInfo.username.empty()) {
		authObj = Object("name",cfg.authInfo.username)
						("password",cfg.authInfo.password);
	}
}


CouchDB::CouchDB(const CouchDB& other)
	:cfg(other.cfg)
	,curConnections(0)
	,uidGen(other.uidGen)
	,queryable(*this)
	,authObj(other.authObj)
{
}


void CouchDB::setCurrentDB(String database) {
	cfg.databaseName = database;
}

String CouchDB::getCurrentDB() const {
	return cfg.databaseName;
}


void CouchDB::createDatabase() {
	PConnection conn = getConnection();
	requestPUT(conn,Value());
}

void CouchDB::createDatabase(unsigned int numShards, unsigned int numReplicas) {
	PConnection conn = getConnection();
	conn->add("n", numReplicas);
	conn->add("q", numShards);
	requestPUT(conn,Value());
}

void CouchDB::deleteDatabase() {
	PConnection conn = getConnection();
	requestDELETE(conn,nullptr);
}

CouchDB::~CouchDB() {
	assert(curConnections == 0);
}

enum ListenExceptionStop {listenExceptionStop};


Query CouchDB::createQuery(const View &view) {
	return Query(view, queryable);
}

Changeset CouchDB::createChangeset() {
	return Changeset(*this);
}




SeqNumber CouchDB::getLastSeqNumber() {
	ChangesFeed chsink = createChangesFeed();
	Changes chgs = chsink.setFilterFlags(Filter::reverseOrder).limit(1).exec();
	if (chgs.hasItems()) return ChangedDoc(chgs.getNext()).seqId;
	else return SeqNumber();
}
SeqNumber CouchDB::getLastKnownSeqNumber() const {
	LockGuard _(lock);
	return lksqid;
}



Query CouchDB::createQuery(std::size_t viewFlags) {
	View v("_all_docs", viewFlags|View::update);
	return createQuery(v);
}

Value CouchDB::getLocal(const StrViewA &localId, std::size_t flags) {
	PConnection conn = getConnection("_local");
	conn->add(localId);
	try {
		return requestGET(conn,nullptr, flags & (flgDisableCache|flgRefreshCache));
	} catch (const RequestError &e) {
		if (e.getCode() == 404 && (flags & flgCreateNew)) {
			return Object("_id",String({"_local/",localId}));
		}
		else throw;
	}
}

StrViewA CouchDB::lkGenUID() const {
	LockGuard _(lock);
	return uidGen(uidBuffer,StrViewA());
}

StrViewA CouchDB::lkGenUID(StrViewA prefix) const {
	return uidGen(uidBuffer,prefix);
}

Value CouchDB::get(const StrViewA &docId, const StrViewA & revId, std::size_t flags) {
	PConnection conn = getConnection();
	conn->add(docId).add("rev",revId);
	return requestGET(conn,nullptr, flags & (flgDisableCache|flgRefreshCache));
}

Value CouchDB::get(const StrViewA &docId, std::size_t flags) {
	PConnection conn = getConnection();
	conn->add(docId);

	if (flags & flgAttachments) conn->add("attachments","true");
	if (flags & flgAttEncodingInfo) conn->add("att_encoding_info","true");
	if (flags & flgConflicts) conn->add("conflicts","true");
	if (flags & flgDeletedConflicts) conn->add("deleted_conflicts","true");
	if (flags & flgSeqNumber) conn->add("local_seq","true");
	if (flags & flgRevisions) conn->add("revs","true");
	if (flags & flgRevisionsInfo) conn->add("revs_info","true");

	try {
		return requestGET(conn,nullptr,flags & (flgDisableCache|flgRefreshCache));
	} catch (const RequestError &e) {
		if (e.getCode() == 404) {
			if (flags & flgCreateNew) {
				return Object("_id",docId);
			} else if (flags & flgNullIfMissing) {
				return nullptr;
			}
		}
		throw;
	}
}

UpdateResult CouchDB::execUpdateProc(StrViewA updateHandlerPath, StrViewA documentId,
		Value arguments) {
	PConnection conn = getConnection(updateHandlerPath);
	conn->add(documentId);
	for (auto &&v:arguments) {
		StrViewA key = v.getKey();
		String vstr = v.toString();
		conn->add(key,vstr);
	}
	Value h(Object("Accept","*/*"));
	Value v = requestPUT(conn, nullptr, &h, flgStoreHeaders);
	lksqid.markOld();
	String rev (h["X-Couch-Update-NewRev"]);
	String ctt(h["content-type"]);
	if (ctt != "application/json") {
		return UpdateResult(v["content"],ctt, rev);
	} else {
		return UpdateResult(v,ctt, rev);
	}

}

ShowResult CouchDB::execShowProc(const StrViewA &showHandlerPath, const StrViewA &documentId,
		const Value &arguments, std::size_t flags) {

	PConnection conn = getConnection(showHandlerPath);
	conn->add(documentId);
	for (auto &&v:arguments) {
		StrViewA key = v.getKey();
		String vstr = v.toString();
		conn->add(key,vstr);
	}
	Value h(Object("Accept","*/*"));
	Value v = requestGET(conn,&h,flags|flgStoreHeaders);
	String ctt(h["content-type"]);
	if (ctt != "application/json") {
		return ShowResult(v["content"],ctt);
	} else {
		return ShowResult(v,ctt);
	}


}


Upload CouchDB::putAttachment(const Value &document, const StrViewA &attachmentName, const StrViewA &contentType) {

	StrViewA documentId = document["_id"].getString();
	StrViewA revId = document["_rev"].getString();
	PConnection conn = getConnection();
	conn->add(documentId);
	conn->add(attachmentName);
	if (!revId.empty()) conn->add("rev",revId);


	class UploadClass: public Upload::Target {
	public:
		UploadClass( PConnection &&urlline)
			:http(urlline->http)
			,out(http.beginBody())
			,urlline(std::move(urlline))
			,finished(false)
	{
		}

		~UploadClass() noexcept(false) {
			//if not finished, finish now
			if (!finished) finish();
		}

		virtual void operator()(json::BinaryView strm) {
			out(strm);
		}

		String finish() {
				if (finished) return String(response);
				finished = true;

				out(nullptr);
				int status = http.send();
				if (status != 201) {


					Value errorVal;
					try{
						errorVal = Value::parse(http.getResponse());
					} catch (...) {

					}
					http.close();
					StrViewA url(*urlline);
					throw RequestError(url,status, http.getStatusMessage(), errorVal);
				} else {
					Value v = Value::parse(http.getResponse());
					http.close();
					response = v["rev"];
					return String(response);
				}
		}

	protected:
		HttpClient &http;
		OutputStream out;
		Value response;
		PConnection urlline;
		bool finished;
	};

		lksqid.markOld();
		//open request
		conn->http.open(conn->getUrl(),"PUT",true);
		//send header
		conn->http.setHeaders(Object("Content-Type",contentType)("Cookie",getToken()));
		//create upload object
		return Upload(new UploadClass(std::move(conn)));


}

String CouchDB::putAttachment(const Value &document, const StrViewA &attachmentName, const AttachmentDataRef &attachmentData) {

	Upload upld = putAttachment(document,attachmentName,attachmentData.contentType);
	upld.write(BinaryView(attachmentData));
	return upld.finish();
}


Value CouchDB::genUIDValue() const {
	LockGuard _(lock);
	return StrViewA(lkGenUID());
}

Value CouchDB::genUIDValue(StrViewA prefix)  const {
	LockGuard _(lock);
	return lkGenUID(prefix);
}


Document CouchDB::newDocument() {
	return Document(lkGenUID(),StrViewA());
}

Document CouchDB::newDocument(const StrViewA &prefix) {
	return Document(lkGenUID(StrViewA(prefix)),StrViewA());
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
		int x = Super::rd.next();
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


				std::vector<char> levelStack;
				while(true) {
					int c = Super::rd.nextCommit();
					functionBuff.push_back(c);
					if (c == '(' || c == '[' || c == '{') {
						levelStack.push_back(c);
					} else if (c == ')' || c == ']' || c == '}') {
						char t = levelStack.empty()?0:levelStack[levelStack.size()-1];
						if (t == 0 ||
								(c == ')' && t != '(') ||
								(c == '}' && t != '{') ||
								(c == ']' && t != '['))
							throw json::ParseError(functionBuff, c);
						levelStack.pop_back();
						if (levelStack.empty() && c == '}') break;
					} else if (c == '"') {
						json::Value v = Super::parseString();
						v.serialize([&](char c) {functionBuff.push_back(c);});
					}
				}
				return json::Value(functionBuff);
			} else {
				throw json::ParseError("Unexpected token", x);
			}
		} else {
			return Super::parse();
		}
	}
};

static const StrViewA _designSlash("_design/");



bool CouchDB::putDesignDocument(const Value &content, DesignDocUpdateRule updateRule) {

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
		Document curddoc = this->get(content["_id"].getString(),0);
		newddoc.setRev(curddoc.getRev());

		///design document already exists, skip uploading
		if (updateRule == ddurSkipExisting) return false;

		//need because couchapp creates empty _attachments node
		newddoc.optimizeAttachments();

/*		Value(curddoc).toStream(std::cerr);std::cerr<<std::endl;
		Value(newddoc).toStream(std::cerr);std::cerr<<std::endl;*/
		///no change in design document, skip uploading
		if (Value(curddoc) == Value(newddoc)) return false;

		if (updateRule == ddurCheck) {
			UpdateException::ErrorItem errItem;
			errItem.document = content;
			errItem.errorDetails = Object();
			errItem.errorType = "conflict";
			errItem.reason = "ddurCheck in effect, cannot update design document";
			throw UpdateException(StringView<UpdateException::ErrorItem>(&errItem,1));
		}

		if (updateRule == ddurOverwrite) {
			chset.update(newddoc);
		} else {
			throw std::runtime_error("Feature is not supported");
		}



	} catch (HttpStatusException &e) {
		if (e.getCode() == 404) {
			chset.update(content);
		} else {
			throw;
		}
	}


	try {
		chset.commit();
	} catch (UpdateException &e) {
		if (e.getErrors()[0].errorType == "conflict") {
			return putDesignDocument(content,updateRule);
		} else {
			throw;
		}
	}
	return true;

}

template<typename Fn>
Value parseDesignDocument(const Fn &fn) {
	DesignDocumentParse<Fn> parser(fn);
	return parser.parse();
}

bool CouchDB::putDesignDocument(const std::string &pathname, DesignDocUpdateRule updateRule) {

	std::ifstream infile(pathname, std::ios::in);
	if (!infile) {
		UpdateException::ErrorItem err;
		err.errorType = "not found";
		err.reason = String({"Failed to open file: ",pathname});
		throw UpdateException(StringView<UpdateException::ErrorItem>(&err,1));
	}
	return putDesignDocument(parseDesignDocument(json::fromStream(infile)), updateRule);
}


bool  CouchDB::putDesignDocument(const char* content,
		std::size_t contentLen, DesignDocUpdateRule updateRule) {

	StrViewA ctt(content, contentLen);
	return putDesignDocument(parseDesignDocument(json::fromString(ctt)), updateRule);

}

int CouchDB::initChangesFeed(const PConnection& conn, ChangesFeed& sink) {
	std::unique_lock<std::mutex> _(sink.initLock);
	Value jsonBody;
	StrViewA method = "GET";
	if (sink.curConn != nullptr) {
		throw std::runtime_error("Changes feed is already in progress");
	}
	if (sink.canceled) return 0;

	if (sink.seqNumber.defined()) {
		conn->add("since", sink.seqNumber.toString());
	}
	if (sink.outlimit != ((std::size_t) (-1))) {
		conn->add("limit", sink.outlimit);
	}
	if (sink.filter != nullptr) {
		const Filter& flt = *sink.filter;
		if (!flt.viewPath.empty()) {
			StrViewA fltpath = flt.viewPath;
			StrViewA ddocName;
			StrViewA filterName;
			bool isView = false;
			auto iter = fltpath.split("/");
			StrViewA x = iter();
			if (x == "_design")
				x = iter();

			ddocName = x;
			x = iter();
			if (x == "_view") {
				isView = true;
				x = iter();
			}
			filterName = x;
			String fpath( { ddocName, "/", filterName });
			if (isView) {
				conn->add("filter", "_view");
				conn->add("view", fpath);
			} else {
				conn->add("filter", fpath);
			}
		}
		if (flt.flags & Filter::allRevs)
			conn->add("style", "all_docs");

		if (flt.flags & Filter::includeDocs || sink.forceIncludeDocs) {
			conn->add("include_docs", "true");
			if (flt.flags & Filter::attachments) {
				conn->add("attachments", "true");
			}
			if (flt.flags & Filter::conflicts) {
				conn->add("conflicts", "true");
			}
			if (flt.flags & Filter::attEncodingInfo) {
				conn->add("att_encoding_info", "true");
			}
		}
		if (((flt.flags & Filter::reverseOrder) != 0) != sink.forceReversed) {
			conn->add("descending", "true");
		}
		for (auto&& itm : flt.args) {
			if (!sink.filterArgs[itm.getKey()].defined()) {
				conn->add(StrViewA(itm.getKey()), StrViewA(itm.toString()));
			}
		}
	} else {
		if (sink.forceIncludeDocs) {
			conn->add("include_docs", "true");
		}
		if (sink.forceReversed) {
			conn->add("descending", "true");
		}
		if (sink.docFilter.defined()) {
			conn->add("filter","_doc_ids");
			Value docIds;
			if (sink.docFilter.type() == json::string) {
				docIds = Value(json::array, {sink.docFilter});
			} else {
				docIds = sink.docFilter;
			}
			jsonBody = Object("doc_ids", docIds);
			method="POST";
		}
	}


	for (auto&& v : sink.filterArgs) {
		String val = v.toString();
		StrViewA key = v.getKey();
		conn->add(key, val);
	}
	conn->http.open(conn->getUrl(), method, true);
	conn->http.connect();
	sink.curConn = conn.get();
	_.unlock();

	conn->http.setTimeout(120000);
	conn->http.setHeaders(Object("Accept", "application/json")("Cookie", getToken())("Content-Type","application/json"));
	OutputStream out(nullptr);
	if (jsonBody.defined()) {
			out = conn->http.beginBody();
			jsonBody.serialize(out);
			out(nullptr);
			out.flush();
		}
	int status = conn->http.send();
	return status;
}


Changes CouchDB::receiveChanges(ChangesFeed& sink) {

	PConnection conn = getConnection("_changes");

	if (sink.timeout > 0) {
		conn->add("feed","longpoll");
		if (sink.timeout == ((std::size_t)-1)) {
			conn->add("heartbeat","true");
		} else {
			conn->add("timeout",sink.timeout);
		}
	}

	int status = initChangesFeed(conn, sink);
	if (status == 0 || sink.canceled) {
		sink.cancelEpilog();
		return Value(json::undefined);
	}

	Value v;
	if (status/100 != 2) {
		handleUnexpectedStatus(conn);
	} else {
		InputStream stream = conn->http.getResponse();
		try {
			v = Value::parse(stream);
		} catch (...) {
			sink.errorEpilog();
			return Value(json::undefined);
		}
		conn->http.close();
		sink.finishEpilog();
	}

	Value results=v["results"];
	sink.seqNumber = v["last_seq"];
	{
		SeqNumber l (sink.seqNumber);
		LockGuard _(lock);
		if (l > lksqid) lksqid = l;
	}

	return results;
}

void CouchDB::updateSeqNum(const Value& seq) {
	SeqNumber l(seq);
	LockGuard _(lock);
	if (l > lksqid)
		lksqid = l;
}

void CouchDB::receiveChangesContinuous(ChangesFeed& sink, ChangesFeedHandler &fn) {

	PConnection conn = getConnection("_changes",true);

	conn->add("feed","continuous");
	if (sink.timeout == ((std::size_t)-1)) {
		conn->add("heartbeat","true");
	} else {
		conn->add("timeout",sink.timeout);
	}


	int status = initChangesFeed(conn, sink);
	if (status == 0) {
		sink.cancelEpilog();
		return;
	}

	Value v;
	if (status/100 != 2) {
		handleUnexpectedStatus(conn);
	} else {

		InputStream stream = conn->http.getResponse();
		try {

			do {
				v = Value::parse(stream);
				Value lastSeq = v["last_seq"];
				if (lastSeq.defined()) {
					sink.seqNumber = lastSeq;
					updateSeqNum(sink.seqNumber);
					conn->http.close();
					break;
				} else {
					ChangedDoc chdoc(v);
					sink.seqNumber = v["seq"];
					if (!fn(chdoc)) {
						conn->http.abort();
						break;
					}
				}
			} while (true);
			sink.finishEpilog();

		} catch (...) {
			sink.errorEpilog();
		}
	}
	updateSeqNum(sink.seqNumber);
}


ChangesFeed CouchDB::createChangesFeed() {
	return ChangesFeed(*this);
}

class EmptyDownload: public Download::Source {
public:
	virtual std::size_t operator()(void *, std::size_t ) {return 0;}
	virtual BinaryView operator()(std::size_t) {
		return BinaryView(nullptr, 0);
	}
};

class StreamDownload: public Download::Source {
public:
	StreamDownload(const InputStream &stream,  CouchDB::PConnection &&conn):stream(stream),conn(std::move(conn)) {}
	virtual std::size_t operator()(void *buffer, std::size_t size) override {
		BinaryView b = stream(0);
		if (size > b.length) size = b.length;
		std::memcpy(buffer,b.data,b.length);
		stream(size);
		return size;
	}
	virtual BinaryView operator()(std::size_t processed) override {
		return stream(processed);
	}

	~StreamDownload() {
		conn->http.close();
	}

protected:
	InputStream stream;
	CouchDB::PConnection conn;
};


Download CouchDB::downloadAttachmentCont(PConnection &conn, const StrViewA &etag) {

		conn->http.open(conn->getUrl(), "GET", true);
		Object hdr;
		hdr("Cookie", getToken());
		if (!etag.empty()) hdr("If-None-Match",etag);
		conn->http.setHeaders(hdr);
		int status = conn->http.send();

		if (status != 200 && status != 304) {

			handleUnexpectedStatus(conn);
			throw;
		} else {
			Value hdrs = conn->http.getHeaders();
			Value ctx = hdrs["Content-Type"];
			Value len = hdrs["Content-Length"];
			Value etag = hdrs["ETag"];
			std::size_t llen = ((std::size_t)-1);
			if (len.defined()) llen = len.getUInt();
			if (status == 304) return Download(new EmptyDownload,ctx.getString(),etag.getString(),llen,true);
			else return Download(new StreamDownload(conn->http.getResponse(),std::move(conn)),ctx.getString(),etag.getString(),llen,false);
		}


}

Download CouchDB::getAttachment(const Document &document, const StrViewA &attachmentName,  const StrViewA &etag) {

	StrViewA documentId = document["_id"].getString();
	StrViewA revId = document["_rev"].getString();

	if (revId.empty()) return getAttachment(documentId, attachmentName, etag);

	PConnection url = getConnection("");
	url->add(documentId);
	url->add(attachmentName);
	url->add("_rev",revId);

	return downloadAttachmentCont(url,etag);


}

Download CouchDB::getAttachment(const StrViewA &docId, const StrViewA &attachmentName,  const StrViewA &etag) {

	PConnection url = getConnection("");
	url->add(docId);
	url->add(attachmentName);

	return downloadAttachmentCont(url,etag);
}


CouchDB::Queryable::Queryable(CouchDB& owner):owner(owner) {
}


Value CouchDB::Queryable::executeQuery(const QueryRequest& r) {

	SeqNumber lastSeq = owner.getLastKnownSeqNumber();
	if (lastSeq.getRevId() == 0 || lastSeq.isOld() && r.needUpdateSeq)
		lastSeq = owner.getLastSeqNumber();


	PConnection conn = owner.getConnection(r.view.viewPath);

	StrViewA startKey = "startkey";
	StrViewA endKey = "endkey";
	StrViewA startKeyDocId = "startkey_docid";
	StrViewA endKeyDocId = "endkey_docid";
	bool useCache;


	bool desc = (r.view.flags & View::reverseOrder) != 0;
	if (r.reversedOrder) desc = !desc;
	if (desc) conn->add("descending","true");
	Value postBody = r.postData;
	if (desc) {
		std::swap(startKey,endKey);
		std::swap(startKeyDocId,endKeyDocId);
	}

	useCache = (r.view.flags & View::noCache) == 0 && !r.nocache;

	switch (r.mode) {
		case qmAllItems: break;
		case qmKeyList: if (r.keys.size() == 1) {
							conn->addJson("key",r.keys[0]);
						}else if (r.keys.size() > 1){
							if (postBody.defined()) {
								conn->add("keys",Value(r.keys).stringify());
							} else {
								String ser = Value(r.keys).stringify();
								if (ser.length() > maxSerializedKeysSizeForGETRequest ) {
									postBody = Object("keys",r.keys);
								} else {
									conn->add("keys",ser);
								}
							}
						} else {
							return Object("total_rows",0)
									("offset",0)
									("rows",json::array)
									("update_seq",owner.getLastKnownSeqNumber().toValue());
						}
						break;
		case qmKeyRange: {
							Value start = r.keys[0];
							Value end = r.keys[1];
							conn->addJson(startKey,start);
							conn->addJson(endKey,end);
							if (r.docIdFromGetKey) {
								StrViewA startDocId = start.getKey();
								if (!startDocId.empty()) {
									conn->add(startKeyDocId, startDocId);
								}
								StrViewA endDocId = end.getKey();
								if (!endDocId.empty()) {
									conn->add(endKeyDocId, endDocId);
								}
							}
						}
						break;
		case qmKeyPrefix:
					conn->addJson(startKey,addToArray(r.keys[0],Query::minKey));
					conn->addJson(endKey,addToArray(r.keys[0],Query::maxKey));
				break;
		case qmStringPrefix:
				conn->addJson(startKey,addSuffix(r.keys[0],Query::minString));
				conn->addJson(endKey,addSuffix(r.keys[0],Query::maxString));
				break;
		}


	switch (r.reduceMode) {
	case rmDefault:
		if ((r.view.flags & View::reduce) == 0) conn->add("reduce","false");
		else {
			std::size_t level = (r.view.flags & View::groupLevelMask) / View::groupLevel;
			if (r.mode == qmKeyList) {
				conn->add("group",level?"true":"false");
			} else {
				conn->add("groupLevel",level);
			}
		}
			break;
	case rmGroup:
		conn->add("group","true");
		break;
	case rmGroupLevel:
		conn->add("group_level",r.groupLevel);
		break;
	case rmNoReduce:
		conn->add("reduce","false");
		break;
	case rmReduce:
		break;
	}

	if (r.offset) conn->add("skip",r.offset);
	if (r.limit != ((std::size_t)-1)) conn->add("limit",r.limit);
	if (r.nosort) conn->add("sorted","false");
	if (r.view.flags & View::includeDocs) {
		conn->add("include_docs","true");
		if (r.view.flags & View::conflicts) conn->add("conflicts","true");
		if (r.view.flags & View::attachments) conn->add("attachments","true");
		if (r.view.flags & View::attEncodingInfo) conn->add("att_encoding_info","true");
	}
	if (r.exclude_end) conn->add("inclusive_end","false");
	conn->add("update_seq","true");
	if (r.view.flags & View::stale) conn->add("stale","ok");
	else if ((r.view.flags & View::update) == 0) conn->add("stale","update_after");
	else conn->http.setTimeout(std::max(owner.cfg.syncQueryTimeout, owner.cfg.iotimeout));



	if (r.view.args.type() == json::object) {
		for(auto &&item: r.view.args) {
			conn->add(StrViewA(item.getKey()),StrViewA(item.toString()));
		}
	} else if (r.view.args.defined()) {
		conn->add("args",StrViewA(r.view.args.toString()));
	}

	Value result;
	if (!postBody.defined()) {
		result = owner.requestGET(conn,0,0);
	} else {
		result = owner.requestPOST(conn,postBody,0,0);
	}
	if (r.view.postprocess) result = r.view.postprocess(&owner, r.ppargs, result);
	Value sq = result["update_seq"];
	if (sq.defined()) {
		SeqNumber l(sq);

		if (l.getRevId() == 0) {
			//HACK: https://github.com/apache/couchdb/issues/984
			l = lastSeq;
			result = result.replace("update_seq", l.toValue());
		}

		LockGuard _(owner.lock);
		if (l > owner.lksqid) owner.lksqid = l;
	}
	return result;

}




Value CouchDB::bulkUpload(const Value docs) {
	PConnection b = getConnection("_bulk_docs");

	Object wholeRequest;
	wholeRequest.set("docs", docs);

	Value r = requestPOST(b,wholeRequest,0,0);
	lksqid.markOld();
	return r;
}

void CouchDB::put(Document& doc) {
	Changeset chset = createChangeset();
	chset.update(doc);
	chset.commit();
	doc.set("_rev", chset.getCommitRev(doc));
}



CouchDB::PConnection CouchDB::getConnection(StrViewA resourcePath, bool fresh) {
	std::unique_lock<std::mutex> _(lock);

	auto now = SysClock::now();
	auto lpcd =  now - lastPoolCheck;
	auto keepAliveTm = std::chrono::milliseconds(cfg.keepAliveTimeout);
	auto keepAliveMaxAge = std::chrono::milliseconds(cfg.keepAliveMaxAge);
	if (lpcd > keepAliveTm) {
		ConnPool newConPool;
		for (auto && x: connPool) {
			auto dur =  now - x->lastUse;
			if (dur < keepAliveTm) newConPool.push_back(std::move(x));
		}
		connPool = std::move(newConPool);
		lastPoolCheck = now;
	}

	if (cfg.databaseName.empty() && resourcePath.substr(0,1) != StrViewA("/"))
		throw std::runtime_error("No database selected");

	if (curConnections >= cfg.maxConnections) {
		connRelease.wait(_,[&]{return curConnections<cfg.maxConnections;});
	}

	PConnection b(nullptr);

	if (connPool.empty()) {
		b = PConnection(new Connection, ConnectionDeleter(this));
	} else if (fresh) {
		connPool.erase(connPool.begin());
		b = PConnection(new Connection, ConnectionDeleter(this));
	} else {
		b = PConnection(connPool[connPool.size()-1].release(), ConnectionDeleter(this));
		connPool.pop_back();
		auto dur =  now - b->lastUse;
		auto durf = now - b->firstUse;
		if (dur > keepAliveTm) {
			connPool.clear();
			delete b.release();
			b = PConnection(new Connection, ConnectionDeleter(this));
		} else if (durf > keepAliveMaxAge) {
			delete b.release();
			b = PConnection(new Connection, ConnectionDeleter(this));
		}

	}
	b->http.setTimeout(cfg.iotimeout);
	setUrl(b,resourcePath);
	curConnections++;
	return std::move(b);
}

void CouchDB::setUrl(PConnection &conn, StrViewA resourcePath) {
	conn->init(cfg.baseUrl,cfg.databaseName,resourcePath);
}
void CouchDB::releaseConnection(Connection* b) {
	LockGuard _(lock);
	b->lastUse = SysClock::now();
	connPool.push_back(PConnection(b));
	curConnections--;
	if (curConnections+1==cfg.maxConnections) {
		connRelease.notify_one();
	}
}

void CouchDB::handleUnexpectedStatus(PConnection& conn) {

	Value errorVal;
	try{
		errorVal = Value::parse(conn->http.getResponse());
	} catch (...) {

	}
	conn->http.close();
	throw RequestError(conn->getUrl(),conn->http.getStatus(), conn->http.getStatusMessage(), errorVal);
}

Value CouchDB::parseResponse(PConnection& conn) {
	return Value::parse(conn->http.getResponse());
}
Value CouchDB::parseResponseBin(PConnection& conn) {
	return Value::parseBinary(conn->http.getResponse());
}


Value CouchDB::postRequest(PConnection& conn, const StrViewA &cacheKey, Value *headers, std::size_t flags) {
	HttpClient &http = conn->http;
	int status = http.getStatus();
	if (status/100 != 2) {
		handleUnexpectedStatus(conn);
		throw;
	} else {
		Value ctt = http.getHeaders()["Content-Type"];
		Value v;
		if (ctt.getString() == "application/binjson") {
			v = parseResponseBin(conn);
			if (!cacheKey.empty()) {
				Value fld = http.getHeaders()["ETag"];
				if (fld.defined()) {
					cfg.cache->set(QueryCache::CachedItem(cacheKey, fld.getString(), v));
				}
			}
		} else if (ctt.getString() == "application/json") {
			v = parseResponse(conn);
			if (!cacheKey.empty()) {
				Value fld = http.getHeaders()["ETag"];
				if (fld.defined()) {
					cfg.cache->set(QueryCache::CachedItem(cacheKey, fld.getString(), v));
				}
			}
		} else {
			std::vector<std::uint8_t> data;
			InputStream rd = http.getResponse();
			int i = rd();
			while (i != json::eof) {
				data.push_back((std::uint8_t)i);
				i = rd();
			}
			Object r;
			r.set("content", Value(BinaryView(&data[0],data.size()),json::defaultBinaryEncoding));
			r.set("content-type",ctt);
			v = r;
		}
		if (flags & flgStoreHeaders && headers != nullptr) {
			*headers = http.getHeaders();
		}
		http.close();
		return v;
	}
}

Value CouchDB::requestGET(PConnection& conn, Value* headers, std::size_t flags) {

	bool usecache = (flags & flgDisableCache) == 0 && cfg.cache != nullptr;

	StrViewA path = conn->getUrl();
	StrViewA cacheKey;

	if (usecache) {
		std::size_t baseUrlLen = cfg.baseUrl.length();
		std::size_t databaseLen = cfg.databaseName.length();
		std::size_t prefixLen = baseUrlLen+databaseLen+1;

		if (path.substr(0,baseUrlLen) != cfg.baseUrl
			|| path.substr(baseUrlLen, databaseLen) != cfg.databaseName) {
			usecache = false;
		} else {
			cacheKey = path.substr(prefixLen);
		}
	}


	//there will be stored cached item
	QueryCache::CachedItem cachedItem;

	if (usecache) {
		cachedItem = cfg.cache->find(cacheKey);
	}

	HttpClient &http = conn->http;
	http.open(path,"GET",true);
	bool redirectRetry = false;

	int status;

    do {
    	redirectRetry = false;
    	Object hdr(headers?*headers:Value());
    	if (!hdr["Accept"].defined())
    		hdr("Accept","application/binjson, application/json");
		if (cachedItem.isDefined()) {
			hdr("If-None-Match", cachedItem.etag);
		}
		if ((flags & flgNoAuth) == 0) hdr("Cookie", getToken());

		status = http.setHeaders(hdr).send();
		if (status == 304 && cachedItem.isDefined()) {
			http.close();
			return cachedItem.value;
		}
		if (status == 301 || status == 302 || status == 303 || status == 307) {
			json::Value val = http.getHeaders()["Location"];
			if (!val.defined()) {
				http.abort();
				throw RequestError(path,http.getStatus(),http.getStatusMessage(), Value("Redirect without Location"));
			}
			http.close();
			http.open(val.getString(), "GET",true);
			redirectRetry = true;
		}
    }
	while (redirectRetry);
    return postRequest(conn,cacheKey,headers,flags);


}

Value CouchDB::requestPOST(PConnection& conn,
		const Value& postData, Value* headers, std::size_t flags) {
	return jsonPUTPOST(conn,true,postData,headers,flags);
}

Value CouchDB::requestPUT(PConnection& conn, const Value& postData,
		Value* headers, std::size_t flags) {
	return jsonPUTPOST(conn,false,postData,headers,flags);
}

Value CouchDB::requestDELETE(PConnection& conn, Value* headers,
		std::size_t flags) {

	HttpClient &http = conn->http;
	StrViewA path = conn->getUrl();

	http.open(path,"DELETE",true);
	Object hdr(headers?*headers:Value());
	if (!hdr["Accept"].defined())
		hdr("Accept","application/binjson, application/json");

	if ((flags & flgNoAuth) == 0) hdr("Cookie", getToken());

	http.setHeaders(hdr).send();
    return postRequest(conn,StrViewA(),headers,flags);
}

Value CouchDB::jsonPUTPOST(PConnection& conn, bool methodPost,
		Value data, Value* headers, std::size_t flags) {
	typedef std::uint8_t byte;

	HttpClient &http = conn->http;
	StrViewA path = conn->getUrl();


	http.open(path,methodPost?"POST":"PUT",true);
	Object hdr(headers?*headers:Value());
	if (!hdr["Accept"].defined())
		hdr("Accept","application/binjson, application/json");
	hdr("Content-Type","application/json");
	if ((flags & flgNoAuth) == 0) hdr("Cookie", getToken());
	http.setHeaders(hdr);

	if (data.type() == json::undefined) {
		http.send(StrViewA());
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
				outstr(BinaryView(buffer,bpos));
				bpos = 0;
			}
		});
		if (bpos && hdrSent) {
			outstr(BinaryView(buffer,bpos));
			outstr(nullptr);
		}
		outstr = nullptr;
		http.send(buffer,bpos);
	}
    return postRequest(conn,StrViewA(),headers,flags);
}

Value CouchDB::getToken() {
	if (!authObj.defined()) return json::undefined;
	time_t now;
	time(&now);
	if (now > tokenRefreshTime) {
		std::unique_lock<std::mutex> sl(tokenLock, std::defer_lock);
		if (now > tokenExpireTime) {
			sl.lock();
			if (now <= tokenExpireTime) return token;
			prevToken = token;
		} else {
			if (!sl.try_lock()) return token;
		}
		Value headers;
		PConnection conn = getConnection("/_session");
		Value result = requestPOST(conn,authObj,&headers,flgStoreHeaders|flgNoAuth);

		if (result["ok"].getBool()) {
			StrViewA c = headers["Set-Cookie"].getString();
			auto splt = c.split(";");
			token = splt();
			tokenExpireTime = now+cfg.tokenTimeout-10;
			tokenRefreshTime = tokenExpireTime-cfg.tokenTimeout/2;
			return token;
		} else {
			throw RequestError(conn->getUrl(),500, "Failed to retrieve token",Value());
		}
	}
	return token;

}

std::size_t CouchDB::updateView(const View& view, bool wait) {

	PConnection conn = getConnection(view.viewPath);
	if (!wait)
		conn->add("stale","update_after");
	conn->add("limit",0);
	Value r = requestGET(conn, 0, flgDisableCache);
	return r["total_rows"].getUInt();
}
void CouchDB::setupHttpConn(HttpClient &http, Flags flags) {
	if (flags & flgLongOperation) {
		http.setTimeout(cfg.syncQueryTimeout);
	}
}

CouchDB::Connection::Connection():firstUse(SysClock::now()) {

}


}

