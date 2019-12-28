/*
 * couchDB.cpp
 *
 *  Created on: 8. 3. 2016
 *      Author: ondra
 */

#include <fstream>
#include <assert.h>
#include <couchit/validator.h>
#include <mutex>


#include "changeset.h"
#include "couchDB.h"
#include <imtjson/json.h>
#include <imtjson/value.h>
#include <imtjson/binjson.tcc>
#include <experimental/string_view>

#include "exception.h"
#include "query.h"

#include "changes.h"


#include "defaultUIDGen.h"
#include "queryCache.h"

#include "document.h"
#include "showProc.h"
#include "updateProc.h"

using std::experimental::fundamentals_v1::string_view;


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
	,authObj(other.authObj)
	,queryable(*this)
{
}


void CouchDB::setCurrentDB(String database) {
	cfg.databaseName = database.str();
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
	Changes chgs = chsink.setFilterFlags(Filter::reverseOrder).limit(1).setTimeout(0).exec();
	if (chgs.hasItems()) return ChangeEvent(chgs.getNext()).seqId;
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

static std::string readFile(const std::string &name) {
	std::ifstream in(name);
	if (!in) return std::string();
	std::string res;
	std::getline(in, res);
	return res;
}
static std::string generateNodeID() {
	std::string id = readFile("/etc/machine-id");
	if (!id.empty()) return id;
	std::string path;
	const char* home = getenv("HOME");
	path.append(home);
	path.append("/.couchit-machine-id");
	id = readFile(path);
	if (!id.empty()) return id;
	std::vector<char> buffer;
	DefaultUIDGen::getInstance()(buffer, "");
	{
		std::ofstream out(path, std::ios::out| std::ios::trunc);
		out.write(buffer.data(), buffer.size());
		out << std::endl;
		if (!out) throw std::runtime_error("Need node_id - and can't generate anyone");
	}
	id = readFile(path);
	if (!id.empty()) return id;
	throw std::runtime_error("Unable to generate node_id");
}

Value CouchDB::getLocal(const StrViewA &localId, std::size_t flags) {
	if (flags & flgNodeLocal) {
		if (cfg.node_id.empty()) {
			static std::string localId =generateNodeID();
			cfg.node_id = localId;
		}
		std::string id = cfg.node_id;
		id.push_back(':');
		id.append(localId.data, localId.length);
		return getLocal(id, flags & ~flgNodeLocal);
	}
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
	return uidGen(uidBuffer,StrViewA());
}

StrViewA CouchDB::lkGenUID(StrViewA prefix) const {
	return uidGen(uidBuffer,prefix);
}

Value CouchDB::get(const StrViewA &docId, const StrViewA & revId, std::size_t flags) {
	PConnection conn = getConnection();
	conn->add(docId);
	if (!revId.empty()) conn->add("rev",revId);

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

Value CouchDB::get(const StrViewA &docId, std::size_t flags) {
	return get(docId, StrViewA(), flags);
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

	DesignDocumentParse(Fn &&source)
		:Super(std::forward<Fn>(source)) {}


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
Value parseDesignDocument(Fn &&fn) {
	DesignDocumentParse<Fn> parser(std::forward<Fn>(fn));
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

int CouchDB::initChangesFeed(const PConnection& conn, ChangesFeed& feed) {
	ChangesFeed::State &state = feed.state;
	std::unique_lock<std::recursive_mutex> _(state.initLock);
	Value jsonBody;
	StrViewA method = "GET";
	if (state.curConn != nullptr) {
		throw std::runtime_error("Changes feed is already in progress");
	}
	if (state.canceled) return 0;

	if (feed.seqNumber.defined()) {
		conn->add("since", feed.seqNumber.toString());
	}
	if (feed.outlimit != ((std::size_t) (-1))) {
		conn->add("limit", feed.outlimit);
	}
	if (feed.filterInUse) {
		const Filter& flt = feed.filter;
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

		if (flt.flags & Filter::includeDocs || feed.forceIncludeDocs) {
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
		if (((flt.flags & Filter::reverseOrder) != 0) != feed.forceReversed) {
			conn->add("descending", "true");
		}
		for (auto&& itm : flt.args) {
			if (!feed.filterArgs[itm.getKey()].defined()) {
				conn->add(StrViewA(itm.getKey()), StrViewA(itm.toString()));
			}
		}
	} else {
		if (feed.forceIncludeDocs) {
			conn->add("include_docs", "true");
		}
		if (feed.forceReversed) {
			conn->add("descending", "true");
		}
		if (feed.docFilter.defined()) {
			conn->add("filter","_doc_ids");
			Value docIds;
			if (feed.docFilter.type() == json::string) {
				docIds = Value(json::array, {feed.docFilter});
			} else {
				docIds = feed.docFilter;
			}
			jsonBody = Object("doc_ids", docIds);
			method="POST";
		}
	}


	for (auto&& v : feed.filterArgs) {
		String val = v.toString();
		StrViewA key = v.getKey();
		conn->add(key, val);
	}
	conn->http.open(conn->getUrl(), method, true);
	conn->http.connect();
	state.curConn = conn.get();
	_.unlock();

	conn->http.setTimeout(feed.iotimeout);
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


Changes CouchDB::receiveChanges(ChangesFeed& feed) {

	PConnection conn = getConnection("_changes");

	if (feed.timeout > 0) {
		conn->add("feed","longpoll");
		if (feed.timeout == ((std::size_t)-1)) {
			conn->add("heartbeat","true");
		} else {
			conn->add("timeout",feed.timeout);
		}
	}

	int status = initChangesFeed(conn, feed);
	if (feed.state.canceled) {
		feed.state.cancelEpilog();
		return Value(json::undefined);
	}

	try {
		Value v;
		if (status/100 != 2) {
			handleUnexpectedStatus(conn);
		} else {
			InputStream stream = conn->http.getResponse();
			try {
				v = Value::parse(stream);
			} catch (...) {
				feed.state.errorEpilog();
				return Value(json::undefined);
			}
			conn->http.close();
			feed.state.finishEpilog();
		}

		Value results=v["results"];
		feed.seqNumber = v["last_seq"];
		{
			SeqNumber l (feed.seqNumber);
			LockGuard _(lock);
			if (l > lksqid) lksqid = l;
		}

		return results;


	} catch (...) {
		feed.state.errorEpilog();
		throw;
	}

}

void CouchDB::updateSeqNum(const Value& seq) {
	SeqNumber l(seq);
	LockGuard _(lock);
	if (l > lksqid)
		lksqid = l;
}

void CouchDB::receiveChangesContinuous(ChangesFeed& feed, ChangesFeedHandler &fn) {

	PConnection conn = getConnection("_changes",true);

	conn->add("feed","continuous");
	if (feed.timeout == ((std::size_t)-1)) {
		conn->add("heartbeat","true");
	} else {
		conn->add("timeout",feed.timeout);
	}


	int status = initChangesFeed(conn, feed);
	if (feed.state.canceled) {
		feed.state.cancelEpilog();
		return;
	}

	try {

		Value v;
		if (status/100 != 2) {
			handleUnexpectedStatus(conn);
		} else {

			InputStream stream = conn->http.getResponse();

			do {
				v = Value::parse(stream);
				Value lastSeq = v["last_seq"];
				if (lastSeq.defined()) {
					feed.seqNumber = lastSeq;
					updateSeqNum(feed.seqNumber);
					conn->http.close();
					break;
				} else {
					ChangeEvent chdoc(v);
					feed.seqNumber = v["seq"];
					if (!fn(chdoc)) {
						conn->http.abort();
						break;
					}
				}
			} while (true);
			feed.state.finishEpilog();

		}
	} catch (...) {
		feed.state.errorEpilog();
	}
	updateSeqNum(feed.seqNumber);
}


ChangesFeed CouchDB::createChangesFeed() {
	return ChangesFeed(*this);
}

class EmptyDownload: public Download::Source {
public:
	virtual BinaryView impl_read() override {
		return BinaryView(nullptr, 0);
	}
};

class StreamDownload: public Download::Source {
public:
	StreamDownload(const InputStream &stream,  CouchDB::PConnection &&conn):stream(stream),conn(std::move(conn)) {}
	virtual BinaryView impl_read() override {
		return stream.read();
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

	return getAttachment(documentId, attachmentName, etag, revId);
}

Download CouchDB::getAttachment(const StrViewA &docId, const StrViewA &attachmentName,  const StrViewA &etag, const StrViewA &rev) {

	PConnection url = getConnection("");
	url->add(docId);
	url->add(attachmentName);
	if (!rev.empty()) url->add("rev",rev);

	return downloadAttachmentCont(url,etag);
}


CouchDB::Queryable::Queryable(CouchDB& owner):owner(owner) {
}


Value CouchDB::Queryable::executeQuery(const QueryRequest& r) {

	SeqNumber lastSeq = owner.getLastKnownSeqNumber();
	if (lastSeq.getRevId() == 0 || (lastSeq.isOld() && r.needUpdateSeq))
		lastSeq = owner.getLastSeqNumber();


	PConnection conn = owner.getConnection(r.view.viewPath);

	StrViewA startKey = "startkey";
	StrViewA endKey = "endkey";
	StrViewA startKeyDocId = "startkey_docid";
	StrViewA endKeyDocId = "endkey_docid";


	bool desc = (r.view.flags & View::reverseOrder) != 0;
	if (r.reversedOrder) desc = !desc;
	if (desc) conn->add("descending","true");
	Value postBody = r.postData;
	if (desc) {
		std::swap(startKey,endKey);
		std::swap(startKeyDocId,endKeyDocId);
	}

//	bool useCache = (r.view.flags & View::noCache) == 0 && !r.nocache;

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
		if ((r.view.flags & (View::reduce|View::groupLevelMask))  == 0 ) conn->add("reduce","false");
		else {
			std::size_t level = (r.view.flags & View::groupLevelMask) / View::groupLevel;
			if (r.mode == qmKeyList) {
				conn->add("group","true");
			} else {
				conn->add("group_level",level);
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

	if (docs.size() > cfg.maxBulkSizeDocs) {

		auto splt = docs.splitAt(cfg.maxBulkSizeDocs);
		Value res1 = bulkUpload(splt.first);
		Value res2 = bulkUpload(splt.second);
		Array res(res1);
		res.addSet(res2);
		return res;

	} else if (docs.size() < cfg.minBulkSizeDocs) {

		Array results;

		for (Value v : docs) {

			Value id = v["_id"];
			Value r;
			try {
				PConnection b = getConnection("");;
				if (id.defined()) {
					b->add(id.getString());
					r= requestPUT(b,v,nullptr,0);
				} else {
					r = requestPOST(b,v,nullptr,0);
				}
				results.push_back(r);
			} catch (RequestError &e) {
				results.push_back(Object(e.getExtraInfo())("id",id));
			}

		}
		return results;

	} else {

		PConnection b = getConnection("_bulk_docs");

		Object wholeRequest;
		wholeRequest.set("docs", docs);

		Value r = requestPOST(b,wholeRequest,0,0);
		lksqid.markOld();
		return r;
	}
}

void CouchDB::put(Document& doc) {
	Changeset chset = createChangeset();
	chset.update(doc);
	chset.commit();
	doc = chset.getUpdatedDoc(doc.getID());
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
	return b;
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

		if (path.substr(0,baseUrlLen) != StrViewA(cfg.baseUrl)
			|| path.substr(baseUrlLen, databaseLen) != StrViewA(cfg.databaseName)) {
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
    	if (!hdr["Accept"].defined()) {
    		hdr("Accept","application/binjson, application/json");
    	}
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

Value CouchDB::getSerialNr()  {
	if (serialNr.defined()) return serialNr;
	//there is no such database serial number.
	//so we will emulate serial number using local document _local/serialNr;
	//on the very first access, serial number is generated
	Document doc = getLocal("serialNr",flgCreateNew);;
	serialNr = doc["serialNr"];
	if (serialNr.defined()) return serialNr;
	doc.set("serialNr", serialNr = genUID());
	try {
		this->put(doc);
	} catch (UpdateException &e) {
		if (e.getErrors()[0].isConflict()) return getSerialNr();
		throw;
	}
	return serialNr;


}

Value CouchDB::getRevisions(const StrViewA docId, Value revisions, Flags flags) {
	PConnection conn = getConnection();
	conn->add(docId);
	if (revisions.empty()) conn->add("open_revs","all");
	else conn->addJson("open_revs",revisions);

	if (flags & flgAttachments) conn->add("attachments","true");
	if (flags & flgRevisions) conn->add("revs","true");

	Value res = requestGET(conn, nullptr,flags & (flgDisableCache|flgRefreshCache));
	Array output;
	output.reserve(res.size());
	for (Value x: res) {
		output.push_back(x[0]); //<pick fist and only value;
	}
	return output;
}

void CouchDB::pruneConflicts(Document& doc) {
	PConnection conn = getConnection();
	conn->add("_bulk_docs");
	{
		Revision curRev(doc.getRevValue());
		Revision newRev(curRev.getRevId()+1, String({curRev.getTag(),"M"}));

		auto revs = doc.object("_revisions");
		auto ids = revs.array("ids");
		if (ids.empty()) ids.insert(0, curRev.getRevId());
		ids.insert(0, newRev.getTag());
		revs("start", newRev.getRevId());
		doc.setRev(newRev.toString());
	}

	Value id = doc.getIDValue();
	Value docv(doc);
	if (cfg.validator) {
		auto r = cfg.validator->validateDoc(docv);
		if (!r.valid) throw ValidationFailedException(r);
	}

	Array docs;
	for (Value c : doc.conflicts()) {
		Revision curRev(c);
		Revision newRev(curRev.getRevId()+1, String({curRev.getTag(),"R"}));
		docs.push_back(Value(json::object,{
				Value("_id",id),
				Value("_rev",newRev.toString()),
				Value("_revisions",Value(json::object,{
					Value("start",newRev.getRevId()),
					Value("ids",{newRev.getTag(),curRev.getTag()})
				})),
				Value("_deleted",true)
		}));
	}

	docs.push_back(docv);


	Value req(json::object,{
		Value("new_edits",false),
		Value("docs",docs)
	});

//	std::cout << req.toString();
	Value r = requestPOST(conn,req,nullptr,0);
	lksqid.markOld();
	std::vector<UpdateException::ErrorItem> errors;
	for (Value x: r) {
		if (x["error"].defined()) {
			UpdateException::ErrorItem err;
			err.document = x["id"];
			err.errorType = x["error"].toString();
			err.reason = x["reason"].toString();
			err.errorDetails = x;
			errors.push_back(err);
		}
	}
	if (!errors.empty()) {
		throw UpdateException(std::move(errors));
	}
}

Result CouchDB::mget_impl(Array &idlist, Flags flags)  {

	if (idlist.empty()) return Result(json::undefined);
	if ((flags & flgOpenRevs) == 0){

		Query q = createQuery(0);
		Array keys;
		for (Value v: idlist) {
			if (!v["rev"].defined()) keys.push_back(v["id"]);
		}
		q.keys(keys);
		Result res = q.exec();
		auto src = res.begin();
		auto trg = idlist.begin();
		auto src_end = res.end();
		auto trg_end = idlist.end();
		while (trg != trg_end) {
			if (!(*trg)["rev"].defined()){
				if (src != src_end && (*trg)["id"] == (*src)["id"]) {
					*trg = (*trg).replace("rev", (*src)["rev"]);
					++src;
				}
			}
			++trg;
		}
	}

	bool only_deleted = (flags & flgOpenRevs) && (!(flags & flgConflicts) || (flags & flgDeletedConflicts)) ;
	bool only_conflicts = (flags & flgOpenRevs) && (!(flags & flgDeletedConflicts) || (flags & flgConflicts));


	PConnection conn = getConnection();
	conn->add("_bulk_get");
	conn->addJson("revs",(flags & flgRevisions) != 0);
	Value req = Object("docs", idlist);
	Array lst;
	try {
		Value r = requestPOST(conn, req, nullptr, 0);
		Value res = r["results"];
		Array conflicts;
		Array deleted_conflicts;
		for (Value v : res) {
			Value docs = v["docs"];
			Value lastDoc = docs[docs.size()-1]["ok"];
			if (lastDoc.defined()) {
				for (Value d: docs) {
					Value doc = d[0];
					Value itm;
					bool deleted;
					if (doc.getKey() == "ok") {
						deleted = doc["_deleted"].getBool();
						itm = doc;
					} else {
						deleted = doc["deleted"].getBool();
						itm = doc["rev"];
					}
					if (!itm.isCopyOf(lastDoc)) {
						if (deleted) {
							if (only_deleted) deleted_conflicts.push_back(itm);
						} else {
							if (only_conflicts) conflicts.push_back(itm);
						}
					}
				}

				if (!conflicts.empty()) {
					lastDoc = lastDoc.replace("_conflicts", conflicts);
					conflicts.clear();
				}
				if (!deleted_conflicts.empty()) {
					lastDoc = lastDoc.replace("_deleted_conflicts", deleted_conflicts);
					deleted_conflicts.clear();
				}
				lst.push_back(lastDoc);
			}
			else {
				lst.push_back(nullptr);
			}
		}
		return Result(lst,lst.size(),0,nullptr);
	} catch (RequestError &e) {
/*		if (e.getCode() != 400) throw;

		Array response;
		for (Value v: req["docs"]) {

			Value id = v["id"];
			Value rev = v["rev"];
			Value r;
			if (rev.defined()) {
				r=this->get(id.getString(),rev.getString(),flgNullIfMissing|flgRevisions);
			} else {
				r=this->get(id.getString(),flgNullIfMissing|flgRevisions);
			}
		}

		return Result(response,response.size(),0,nullptr);*/
		throw;
	}

}

}

