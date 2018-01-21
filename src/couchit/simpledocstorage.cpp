#include "simpledocstorage.h"

#include <imtjson/binjson.tcc>

#include "changes.h"

#include "changeset.h"

#include "checkpointFile.h"


#include "defaultUIDGen.h"
#include "exception.h"
#include "query.h"

namespace couchit {


SimpleDocStorage::SimpleDocStorage(const Config &cfg)
	:cfg(cfg)
{
if (this->cfg.idgen == nullptr)
	this->cfg.idgen = &DefaultUIDGen::getInstance();
}

void SimpleDocStorage::open(String f, PCheckpoint chkpointFile) {

	Sync _(lock);
	dbfile.open(f.c_str(), std::ios::out|std::ios::in);
	if (!dbfile) {
		int e = errno;
		throw SystemException({"Failed to create database file: ", f},e);
	}

	lastSeqNum = 0;
	dbpname = f;
	docMap.clear();
	seqNumMap.clear();
	chkpoint = chkpointFile;
	loadCheckpointAndSync();

}

void SimpleDocStorage::create(String f, PCheckpoint chkpointFile) {

	Sync _(lock);
	dbfile.open(f.c_str(), std::ios::out|std::ios::in|std::ios::trunc);
	if (!dbfile) {
		int e = errno;
		throw SystemException({"Failed to create database file: ", f},e);
	}
	lastSeqNum = 0;
	docMap.clear();
	seqNumMap.clear();
	dbpname = f;
	chkpoint = chkpointFile;
}

template<typename Fn>
class BinarySerializer: public json::BinarySerializer<Fn> {
public:
	using json::BinarySerializer<Fn>::BinarySerializer;
	using json::BinarySerializer<Fn>::serializeInteger;
	using json::BinarySerializer<Fn>::serializeString;

};

template<typename Fn>
class BinaryParser: public json::BinaryParser<Fn> {
public:
	using json::BinaryParser<Fn>::BinaryParser;
	using json::BinaryParser<Fn>::parseInteger;
	using json::BinaryParser<Fn>::parseString;

};


SimpleDocStorage::SeqNum SimpleDocStorage::generateSeqNumMap(const DocMap &docMap, SeqNumMap &seqNumMap){
	SeqNum maxSeqNum = 0;
	seqNumMap.clear();
	for (auto &&itm : docMap) {
		maxSeqNum = std::max(maxSeqNum, itm.second.seqNum);
		seqNumMap.insert(SeqNumMap::value_type(itm.second.seqNum, itm.second.offset));
	}
	return maxSeqNum;
}

SimpleDocStorage::SeqNum SimpleDocStorage::putDocToIndex(DocMap &docMap, const Value &document, Offset offset) {
	String id(document["_id"]);
	SeqNum seqNum = document["_local_seq"].getUInt();
	docMap[id] = DocInfo(seqNum, offset, document["_deleted"].getBool());
	return seqNum;
}

Changes SimpleDocStorage::receiveChanges(ChangesFeed& sink) {
}

void SimpleDocStorage::receiveChangesContinuous(ChangesFeed& sink,
		ChangesFeedHandler& fn) {
}



Value SimpleDocStorage::bulkUpload(const Value docs) {

	Sync _(lock);
	Array results;
	results.reserve(docs.size());
	for (Value doc: docs) {
		results.push_back(uploadDocument(doc));
	}
	return results;

}

SeqNumber SimpleDocStorage::getLastSeqNumber() {
}

SeqNumber SimpleDocStorage::getLastKnownSeqNumber() const {
}

Query SimpleDocStorage::createQuery(const View& view) {
}

Query SimpleDocStorage::createQuery(Flags viewFlags) {
}

Changeset SimpleDocStorage::createChangeset() {
}

ChangesFeed SimpleDocStorage::createChangesFeed() {
}

Value SimpleDocStorage::getLocal(const StrViewA& localId, Flags flags) {
}

Value SimpleDocStorage::get(const StrViewA& docId, Flags flags) {
}

Value SimpleDocStorage::get(const StrViewA& docId, const StrViewA& revId,
		Flags flags) {
}

Upload SimpleDocStorage::putAttachment(const Value& document,
		const StrViewA& attachmentName, const StrViewA& contentType) {
}

String SimpleDocStorage::putAttachment(const Value& document,
		const StrViewA& attachmentName,
		const AttachmentDataRef& attachmentData) {
}

Download SimpleDocStorage::getAttachment(const Document& document,
		const StrViewA& attachmentName, const StrViewA& etag) {
}

Download SimpleDocStorage::getAttachment(const StrViewA& docId,
		const StrViewA& attachmentName, const StrViewA& etag) {
}

void SimpleDocStorage::put(Document& doc) {
}

Validator* SimpleDocStorage::getValidator() const {
}

bool SimpleDocStorage::canIndexDocument(const Value &document) {
	if (document.type() == json::object) {
		if ((cfg.flags & Config::deleteFromIndex) == 0
			|| (document["_deleted"].getBool())) {
			return true;
		}
	}
	return false;
}
void SimpleDocStorage::makeCheckpoint() {
	typedef DocMap::value_type KeyValue;
	typedef std::vector<KeyValue> Items;

	typedef std::shared_ptr<Items> SharedItems;
	SharedItems ptm (new Items);
	{
		SharedSync _(lock);
		ptm->reserve(docMap.size());
		for (auto &&itm:docMap) {
			ptm->push_back(itm);
		}
	}




	chkpoint->store([ptm](std::ostream &stream){

		auto wrfn = [&](char c) {
			stream.put(c);
		};

		BinarySerializer<decltype(wrfn)> srz(wrfn, compressKeys);
		srz.serializeInteger(2, json::opcode::array);
		srz.serializeInteger(ptm->size(), json::opcode::array);
		for (auto &&itm:*ptm) {
			srz.serializeInteger(3, json::opcode::array);
			srz.serializeString(itm.first, json::opcode::string);
			srz.serializeInteger(itm.second.offset, json::opcode::posint);
			srz.serializeInteger(itm.second.seqNum, json::opcode::posint);
		}

	});

}

enum ReadEOFException {readEofException};

void SimpleDocStorage::loadCheckpointAndSync() {

	Sync _(lock);
	docMap.clear();
	seqNumMap.clear();

	Offset maxOffset = 0;

	chkpoint->load([&](std::istream &stream) {

		auto rdfn = [&]{
			int x = stream.get();
			if (x == -1)
				throw CheckpointIOException("Checkpoint invalid format (EOF)",0);
			return (char)x;
		};


		BinaryParser<decltype(rdfn)> parser(rdfn, base64);
		size_t i = parser.parseInteger(rdfn());
		if (i != 2) throw CheckpointIOException("Checkpoint invalid format (hdr)",0);
		size_t endsqn = parser.parseInteger(rdfn());
		size_t cnt = parser.parseInteger(rdfn());
		for (size_t i = 0; i < cnt; i++) {
			size_t l = parser.parseInteger(rdfn());
			if (l != 3) throw CheckpointIOException("Checkpoint invalid format (row)",0);
			String docid(parser.parseString(rdfn(),nullptr));
			Offset offset = parser.parseInteger(rdfn());
			SeqNum seqNum = parser.parseInteger(rdfn());
			if (offset > maxOffset) maxOffset = offset;
			docMap.insert(DocMap::value_type(docid, DocInfo(seqNum,offset)));
		}
	});

	while (true) {
		std::size_t docOffset = maxOffset;
		Value doc = loadDocument(maxOffset);
		if (doc.defined()) {
			if (canIndexDocument(doc)) {
				putDocToIndex(docMap,doc,docOffset);
			}
		} else {
			break;
		}
	}
	syncFileSize = maxOffset;
	lastSeqNum = generateSeqNumMap(docMap,seqNumMap);

}

Value SimpleDocStorage::loadDocument(Offset &offset) {
	dbfile.seekg(offset);
	try {
		Value res = Value::parseBinary([&]{
			int x = dbfile.get();
			if (x == -1)
				throw readEofException;
			return (char)x;
		});
		offset = dbfile.tellg();
	} catch (ReadEOFException) {
		return Value();
	}
}

void SimpleDocStorage::compact() {
	DocMap oldDocMap, newDocMap;

	std::string cmpName = dbpname.c_str();
	cmpName.append(".compact");
	std::ofstream outf(cmpName, std::ios::out|std::ios::trunc|std::ios::binary);

	auto writter = [&](char c){outf.put(c);};
	Offset lastMaxOffset;
	SeqNum maxSeqNum;


	SeqNumMap newSeqNumMap;
	{
		SharedSync _(lock);
		oldDocMap = docMap;
		lastMaxOffset = syncFileSize;
		maxSeqNum = lastSeqNum;
	}

	for (auto &&itm : oldDocMap) {

		Value document;
		{
			SharedSync _(lock);
			Offset ofs = itm.second.offset;
			document = loadDocument(ofs);
		}
		bool isDeleted = document["_deleted"].getBool();

		if (cfg.flags & Config::deletePurgeOnCompact && isDeleted) {
			//skip, because purge is requested
		} else {
			Object docEdit(document);
			auto attachments = docEdit.object("_attachments");
			for (Value itm : attachments) {

				Value offset = itm["offset"];
				if (offset.defined()) {
					auto attchdef = attachments.object(itm.getKey());
					Offset ofs = offset.getUInt();
					Value attch;
					{
						SharedSync _(lock);
						attch = loadDocument(ofs);
					}
					attchdef.set("offset", (Offset)outf.tellp());
					attch.serializeBinary(writter, json::compressKeys);
				}
			}
			Offset newOfs = outf.tellp();
			Value(docEdit).serializeBinary(writter, json::compressKeys);
			newDocMap.insert(DocMap::value_type(itm.first, DocInfo(itm.second.seqNum,newOfs)));
		}
	}

	{
		Offset maxOffset = lastMaxOffset;

		Sync _(lock);

		while (true) {
			std::size_t docOffset = outf.tellp();
			Value doc = loadDocument(maxOffset);
			if (doc.defined()) {
				if (canIndexDocument(doc)) {
					putDocToIndex(newDocMap,doc, docOffset);
				}
				Value(doc).serializeBinary(writter, json::compressKeys);
			} else {
				break;
			}
		}


		dbfile.close();
		rename(cmpName.c_str(), dbpname.c_str());

		lastSeqNum = generateSeqNumMap(newDocMap, newSeqNumMap);

		std::swap(newDocMap, docMap);
		std::swap(newSeqNumMap, seqNumMap);
		syncFileSize = maxOffset;
		chkpoint->erase();

	}

}

SimpleDocStorage::Offset SimpleDocStorage::saveDocument(Value rawDoc) {
	dbfile.seekp(syncFileSize);
	SimpleDocStorage::Offset ret = syncFileSize;
	rawDoc.serializeBinary([&](char c){
		dbfile.put(c);
	});
	syncFileSize = dbfile.tellp();
	return ret;
}

Value SimpleDocStorage::uploadDocument(Value doc) {

	Value idv=doc["_id"];
	if (!idv.defined()) return updateError(nullptr, "document", "Document has no id");
	if (idv.type() != json::string)
		return updateError(idv, "document", "Document's _id must be a string");
	String id(idv);
	Value seqid = doc["_local_seq"];
	if (seqid.defined()) {
		auto iter = docMap.find(id);
		if (iter == docMap.end()) {
			if (seqid.getUInt() != 0) return updateError(idv, "conflict", "Document doesn't exist");
		} else {
			if (iter->second.deleted) {
				if (seqid.getUInt() != 0 && seqid.getUInt() != iter->second.seqNum)
					return updateError(idv, "conflict", "Document is deleted");
			} else {
				if (seqid.getUInt() != iter->second.seqNum) {
					return updateError(idv, "conflict", "Document has been changed");
				}
			}
		}
	}


	Object ndoc(doc);
	bool deleted = doc["_deleted"].getBool();
	if (deleted && cfg.flags & Config::deletedMakeTombstone) {
		ndoc.setBaseObject(json::object);
		ndoc.set("_id", idv);
		ndoc.set("_deleted",true);
		ndoc.set("_rev", doc["_rev"]);
	} else {
		Value att = doc["_attachments"];
		if (att.type() == json::object) {
			auto attobj = ndoc.object(att.getKey());
			uploadAttachments(attobj);
		}
	}
	SeqNum sqn = lastSeqNum+1;
	ndoc.set("_local_seq", sqn);
	lastSeqNum = sqn;
	Value sdoc (ndoc);
	Offset pos = saveDocument(sdoc);
	if (deleted && cfg.flags & Config::deleteFromIndex) {
		auto iter = docMap.find(id);
		if (iter != docMap.end()) {
			seqNumMap.erase(iter->second.seqNum);
			docMap.erase(iter);
		}
	} else {
		DocInfo &dnf = docMap[id];
		seqNumMap.erase(dnf.seqNum);
		dnf.deleted = deleted;
		dnf.offset = pos;
		dnf.seqNum = sqn;
	}
	lock.unlock();
	try {
		notifyObservers(sdoc);
		if (sqn % cfg.updates == 0) {
			makeCheckpoint();
		}
	}
	catch (...) {

	}
	lock.lock();
	return sdoc;

}


}

