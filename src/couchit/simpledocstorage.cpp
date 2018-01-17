#include "simpledocstorage.h"

#include <imtjson/binjson.tcc>

#include "checkpointFile.h"


#include "defaultUIDGen.h"
#include "exception.h"

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
	docMap[id] = DocInfo(seqNum, offset);
	return seqNum;
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


}
