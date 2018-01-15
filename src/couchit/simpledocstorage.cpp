#include "simpledocstorage.h"

#include <imtjson/binjson.tcc>


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

	seqnum = 0;
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
	seqnum = 0;
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

void SimpleDocStorage::makeCheckpoint() {
	typedef DocMap::value_type KeyValue;
	typedef std::vector<KeyValue> Items;

	typedef std::shared_ptr<Items> SharedItems;
	std::size_t sqn;
	SharedItems ptm (new Items);
	{
		SharedSync _(lock);
		ptm->reserve(docMap.size());
		for (auto &&itm:docMap) {
			ptm->push_back(itm);
		}
		sqn = seqnum;
	}




	chkpoint->store([ptm,sqn](std::ostream &stream){

		auto wrfn = [&](char c) {
			stream.put(c);
		};

		BinarySerializer<decltype(wrfn)> srz(wrfn, compressKeys);
		srz.serializeInteger(2, json::opcode::array);
		srz.serializeInteger(sqn, json::opcode::posint);
		srz.serializeInteger(ptm->size(), json::opcode::array);
		for (auto &&itm:*ptm) {
			srz.serializeInteger(3, json::opcode::array);
			srz.serializeString(itm.first, json::opcode::string);
			srz.serializeInteger(itm.second.offset, json::opcode::posint);
			srz.serializeInteger(itm.second.seqNum, json::opcode::posint);
		}

	});

}

void SimpleDocStorage::loadCheckpointAndSync() {

}


}
