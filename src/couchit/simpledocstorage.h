#pragma once

#include <fstream>
#include <shared_mutex>

#include "abstractCheckpoint.h"
#include "documentdb.h"

namespace couchit {


///allows to store document without need to connect CouchDB.
/** Very simple, very easy, but effective storage for small or medium set of documents. The set
 * should fit into memory, however, the storage allows to store documents at disk.
 *
 * Documents are stored in signle append only file. There is no internal structure or tree, just
 * file, where each document is put one after other. Lookup indexes are kept whole in memory. They
 * are rebuild everytime the storage is opened. To speedup this operation, the checkpoints
 * can be created after count of writes.
 *
 * The SimpleDocStorage supports ChangesFeed, so you can connect it with Memview to create in-memory views.
 *
 * The SimpleDocStorage is not intended for master-master replication. The revision ID of each document is
 * just compared and conflicts are reported during writting operation. There is no other way to
 * create a conflict.
 */

class SimpleDocStorage: public DocumentDB {
public:


	struct Config {
		///count of updates between checkpoints
		/** Index data are stored in memory and flushed each nth update. When database is
		 * opened, the checkpoint is used to initialize the memory index. Rest of the
		 * index is synced from the database file. Higher number reduces frequency of the
		 * storing the checkpoints, but causes that database is opened slower.
		 */
		unsigned int updates;
		///pointer to ID generator. If null, default is used
		const IIDGen *idgen;
		///pointer to validator. If null, none is used
		const Validator *validator;

		Config ():updates(1000), idgen(nullptr), validator(nullptr) {}
	};

	SimpleDocStorage(const Config &cfg = Config());

	///Open database
	/**
	 * Database consists from two files. The database file contains stored documents,
	 * the checkpoint files contains checkpoint of the memory index. The checkpoint
	 * allows to regenerate the index faster. If the checkpoint file doesn't exists, it
	 * is created, but the opening the DB can be slower.
	 *
	 * @param dbfile pathname to database file
	 * @param chkpointFile checkpoint file
	 *
	 * @note if database file doesn't exists, the function throws an  exception
	 */
	void open(String dbfile, PCheckpoint chkpointFile);
	///Creates new database
	/**
	 * Creates new database. Deletes existing database
	 *
	 * @param dbfile new database file
	 * @param chkpointFile new checkpoint file
	 */

	void create(String dbfile, PCheckpoint chkpointFile);
	///flushes checkpoint to disk
	void makeCheckpoint();

	virtual Changes receiveChanges(ChangesFeed &sink) ;
	virtual void receiveChangesContinuous(ChangesFeed &sink, ChangesFeedHandler &fn);

	///Bulk upload
	virtual Value bulkUpload(const Value docs) ;
	virtual SeqNumber getLastSeqNumber() ;
	virtual SeqNumber getLastKnownSeqNumber() const ;
	virtual Query createQuery(const View &view) ;
	virtual Query createQuery(Flags viewFlags) ;
	virtual Changeset createChangeset() ;
	virtual ChangesFeed createChangesFeed() ;
	virtual Value getLocal(const StrViewA &localId, Flags flags = 0) ;
	virtual Value get(const StrViewA &docId, Flags flags = 0) ;
	virtual Value get(const StrViewA &docId, const StrViewA &revId, Flags flags = flgDisableCache);
	virtual Upload putAttachment(const Value &document, const StrViewA &attachmentName, const StrViewA &contentType);
	String putAttachment(const Value &document, const StrViewA &attachmentName, const AttachmentDataRef &attachmentData);
	virtual Download getAttachment(const Document &document, const StrViewA &attachmentName,  const StrViewA &etag=StrViewA());
	virtual Download getAttachment(const StrViewA &docId, const StrViewA &attachmentName,  const StrViewA &etag=StrViewA());
	virtual void put(Document &doc);
	virtual Validator *getValidator() const;



private:

	typedef std::uint64_t Offset;
	typedef std::uint64_t SeqNum;


	struct DocInfo {
		SeqNum seqNum;
		Offset offset;
	};


	///Map of documents
	typedef std::map<String, DocInfo> DocMap;
	///Simple index, maps sequence numbers to offsets
	/** this is restured from DocMap */
	typedef std::map<SeqNum, Offset> SeqNumMap;

	DocMap docMap;
	SeqNumMap seqNumMap;
	std::fstream dbfile;

	String dbpname;
	PCheckpoint chkpoint;
	std::size_t seqnum;
	Offset syncFileSize;


	Config cfg;

	std::shared_timed_mutex lock;
	typedef std::shared_lock<std::shared_timed_mutex> SharedSync;
	typedef std::unique_lock<std::shared_timed_mutex> Sync;
	void loadCheckpointAndSync();



};



}
