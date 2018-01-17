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

		typedef unsigned int Flags;

		///Deleted documents are kept in database file and also in index
		/** This flag is default and emulates behaviour of the CouchDB. However
		 * deleted documents still occupy the space and affects performance
		 * negatively.
		 */
		static const Flags keepDeleted = 0;

		///Deleted documents are tombstoned into minimal
		/** This option automatically removes all keys, only _id, _rev and _deleted
		 * remains
		 */
		static const Flags deletedMakeTombstone = 1;

		///Once the document is deleted, it is also removed from the index
		/** This options causes, that the deleted document is no longer
		 * found by its ID (they are still appear in the changes stream).
		 * Option can help to better performance on database, where
		 * many documents are deleted. Also note, that deleted documents
		 * are removed during compaction.
		 *
		 *
		 */
		static const Flags deleteFromIndex = 2;


		///Deleted documents are removed during compaction.
		/** Until compaction, deleted documents are still available,
		 * (how - it depends on other flags) but these documents are removed
		 * during compaction
		 */
		static const Flags deletePurgeOnCompact = 4;

		///Deleted document is immediatelly purged
		/** This makes database fastest, but it cannot be used for replication*/
		static const Flags deletePurge = deleteFromIndex|deletedMakeTombstone;


		Flags flags;

		Config ():updates(1000), idgen(nullptr), validator(nullptr),flags(0){}
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

	void compact();

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
		DocInfo() {}
		DocInfo(SeqNum seqNum,Offset offset)
			:seqNum(seqNum), offset(offset) {}
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
	std::size_t lastSeqNum;
	Offset syncFileSize;


	Config cfg;

	std::shared_timed_mutex lock;
	typedef std::shared_lock<std::shared_timed_mutex> SharedSync;
	typedef std::unique_lock<std::shared_timed_mutex> Sync;
	void loadCheckpointAndSync();

	Value loadDocument(Offset &offset);
	Offset saveDocument(Value rawDoc);

	static SeqNum generateSeqNumMap(const DocMap &docMap, SeqNumMap &seqNumMap);
	static SeqNum putDocToIndex(DocMap &docMap, const Value &document, Offset offset);
	bool canIndexDocument(const Value &document);




};



}
