#pragma once
#include <imtjson/value.h>
#include <shared_mutex>

#include "abstractCheckpoint.h"
#include "couchDB.h"
#include "revision.h"
#include "view.h"
#include "query.h"
#include "changes.h"

namespace couchit {

using namespace json;

class Query;
class CouchDB;
class View;

class EmitFn {
public:
	virtual void operator()(const Value &key, const Value &value) const = 0;
};


class MemViewDef {
public:

	typedef std::function<void(const Value &, const EmitFn &Fn)> MapFn;
	typedef std::function<Value(const Result &)> ListFn;

	MapFn mapFn;
	ListFn listFn;

	MemViewDef(MapFn fn):mapFn(fn),listFn(nullptr) {}
	MemViewDef(MapFn mapFn, ListFn listFn):mapFn(mapFn),listFn(listFn) {}

};


///Memory view
/** a view materialized in the memory. It brings similar functionality as couchdb's view but
 * faster, however if has some limitations
 */

class MemView: public IChangeObserver {
public:



	typedef unsigned int Flags;

	static const Flags  flgIncludeDocs = 0x1;
	static const Flags  flgIncludeDesignDocs = 0x2;


	MemView(Flags flags = 0):flags(flags),queryable(*this),viewDef(&defaultMapFn) {}

	MemView(MemViewDef viewDef, Flags flags = 0):flags(flags),queryable(*this), viewDef(viewDef) {}

	SeqNumber load(const Query &q);
	SeqNumber load(CouchDB &db, const View &view);


	///Sets config of generating checkpoints
	/**
	 * Checkpoint is the view stored as file on the HDD. To maintain abstract level, the abstract
	 * checkpoint service requires just storing a loading functions. Function also loads the data
	 * from the last checkpoint and initializes updateSeq. You can call update() after checkpoint
	 * is loaded. Checkpoints are generated after reasoned updates are made.
	 * @param checkpointFile definition of checkpoint file.
	 * @param serialNr serial number of current database
	 * @param saveInterval interval in updates.Default value is 1000 updates so every 1000th
	 * update new checkpoint is stored (replacing the oldone)
	 */
	void setCheckpointFile(const PCheckpoint &checkpointFile, Value serialNr, std::size_t saveInterval = 1000);


	virtual void mapDoc(const Value &document, const EmitFn &emitFn);


	bool haveDoc(const String &docId) const;


	///Directly erases the document from the view
	/**
	 * @param docId documentId to erase
	 */
	void eraseDoc(const String &docId);
	///Directly adds the document
	/**
	 *
	 * @param doc document (must have "_id")
	 * @param key key (must not be null, but can contain a null value)
	 * @param value value (must not be null, but can contain a null value)
	 *
	 * Function will not add document if already exists with the same key. However, document can
	 * be inserted with a different key.
	 */
	void addDoc(const String &id, const Value &doc, const Value &key, const Value &value);


	///Asks for a document
	/**
	 * @param docId document id
	 * @return the document content, or null if not found
	 *
	 * @note the document must be included in the view.
	 */
	Value getDocument(const String &docId) const;

	///Query object which can ask LocalView
	/** You can create Query using createQuery() function.
	 *
	 * Query inherits QueryBase, so you can use same functions to ask local views
	 */
	class Queryable: public IQueryableObject  {
	public:
		Queryable(const MemView &mview);

		virtual Value executeQuery(const QueryRequest &r);

	protected:
		const MemView &mview;
	};

	///Creates Query object to ask LocalView
	/**
	 * @param viewFlags various flags defined by View object. You cannot supply View object directly
	 * because it is connected to CouchDB's view. However, you can supply its flags
	 *
	 * @return created query.
	 */
	Query createQuery(std::size_t viewFlags) const;

	///Creates Query object to ask LocalView with list function
	/**
	 *
	 * @param viewFlags various flags defined by View object. You cannot supply View object directly
	 * @param fn function to postprocess results.
	 * @param db optional pointer to an CouchDB instance. This pointer is used as argumet of
	 * the function and it has no other meaning. Default value is nullptr, so by default,
	 * the postprocess function is called with nullptr
	 *
	 *
	 * @return query
	 */
	Query createQuery(std::size_t viewFlags, View::Postprocessing fn, CouchDB *db = nullptr) const;

	///clear the view;
	void clear();

	bool empty() const;

	static void defaultMapFn(const Value &document, const EmitFn &emitFn);

	virtual void onChange(const ChangedDoc &doc);


	void addDoc(const Value &doc);

	///Updates the view from the changes feed
	/** The view must be previously loaded by load(), or cleared by clear();
	 *
	 * @param db database to read
	 * @note only one update can be active at time
	 */
	void update(CouchDB &db);

	///Updates the view if needed.
	/**
	 * "Needed" is meant as the view is old and there is no other update process
	 *
	 * @param db database which stream is used to update view
	 * @param wait set true to wait for finishing current update. Set false to return immmediatelly
	 * when there is other update processs
	 *
	 * If update is not needed, then function doesn't perform request to the database. However
	 * it requires to have updated "last known sequence id". Failing to update this
	 * global state causes that function reject to update the view even if there are new data
	 */
	bool updateIfNeeded(CouchDB &db, bool wait = false);


	Value getUpdateSeq() const;

	///manually creates a checkpoint
	void makeCheckpoint();
	///manually creates a checkpoint to specified store
	void makeCheckpoint(PCheckpoint chkpStore);

protected:

	Flags flags;

	struct KeyAndDocId {
		Value key;
		String docId;

		KeyAndDocId() {}
		KeyAndDocId(const Value &key,const String &docId):key(key),docId(docId) {}

		int compare(const KeyAndDocId &other) const;

	};

	struct ValueAndDoc {
		Value value;
		Value doc;

		ValueAndDoc() {}
		ValueAndDoc(const Value &value,const Value &doc):value(value),doc(doc) {}
	};


	struct CmpKeyAndDocId {
		bool operator()(const KeyAndDocId &l, const KeyAndDocId &r) const { return l.compare(r) < 0;}
	};


	///Contains for each document set of keys
	/** It is used to easy find keys to erase during update */
	typedef std::multimap<String, Value> DocToKey;
	///Contains keys mapped to documents
	/** Key contains the key itself and documentId to easyly handle duplicated keys */
	typedef std::map<KeyAndDocId, ValueAndDoc, CmpKeyAndDocId> KeyToValue;

	///Contains map where documendID is key and view's key is value
	/** This helps to search all keys for selected document. The documentID string can
	 * be taken from the _id (directly as address and length, because this string should not be changed)
	 */
	DocToKey docToKeyMap;

	///Contains the view itself - keys and values
	/** @note Note that keys are stored as KeyAndDocId. This allows to make duplicated keys */
	KeyToValue keyToValueMap;

	mutable Queryable queryable;

	friend class Queryable;
	Value runQuery(const QueryRequest& r) const;

	void addDocLk(const String &id, const Value &doc, const Value &key, const Value &value);


	typedef std::shared_timed_mutex Lock;
	typedef std::unique_lock<Lock> Sync;
	typedef std::shared_lock<Lock> SharedSync;
	typedef std::unique_lock<std::mutex> USync;

	mutable Lock lock;
	mutable std::mutex updateLock;


	Value getAllItems() const;
	Value getItemsByKeys(const json::Array &keys) const;
	Value getItemsByRange(const json::Value &from, const json::Value &to, bool exclude_end, bool extractDocIDs) const;
	void updateLk(CouchDB &db);
	void eraseDocLk(const String &docId);

	MemViewDef viewDef;
	Value updateSeq;

	PCheckpoint chkpStore;
	std::size_t chkpNextUpdate = 0;
	std::size_t chkpInterval = 0;
	Value chkSrNr;

	void onUpdate();

public:
	class DirectAccess {
	public:
		typedef KeyToValue::const_iterator iterator;
		typedef KeyToValue::const_iterator const_iterator;


		DirectAccess(const KeyToValue &kmap, Lock &lock):kmap(kmap),sync(lock) {}
		const_iterator begin() const {
			return kmap.begin();
		}
		const_iterator end() const {
			return kmap.end();
		}
		const_iterator lower_bound(const Value &key) const {
			return kmap.lower_bound(KeyAndDocId(key,String()));
		}
		const_iterator upper_bound(const Value &key) const {
			return kmap.lower_bound(KeyAndDocId(key,Query::maxString));
		}
		std::pair<const_iterator,const_iterator> equal_range(const Value &key) const {
			return std::pair<const_iterator,const_iterator>(
					lower_bound(key),upper_bound(key)
					);
		}
		const_iterator lower_bound(const Value &key, const String &docId) const {
			return kmap.lower_bound(KeyAndDocId(key,docId));
		}
		const_iterator upper_bound(const Value &key, const String &docId) const {
			return kmap.lower_bound(KeyAndDocId(key,docId));
		}


	protected:
		const KeyToValue &kmap;
		SharedSync sync;

	};

	///access the view directly
	/**
	 * Returns object which can be used to access the view directly using the standard c++ iterators
	 *
	 * @return direct access interface
	 *
	 * @note If the object has MT locking, then it remains locked until the last instance of the DirectAccess
	 * is destroyed
	 */
	DirectAccess direct() const {return DirectAccess(keyToValueMap,lock);}
};




}
