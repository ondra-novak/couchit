#pragma once
#include <imtjson/value.h>
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

///Map request
/**
 * During mapping documents to the MemView, a user function is called with the "MapReq", which
 * contains mapped document and can be used to emit rows to the final view
 */
class MapReq: public Value {
public:

	MapReq(){}
	MapReq(const Value &doc):Value(doc) {}
	///Emits one row to the view
	/**
	 * @param key key
	 * @param value value
	 *
	 * @note Limitation. One document can emit only one row per key. You cannot emit multiple rows per
	 * single key and document - the couple [key, docid] is the primary key
	 */
	virtual void operator()(const Value& key, const Value &value) const = 0;
	StrViewA getID() const;
	StrViewA getRev() const;
	Value getIDValue() const;
	Value getRevValue() const;
	Value getConflicts() const;
	void emit(const Value& key, const Value &value) const {operator()(key,value);}
};


///Memory view
/** a view materialized in the memory. It brings similar functionality as couchdb's view but
 * faster, however if has some limitations
 */

class MemView {
public:

	typedef unsigned int Flags;

	static const Flags  flgIncludeDocs = 0x1;

	MemView(Flags flags = 0):flags(flags),queryable(*this) {}

	void load(const Query &q);
	void load(CouchDB &db, const View &view);

	template<typename Fn>
	void load(const Query &q, const Fn &mapFn);

	template<typename Fn>
	void load(CouchDB &db, const View &view, const Fn &mapFn);



	template<typename Fn>
	void update(CouchDB &database, const Fn &mapFn);

	template<typename Fn>
	void update(CouchDB &database, const Filter &flt, const Fn &mapFn);

	template<typename Fn>
	void updateDoc(const Value &doc, const Fn &mapFn);


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
	void addDoc(const Value &doc, const Value &key, const Value &value);


	template<typename Fn>
	void addDoc(const Value &doc, const Fn &fn);

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

	void clear();

	bool empty() const;

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
	SeqNumber updateSeq;

	friend class Queryable;
	Value runQuery(const QueryRequest& r) const;


	Value getAllItems() const;
	Value getItemsByKeys(const json::Array &keys) const;
	Value getItemsByRange(const json::Value &from, const json::Value &to, bool exclude_end, bool extractDocIDs) const;

public:
	class DirectAccess {
	public:
		typedef KeyToValue::const_iterator iterator;
		typedef KeyToValue::const_iterator const_iterator;


		DirectAccess(const KeyToValue &kmap):kmap(kmap) {}
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
	DirectAccess direct() const {return DirectAccess(keyToValueMap);}
};


template<typename Fn>
inline void MemView::load(const Query& q, const Fn& mapFn) {
	updateSeq = Value(json::undefined);
	clear();
	Result res = q.exec();

	for (Row rw : res) {
		addDoc(rw.doc,mapFn);
	}
	updateSeq = res.getUpdateSeq();
}

template<typename Fn>
inline void MemView::load(CouchDB& db, const View& view,
		const Fn& mapFn) {
	load(db.createQuery(view), mapFn);

}

template<typename Fn>
inline void MemView::update(CouchDB& database, const Fn& mapFn) {
	ChangesFeed chf = database.createChangesFeed();
	Changes chs = chf.since(updateSeq).includeDocs().exec();
	for (ChangedDoc chd : chs.getAllChanges()) {
		updateDoc(chd.doc, mapFn);
	}
	updateSeq = chf.getLastSeq();
}

template<typename Fn>
inline void MemView::update(CouchDB& database, const Filter& flt, const Fn& mapFn) {
	ChangesFeed chf = database.createChangesFeed();
	Changes chs = chf.since(updateSeq).includeDocs().setFilter(flt).exec();
	for (ChangedDoc chd : chs.getAllChanges()) {
		updateDoc(chd.doc, mapFn);
	}
	updateSeq = chf.getLastSeq();
}

template<typename Fn>
inline void MemView::updateDoc(const Value& doc, const Fn& mapFn) {
	eraseDoc(String(doc["_id"]));
	if (!doc["_deleted"].getBool())  {
		addDoc(doc,mapFn);
	}
}

template<typename Fn>
inline void MemView::addDoc(const Value& doc, const Fn& mapFn) {
	class EmitDoc: public MapReq {
	public:
		EmitDoc(MemView *owner, const Value &doc) :	MapReq(doc),owner(owner) {}
		virtual void operator()(const Value& key, const Value &value) const override {
			owner->addDoc(key,value,*this);
		}

	protected:
		MemView *owner;
	};

	EmitDoc mr(this,doc);
	mapFn(mr);
}



}

