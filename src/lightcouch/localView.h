/*
 * localView.h
 *
 *  Created on: 5. 6. 2016
 *      Author: ondra
 */

#ifndef LIBS_LIGHTCOUCH_SRC_LIGHTCOUCH_LOCALVIEW_H_
#define LIBS_LIGHTCOUCH_SRC_LIGHTCOUCH_LOCALVIEW_H_


#include "lightspeed/mt/rwlock.h"
#include "query.h"

#include "lightspeed/base/compare.h"

#include "lightspeed/base/actions/promise.h"

#include "lightspeed/base/containers/map.h"
namespace LightCouch {



///View with similar features created in memory
/** This view works very similar as classical CouchDB's view. However it is
 * created whole in memory of the application's memory; there is no communication with the CouchDB. The view
 * can be either created as an snapshot of a view on the CouchDB server, or it can be brand new
 * view which is created only for the application itself. The view can be synchronized from _changes interface, even
 * asynchronously ( - using extra thread, because the object is MT safe!).
 *
 * Because the view is not backed in permanent memory (disk), the application need to recreate it during its startup.
 * This can take a lot of time. However, you can create a view on CouchDB server, which will be loaded on
 * every start in order to recreate the LocalView
 *
 */
class LocalView {
public:
	LocalView();
	virtual ~LocalView();




	///Update document
	/**
	 * @param doc document to update. To erase document, the document need to have _deleted flag. The
	 * changes stream must not skip deleted documents
	 *
	 * Also note, if the document no longer matches conditions defined by the map() function, it will
	 * be also deleted.
	 *
	 * @note The document must have "_id" at least.
	 */
	void updateDoc(const Value &doc);

	///Function loads content of the view from the couchdb's view
	/**
	 *
	 * @param db database connection
	 * @param view a view defintion.
	 * @param runMapFn if this is true, every result of the view is processed by the map() function. This
	 * require the view has the flag includeDocs enabled (otherwise, exception is thrown). If this argument
	 * is false, function will directly insert result to the LocalView without processing it by the map() function.
	 * The second way is much faster, but it can make the view inconsistent. Additionally, the second
	 * version don't need to have includeDocs. In such a case, whole documents will not available.
	 */
	void loadFromView(CouchDB &db, const View &view, bool runMapFn );

	///Directly erases the document from the view
	/**
	 * @param docId documentId to erase
	 */
	void eraseDoc(const StrView &docId);
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

	///Asks for a document
	/**
	 * @param docId document id
	 * @return the document content, or null if not found
	 *
	 * @note the document must be included in the view.
	 */
	Value getDocument(const StrView &docId) const;


	typedef View::Postprocessing PostProcessFn;

	///Query object which can ask LocalView
	/** You can create Query using createQuery() function.
	 *
	 * Query inherits QueryBase, so you can use same functions to ask local views
	 */
	class Queryable: public IQueryableObject  {
	public:
		Queryable(const LocalView &lview);

		virtual Value executeQuery(const QueryRequest &r);

	protected:
		const LocalView &lview;
	};

	///Creates Query object to ask LocalView
	/**
	 * @param viewFlags various flags defined by View object. You cannot supply View object directly
	 * because it is connected to CouchDB's view. However, you can supply its flags
	 *
	 * @return created query.
	 */
	Query createQuery(natural viewFlags) const;

	///Creates Query object to ask LocalView with list function
	/**
	 *
	 * @param viewFlags various flags defined by View object. You cannot supply View object directly
	 * @param fn function to postprocess results.
	 *
	 *
	 * @return query
	 *
	 * @note function is not compatible with function from View, because it cannot supply active
	 *  CouchDB client. You have to supply it in function instance.
	 */
	Query createQuery(natural viewFlags, PostProcessFn fn) const;


protected:


	struct KeyAndDocId: public Comparable<KeyAndDocId> {
		Value key;
		StrView docId;

		KeyAndDocId() {}
		KeyAndDocId(const Value &key,const StrView &docId):key(key),docId(docId) {}

		CompareResult compare(const KeyAndDocId &other) const;

	};

	struct ValueAndDoc {
		Value value;
		Value doc;

		ValueAndDoc(const Value &value,const Value &doc):value(value),doc(doc) {}
	};


	///Perform map operation
	/**
	 * @param doc document to map
	 *
	 * Function must call emit() at least once for the document, if it need to include it to the view. Otherwise
	 * document will be excluded.  Function emit() can be called by multiple times, each call will
	 * introduce new key. Note that emitting the same key twice for the same document is not allowed (unsupported),
	 * however, two documents are allowd to emit the same key and create duplicated keys.
	 */
	virtual void map(const Value &doc) ;

	///Perform reduce operation
	/**
	 * called everytime reduce is required.
	 *
	 * @param keys list of keys
	 * @param values list of values
	 * @param rereduce if true, extra reduce cycle is being executed. In this case, the array keys is empty.
	 * @return result of reduce operation. If null pointer is return (not json's null value), reduce is
	 * not available for this null. The default implementation return null pointer (so reduce is not available
	 * until it is implemented)
	 *
	 * @note currently function is not optimized and will not store reduced subresults. Everytime the reduce
	 * is required, the function is called again and again
	 */
	virtual Value reduce(const ConstStringT<KeyAndDocId>  &keys, const ConstStringT<Value> &values, bool rereduce) const;



	///Emit key and value
	/** The function can be called during map() function
	 *
	 * @param key key of current document
	 * @param value value of current document
	 */
	virtual void emit(const Value &key, const Value &value);
	///Emit key
	/** The function can be called during map() function
	 *
	 * @param key key of current document
	 *
	 * @note value is set to JSON's null
	 */
	virtual void emit(const Value &key);
	///Store current document to the view
	/** The function can be called during map() function
	 *
	 * @note document is stored under "null" key with no value. You can use this function to build
	 * simple database of document. This is also default behaviour of map() function.
	 *
	 */
	virtual void emit();


	///RW lock for MT access
	mutable RWLock lock;

	typedef Synchronized<RWLock::ReadLock> Shared;
	typedef Synchronized<RWLock::WriteLock> Exclusive;



	///Contains for each document set of keys
	/** It is used to easy find keys to erase during update */
	typedef MultiMap<StrView, Value> DocToKey;
	///Contains keys mapped to documents
	/** Key contains the key itself and documentId to easyly handle duplicated keys */
	typedef Map<KeyAndDocId, ValueAndDoc> KeyToValue;

	///Contains map where documendID is key and view's key is value
	/** This helps to search all keys for selected document. The documentID string can
	 * be taken from the _id (directly as address and length, because this string should not be changed)
	 */
	DocToKey docToKeyMap;

	///Contains the view itself - keys and values
	/** @note Note that keys are stored as KeyAndDocId. This allows to make duplicated keys */
	KeyToValue keyToValueMap;

	///Current document being currently processed
	Value curDoc;

	mutable Queryable queryable;



	void eraseDocLk(const StrView &docId);
	void addDocLk(const Value &doc, const Value &key, const Value &value);
	void updateDocLk(const Value &doc);


	Value searchKeys(const Value &keys, natural groupLevel) const;
	Value searchOneKey(const Value &key) const;
	Value searchRange(const Value &startKey, const Value &endKey,
			natural groupLevel, bool descending, natural offset, natural limit,
			const StrView & offsetDoc,
			bool excludeEnd) const;


	Value runReduce(const Value &rows) const;

};


} /* namespace LightCouch */


#endif /* LIBS_LIGHTCOUCH_SRC_LIGHTCOUCH_LOCALVIEW_H_ */
