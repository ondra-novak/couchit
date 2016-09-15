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
	LocalView(const Json &json);
	virtual ~LocalView();


	class Query;


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
	void updateDoc(const ConstValue &doc);

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
	void eraseDoc(ConstStrA docId);
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
	void addDoc(const ConstValue &doc, const ConstValue &key, const ConstValue &value);

	///Asks for a document
	/**
	 * @param docId document id
	 * @return the document content, or null if not found
	 *
	 * @note the document must be included in the view.
	 */
	ConstValue getDocument(const ConstStrA docId) const;


	///Postprocess function
	/**
	 * @param Json json object
	 * @param ConstValue arguments from query
	 * @param ConstValue result from query
	 * @return Modified result
	 *
	 */
	typedef std::function<ConstValue(Json , ConstValue , ConstValue )> PostProcessFn;

	///Query object which can ask LocalView
	/** You can create Query using createQuery() function.
	 *
	 * Query inherits QueryBase, so you can use same functions to ask local views
	 */
	class Query: public QueryBase {
	public:
		Query(const LocalView &lview, const Json &json, natural viewFlags, PostProcessFn ppfn);

		virtual Result exec() const override;

	protected:
		const LocalView &lview;
		PostProcessFn ppfn;
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


	///Item of update stream
	struct UpdateStreamItem {
		///document to update in views
		ConstValue document;
		///contains initialized future for next item
		Future<UpdateStreamItem> nextItem;

		UpdateStreamItem (const ConstValue &document, const Future<UpdateStreamItem> &nextItem)
			:document(document),nextItem(nextItem) {}
	};


	///Update stream - you can update various views from single source
	/** Just create an UpdateStream and call getPromise() on it. UpdateStream
	 * is future of future, where each future is carrying future to next item.
	 */
	typedef Future<UpdateStreamItem> UpdateStream;

	///Creates souce of document for update
	/** Just create instance of this object, then create a stream using the createStream() and
	 * distribute stream through various views. Everytime tou call updateDoc(), these views will
	 * be updated.
	 *
	 * The stream can be stopped by destroying the object or by recreating the stream
	 */
	class DocumentSource {
	public:

		///Creates an update stream
		/** Note that function closes current stream, if any is active */
		UpdateStream createStream();
		///send update to every view
		void updateDoc(const ConstValue &doc);

	protected:
		Promise<UpdateStreamItem> nextItem;

	};


	void setUpdateStream(const UpdateStream &stream);

	UpdateStream getUpdateStream() const;

	const Json json;


protected:


	struct KeyAndDocId: public Comparable<KeyAndDocId> {
		ConstValue key;
		ConstStrA docId;

		KeyAndDocId() {}
		KeyAndDocId(ConstValue key,ConstStrA docId):key(key),docId(docId) {}

		CompareResult compare(const KeyAndDocId &other) const;

	};

	struct ValueAndDoc {
		ConstValue value;
		ConstValue doc;

		ValueAndDoc(ConstValue value,ConstValue doc):value(value),doc(doc) {}
	};

	class UpdateReceiver: public UpdateStream::IObserver {
	public:

		LocalView &view;

		UpdateReceiver(LocalView &view);

		virtual void resolve(const UpdateStreamItem &result) throw();
		virtual void resolve(const PException &e) throw();
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
	virtual void map(const ConstValue &doc) ;

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
	virtual ConstValue reduce(const ConstStringT<KeyAndDocId>  &keys, const ConstStringT<ConstValue> &values, bool rereduce) const;



	///Emit key and value
	/** The function can be called during map() function
	 *
	 * @param key key of current document
	 * @param value value of current document
	 */
	virtual void emit(const ConstValue &key, const ConstValue &value);
	///Emit key
	/** The function can be called during map() function
	 *
	 * @param key key of current document
	 *
	 * @note value is set to JSON's null
	 */
	virtual void emit(const ConstValue &key);
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
	typedef MultiMap<ConstStrA, ConstValue> DocToKey;
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
	ConstValue curDoc;


	UpdateStream updateStream;
	UpdateReceiver updateReceiver;


	void eraseDocLk(ConstStrA docId);
	void addDocLk(const ConstValue &doc, const ConstValue &key, const ConstValue &value);
	void updateDocLk(const ConstValue &doc);


	ConstValue searchKeys(const ConstValue &keys, natural groupLevel) const;
	ConstValue searchOneKey(const ConstValue &key) const;
	ConstValue searchRange(const ConstValue &startKey, const ConstValue &endKey,
			natural groupLevel, bool descending, natural offset, natural limit, ConstStrA offsetDoc,
			bool excludeEnd) const;


	ConstValue runReduce(const ConstValue &rows) const;

private:
	void cancelStream();
	void setUpdateStreamLk(UpdateStream stream);
};


} /* namespace LightCouch */


#endif /* LIBS_LIGHTCOUCH_SRC_LIGHTCOUCH_LOCALVIEW_H_ */
