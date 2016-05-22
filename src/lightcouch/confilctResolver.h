/*
 * confilctResolver.h
 *
 *  Created on: May 22, 2016
 *      Author: ondra
 */

#ifndef LIBS_LIGHTCOUCH_SRC_LIGHTCOUCH_CONFILCTRESOLVER_H_
#define LIBS_LIGHTCOUCH_SRC_LIGHTCOUCH_CONFILCTRESOLVER_H_

#include "lightspeed/utils/json/json.h"
#include "lightspeed/base/containers/constStr.h"
#include "object.h"

namespace LightCouch {

class CouchDB;
class Document;

using namespace LightSpeed;

///Helps to resolve conflicts
/** uses 3-way resolver. Downloads top revision, base revision and conflicted revision and applies
 * changes from conflicted revision to the top revision. Class can be inherited to
 * modify way how the merge is performed
 */
class ConfilctResolver {
public:
	ConfilctResolver(CouchDB &db);

	///Resolves the conflicts on the document
	/**
	 * @param docId ID of conflicted document.
	 * @return document with resolved conflicts. You need to put document into ChangeSet to store it in database
	 */
	Document resolve(const ConstStrA &docId);


protected:

	struct Path {
		const ConstStrA key;
		const Path *parent;

		Path(ConstStrA key):key(key),parent(0) {}
		Path(const Path &parent, ConstStrA key):key(key),parent(&parent) {}

	};

	///Perform 3way merge. You can overwrite function specify own rules
	/** Function by default merges conflicty to the document by writting changed values relative to base
	 *
	 * @param doc current (top) document.
	 * @param conflict conflicted document.
	 * @param base base document
	 *
	 * @return merged document. If you need to reject revision, return null
	 *
	 * @note Result document must pass validation rules. If document is rejected, changes from revision will discarded
	 *
	 */
	virtual Value merge3w(const ConstValue &doc, const ConstValue &conflict, const ConstValue &base);

	///Perform 2way merge. You have to overwrite function because default implementation will reject the conflict
	/**
	 *
	 * @param doc top document
	 * @param conflict conflicted document
	 * @return
	 */
	virtual Value merge2w(const ConstValue &doc, const ConstValue &conflict);

	///Performs merge of values
	/**
	 * @param doc top document
	 * @param path path to the value (key name)
	 * @param oldValue current value in the top document. The variable is nil, if newValue is inserted
	 * @param newValue new value. The variable is nil, if key is being erased
	 * @param baseValue value common for both revisions
	 * @return Value to store at given key. Set to nil, if you want to erase the key.
	 *
	 * Default implementation will return newValue. If both values are objects, function performs recursive
	 * merge for keys in the object. Then it returns merged object.
	 *
	 * @note you can modify document if you need. On each call, document contains updated version
	 * as the function processes for each key in document
	 */
	virtual ConstValue mergeValue(Document &doc, const Path &path, const ConstValue &oldValue, const ConstValue &newValue, const ConstValue &baseValue);


	CouchDB &db;


	Value mergeObject(Document &doc, const Path &path, const ConstValue &oldValue, const ConstValue &newValue, const ConstValue &baseValue);
};

} /* namespace LightCouch */

#endif /* LIBS_LIGHTCOUCH_SRC_LIGHTCOUCH_CONFILCTRESOLVER_H_ */
