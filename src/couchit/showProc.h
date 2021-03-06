/*
 * update.h
 *
 *  Created on: 13. 2. 2017
 *      Author: ondra
 */

#pragma once

#include "json.h"

namespace couchit {

///Contains result of external show procedude
class ShowResult {
public:
	ShowResult();
	///Construct the object from the response
	ShowResult(const Value &result, const String &contentType);
	///Retrieves content as binary string
	/**
	 * @return Function receives response as binary string - this function
	 * works only if the response is not json. To determine, whether response
	 * is json use function isJSON()
	 */
	BinaryView getContent() const;

	///Retrieves content as json
	/**
	 * @return Function receives response as JSON - this function
	 * works only if the response is JSON. To determine, whether response
	 * is json use function isJSON()
	 */
	Value getJSONContent() const;
	///Retrieves content type of the response
	/**
	 * @return Content type of response. In case the result is JSON, function returns "application/json".
	 */
	String getContentType() const;
	///Determines whether response is JSON
	/**
	 * @retval false response is not JSON. The function getContent() returns binary response
	 * @retval true response is JSON.
	 */
	bool isJSON();
protected:
	Value result;
	String contentType;

};


///Declaration of external update procedure
class ShowProc{
public:

	///Declares procedure by specifiing the path to the procedure
	/**
	 * @param path path to exteral show procedure
	 * @param cacheable specify true, when the result of the show procedure is cacheable. In this
	 *  case, result will be stored in cache. Note that only JSON results can be cacheable. If
	 *  the view returns non-json result, argument is ignored
	 *
	 */
	ShowProc(const String path, bool cacheable = true);
	///Executes external show procedure
	/**
	 * @param db specified database instance on which the external procedure will be executed
	 * @param docId specifies document-id on which the update will be executed. Document don't need to exist.
	 * @param arguments an arguments passed to the update function.
	 * @return result generated by show procedure. The procedure can return either JSON or binary
	 *  response. The class ShowResult can handle both types of result
	 */
	ShowResult operator()(CouchDB &db, StrViewA docId, Value arguments = json::undefined);

protected:
	String path;
	bool cacheable;

};


}





