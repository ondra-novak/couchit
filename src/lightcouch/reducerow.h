/*
 * reducerow.h
 *
 *  Created on: 16. 11. 2016
 *      Author: ondra
 */

#ifndef LIGHTCOUCH_REDUCEROW_H_
#define LIGHTCOUCH_REDUCEROW_H_


namespace LightCouch {


class ReducedRow {
public:
	///contains value
	const Value value;

	ReducedRow(const Value value):value(value) {}
};
///Row - contains single row of view which is being reduced
class RowWithKey: public ReducedRow {
public:
	///contains document id
	const Value docId;
	///contains key
	const Value key;

	///constructor
	RowWithKey(const Value docId,const Value key,const Value value)
		:ReducedRow (value),docId(docId),key(key) {}
};

typedef json::StringView<RowWithKey> RowsWithKeys;
typedef json::StringView<ReducedRow> ReducedRows;




}


#endif /* LIGHTCOUCH_REDUCEROW_H_ */
