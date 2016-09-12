/*
 * changeDoc.cpp
 *
 *  Created on: 21. 3. 2016
 *      Author: ondra
 */

#include "changes.h"

#include "couchDB.h"
namespace LightCouch {


static bool safeBool(const JSON::INode *ptr) {
	if (ptr == 0) return false;
	return ptr->getBool();
}



ChangedDoc::ChangedDoc(const ConstValue& allData)
:allData(allData)
,seqId(allData["seq"]->getUInt())
,id(allData["id"]->getStringUtf8())
,revisions(allData["changes"])
,deleted(safeBool(allData->getPtr("deleted")))
,doc(allData->getPtr("doc"))
{
}

Changes::Changes(ConstValue jsonResult)
:rows(jsonResult),rowIter(rows->getFwIter())
{

}

const ConstValue& Changes::getNext() {
	return rowIter.getNext();
}

const ConstValue& Changes::peek() const {
	return rowIter.peek();
}

bool Changes::hasItems() const {
	return rowIter.hasItems();
}

void Changes::rewind() {
	rowIter = rows->getFwIter();
}

ChangesSink::ChangesSink(CouchDB& couchdb)
	:json(couchdb.json),couchdb(couchdb), seqNumber(0),outlimit(naturalNull),timeout(0)
	,cancelState(0)
{
}

ChangesSink& ChangesSink::fromSeq(natural seqNumber) {
	this->seqNumber = seqNumber;
	return *this;
}

ChangesSink& ChangesSink::setTimeout(natural timeout) {
	this->timeout = timeout;
	return *this;
}

ChangesSink& ChangesSink::setFilter(const Filter& filter) {
	this->filter = filter;
	filterArgs = null;
	return *this;
}

ChangesSink& ChangesSink::unsetFilter() {
	this->filter = null;
	filterArgs = null;
	return *this;
}


ChangesSink& ChangesSink::limit(natural count) {
	this->outlimit = count;
	return *this;
}

Changes ChangesSink::exec() {
	return couchdb.receiveChanges(*this);
}

ChangesSink& ChangesSink::setFilterFlags(natural flags) {
	return setFilter(Filter(ConstStrA(),flags));
}

void ChangesSink::cancelWait() {
	lockCompareExchange(cancelState,0,1);
}

}
