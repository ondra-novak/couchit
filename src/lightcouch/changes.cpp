/*
 * changeDoc.cpp
 *
 *  Created on: 21. 3. 2016
 *      Author: ondra
 */

#include "changes.h"

#include "couchDB.h"
namespace LightCouch {





ChangedDoc::ChangedDoc(const Value& allData)
:Value(allData)
,seqId(allData["seq"])
,id(allData["id"].getString())
,revisions(allData["changes"])
,deleted(allData["deleted"].getBool())
,doc(allData["doc"])
{
}

Changes::Changes(Value jsonResult)
:rows(jsonResult),pos(0),sz(jsonResult.size())
{

}

const Value& Changes::getNext() {
	vout = rows[pos++];
	return vout;
}

const Value& Changes::peek() const {
	vout = rows[pos];
	return vout;

}

bool Changes::hasItems() const {
	return pos < sz;
}

void Changes::rewind() {
	pos = 0;
}

ChangesSink::ChangesSink(CouchDB& couchdb)
	:couchdb(couchdb), outlimit(naturalNull),timeout(0),canceled(false)
{
}

ChangesSink& ChangesSink::fromSeq(const Value &seqNumber) {
	this->seqNumber = seqNumber;
	return *this;
}

ChangesSink& ChangesSink::setTimeout(natural timeout) {
	this->timeout = timeout;
	return *this;
}

ChangesSink& ChangesSink::setFilter(const Filter& filter) {
	this->filter = filter;
	filterArgs.revert();
	return *this;
}

ChangesSink& ChangesSink::unsetFilter() {
	this->filter = null;
	filterArgs.revert();
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
	return setFilter(Filter(String(),flags));
}

void ChangesSink::cancelWait() {
	initCancelFunction();
	canceled = true;
	cancelFunction();
}

void ChangesSink::initCancelFunction() {
	if (!cancelFunction) {
		Synchronized<MicroLock> _(cancelFnInitLock);
		if (!cancelFunction)
			cancelFunction = NetworkConnection::createCancelFunction();
	}
}

}
