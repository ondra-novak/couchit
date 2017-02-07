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

Value Changes::getNext() {
	return  rows[pos++];
}

Value Changes::peek() const {
	return  rows[pos];

}

bool Changes::hasItems() const {
	return pos < sz;
}

void Changes::rewind() {
	pos = 0;
}

ChangesSink::ChangesSink(CouchDB& couchdb)
	:couchdb(couchdb), outlimit(((std::size_t)-1)),timeout(0),canceled(false)
{
}

ChangesSink& ChangesSink::fromSeq(const Value &seqNumber) {
	this->seqNumber = seqNumber;
	return *this;
}

ChangesSink& ChangesSink::setTimeout(std::size_t timeout) {
	this->timeout = timeout;
	return *this;
}

ChangesSink& ChangesSink::setFilter(const Filter& filter) {
	this->filter = std::unique_ptr<Filter>(new Filter(filter));
	filterArgs.revert();
	return *this;
}

ChangesSink& ChangesSink::unsetFilter() {
	this->filter = nullptr;
	filterArgs.revert();
	return *this;
}


ChangesSink& ChangesSink::limit(std::size_t count) {
	this->outlimit = count;
	return *this;
}

Changes ChangesSink::exec() {
	return couchdb.receiveChanges(*this);
}

ChangesSink& ChangesSink::setFilterFlags(std::size_t flags) {
	return setFilter(Filter(String(),flags));
}

void ChangesSink::cancelWait() {
	initCancelFunction();
	canceled = true;
	cancelFunction();
}

void ChangesSink::initCancelFunction() {
	if (!cancelFunction) {
		std::lock_guard<std::mutex> _(cancelFnInitLock);
		if (!cancelFunction)
			cancelFunction = NetworkConnection::createCancelFunction();
	}
}

ChangesSink::ChangesSink(ChangesSink&& other)
	:couchdb(other.couchdb)
	,seqNumber(std::move(other.seqNumber))
	,outlimit(std::move(other.outlimit))
	,timeout(std::move(other.timeout))
	,filter(std::move(other.filter))
	,filterArgs(std::move(other.filterArgs))
	,cancelFunction(std::move(other.cancelFunction))
	,canceled(false)


{

}


}

