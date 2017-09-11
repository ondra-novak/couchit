/*
 * changeDoc.cpp
 *
 *  Created on: 21. 3. 2016
 *      Author: ondra
 */

#include "changes.h"

#include "couchDB.h"
namespace couchit {





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

ChangesFeed::ChangesFeed(CouchDB& couchdb)
	:couchdb(couchdb), outlimit(((std::size_t)-1)),timeout(0),canceled(false)
{
}

ChangesFeed& ChangesFeed::fromSeq(const Value &seqNumber) {
	this->seqNumber = seqNumber;
	return *this;
}

ChangesFeed& ChangesFeed::setTimeout(std::size_t timeout) {
	this->timeout = timeout;
	return *this;
}

ChangesFeed& ChangesFeed::setFilter(const Filter& filter) {
	this->filter = std::unique_ptr<Filter>(new Filter(filter));
	filterArgs.revert();
	return *this;
}

ChangesFeed& ChangesFeed::unsetFilter() {
	this->filter = nullptr;
	filterArgs.revert();
	return *this;
}


ChangesFeed& ChangesFeed::limit(std::size_t count) {
	this->outlimit = count;
	return *this;
}

Changes ChangesFeed::exec() {
	return couchdb.receiveChanges(*this);
}

void  ChangesFeed::continuous(ChangesFeedHandler &handler) {
	return couchdb.receiveChangesContinuous(*this,handler);
}

ChangesFeed& ChangesFeed::setFilterFlags(std::size_t flags) {
	return setFilter(Filter(String(),flags));
}

void ChangesFeed::cancelWait() {
	std::lock_guard<std::mutex> _(initLock);
	canceled = true;
	if (curConn != nullptr) {
		curConn->http.abort();
	}
}


ChangesFeed::ChangesFeed(ChangesFeed&& other)
	:couchdb(other.couchdb)
	,seqNumber(std::move(other.seqNumber))
	,outlimit(std::move(other.outlimit))
	,timeout(std::move(other.timeout))
	,filter(std::move(other.filter))
	,filterArgs(std::move(other.filterArgs))
	,canceled(false)


{

}

void ChangesFeed::cancelEpilog() {
	std::lock_guard<std::mutex> _(initLock);
	curConn = nullptr;
	canceled = false;
	wasCanceledState = true;
}

void ChangesFeed::finishEpilog() {
	std::lock_guard<std::mutex> _(initLock);
	curConn = nullptr;
	wasCanceledState = false;
}


void ChangesFeed::errorEpilog() {
	std::lock_guard<std::mutex> _(initLock);
	if (curConn != nullptr) {
		curConn->http.abort();
		curConn = nullptr;
	}
	if (canceled) {
		canceled = false;
		wasCanceledState = true;
		return;
	}
	throw;
}

}

