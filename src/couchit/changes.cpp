/*
 * changeDoc.cpp
 *
 *  Created on: 21. 3. 2016
 *      Author: ondra
 */

#include <thread>
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
:Value(jsonResult),pos(0),sz(jsonResult.size())
{

}

Value Changes::getNext() {
	return (*this)[pos++];
}

Value Changes::peek() const {
	return (*this)[pos];

}

bool Changes::hasItems() const {
	return pos < sz;
}

void Changes::rewind() {
	pos = 0;
}

ChangesFeed::ChangesFeed(CouchDB& couchdb)
	:couchdb(couchdb)
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
	this->filter = filter;
	filterInUse = true;
	filterArgs.revert();
	return *this;
}

ChangesFeed& ChangesFeed::unsetFilter() {
	filterInUse = false;
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

void ChangesFeed::State::cancelWait() {
	std::lock_guard<std::mutex> _(initLock);
	canceled = true;
	if (curConn != nullptr) {
		curConn->http.abort();
	}
}


void ChangesFeed::cancelWait() {
	state.cancelWait();
}

void ChangesFeed::State::cancelEpilog() {
	std::lock_guard<std::mutex> _(initLock);
	curConn = nullptr;
	canceled = false;
	wasCanceledState = true;
}

void ChangesFeed::State::finishEpilog() {
	std::lock_guard<std::mutex> _(initLock);
	curConn = nullptr;
	wasCanceledState = false;
}


void ChangesFeed::State::errorEpilog() {
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

static void stdDeleteObserver(IChangeObserver *obs) {
	delete obs;
}

static void stdLeaveObserver(IChangeObserver *) {

}

ChangesDistributor::RegistrationID ChangesDistributor::add(IChangeObserver &observer) {
	return add(PObserver(&observer, &stdLeaveObserver));
}
ChangesDistributor::RegistrationID ChangesDistributor::add(PObserver &&observer) {
	std::unique_lock<std::mutex> _(state.initLock);
	RegistrationID id = observer.get();
	observers.push_back(std::move(observer));
	return id;
}
ChangesDistributor::RegistrationID ChangesDistributor::add(std::unique_ptr<IChangeObserver> &&observer) {
	return add(PObserver(observer.release(), &stdDeleteObserver));
}


void ChangesDistributor::remove(RegistrationID regid) {
	std::unique_lock<std::mutex> _(state.initLock);
	auto e = observers.end();
	for (auto b = observers.begin(); b != e; ++b) {
		if (b->get() == regid) {
			observers.erase(b);
			break;
		}
	}

}


Value ChangesDistributor::getInitialUpdateSeq() const {
	Value z;

	std::unique_lock<std::mutex> _(state.initLock);
	for (auto &&x: observers) {
		Value a = x->getLastKnownSeqID();
		if (a.defined()) {
			if (a.isNull()) return "0";
			if (z.defined()) {
				SeqNumber seq_cur(z);
				SeqNumber seq_now(a);
				if (seq_cur < seq_now) {
					z = a;
				}
			} else {
				z = a;
			}
		}
	}
	if (z.defined()) return z; else return "now";
}

void ChangesDistributor::run() {

	Value u = getInitialUpdateSeq();
	if (u.defined()) this->since(u);

	this->operator >>([&](const ChangedDoc &doc) {
		std::unique_lock<std::mutex> _(state.initLock);
		for (auto &&x : observers) x->onChange(doc);
		return true;
	});
}

void ChangesDistributor::sync() {


	Value s = getInitialUpdateSeq();
	ChangesFeed chf(*this);
	chf.since(s);
	chf.setTimeout(0);
	chf >> [&](const ChangedDoc &doc) {
		std::unique_lock<std::mutex> _(state.initLock);
		for (auto &&x : observers) x->onChange(doc);
		return true;
	};
	since(chf.getLastSeq());
}


void ChangesDistributor::runService() {
	stopService();
	thr = std::unique_ptr<std::thread> (
		new std::thread([=]{
			setTimeout(-1);
			run();
	}));
}

void ChangesDistributor::runService(std::function<bool()> onError) {
	stopService();
	thr = std::unique_ptr<std::thread> (
			new std::thread([=]{
				setTimeout(-1);
				bool goon = true;
				while (goon) {
					try {
						run();
						goon = false;
					} catch (...) {
						goon = onError();
					}
				}
	}));

}

ChangesDistributor::~ChangesDistributor() {
	stopService();
}

ChangesDistributor::ChangesDistributor(ChangesFeed &&feed):ChangesFeed(std::move(feed)) {}


void ChangesDistributor::stopService() {
	if (thr != nullptr) {
		cancelWait();
		thr->join();
		thr = nullptr;
	}
}


}
