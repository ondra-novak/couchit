/*
 * changeDoc.cpp
 *
 *  Created on: 21. 3. 2016
 *      Author: ondra
 */

#include <thread>
#include "changes.h"

#include "../../../shared/logOutput.h"
#include "couchDB.h"

using ondra_shared::logError;
namespace couchit {





ChangeEvent::ChangeEvent(const Value& allData)
:Value(allData)
,seqId(allData["seq"])
,id(allData["id"].getString())
,revisions(allData["changes"])
,deleted(allData["deleted"].getBool())
,idle(false)
,doc(allData["doc"])
{
}


ChangeEvent::ChangeEvent(_IdleEvent, const json::Value seqId)
:Value(nullptr)
,seqId(seqId)
,deleted(false)
,idle(true){
}



Changes::Changes(Value jsonResult)
:Value(jsonResult),pos(0),sz(jsonResult.size())
{

}

Changes::Changes()
:Value(),pos(0),sz(0)
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

ChangesFeed& ChangesFeed::restartAfter(std::size_t ms) {
	restart_after = ms;
	return *this;
}

ChangesFeed& ChangesFeed::setFilterFlags(std::size_t flags) {
	return setFilter(Filter(String(),flags));
}

void ChangesFeed::State::cancelWait() {
	std::lock_guard<std::recursive_mutex> _(initLock);
	canceled = true;
	if (curConn != nullptr) {
		curConn->http.abort();
	}
}


void ChangesFeed::cancelWait() {
	state.cancelWait();
}

void ChangesFeed::State::cancelEpilog() {
	std::lock_guard<std::recursive_mutex> _(initLock);
	curConn = nullptr;
	canceled = false;
	wasCanceledState = true;
}

void ChangesFeed::State::finishEpilog() {
	std::lock_guard<std::recursive_mutex> _(initLock);
	curConn = nullptr;
	wasCanceledState = false;
}


void ChangesFeed::State::errorEpilog() {
	std::lock_guard<std::recursive_mutex> _(initLock);
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

static void stdDeleteObserver(IChangeEventObserver *obs) {
	delete obs;
}

static void stdLeaveObserver(IChangeEventObserver *) {

}

ChangesDistributor::RegistrationID ChangesDistributor::add(IChangeEventObserver &observer) {
	return add(PObserver(&observer, &stdLeaveObserver), {});
}
ChangesDistributor::RegistrationID ChangesDistributor::add(IChangeEventObserver &observer, const FilterDef &flt) {
	return add(PObserver(&observer, &stdLeaveObserver), flt);
}
ChangesDistributor::RegistrationID ChangesDistributor::add(PObserver &&observer) {
	return add(std::move(observer),{});
}
ChangesDistributor::RegistrationID ChangesDistributor::add(PObserver &&observer, const FilterDef &flt) {
	if (observer == nullptr) return nullptr;

	RegistrationID id;

	auto since = observer->getLastKnownSeqID();
	if (since.defined()) {
		if (since == nullptr) since = "0";
		CouchDB::ChangeFeedState feedState;
		feedState.timeout = 0;
		feedState.include_docs = true;
		feedState.since = since;
		feedState.conflicts = true;
		feedState.filter = flt.filterType;
		feedState.filter_spec = flt.filterParams;
		Changes chg = db.receiveChanges(feedState);
		while (!chg.empty()) {
			//read next changes before processing these onces
			//because processing can generate additional changes
			Changes ch2 = db.receiveChanges(feedState);
			for (ChangeEvent ev: chg) {
				if (!observer->onEvent(ev)) {
					return nullptr;
				}
			}
			chg = ch2;

		}
		std::unique_lock<std::recursive_mutex> _(lock);
		chg = db.receiveChanges(feedState);
		while (!chg.empty()) {
			Changes ch2 = db.receiveChanges(feedState);
			for (ChangeEvent ev: chg) {
				if (!observer->onEvent(ev)) {
					return nullptr;
				}
				filterOut[json::Value({ev.id, ev.revisions})].push_back(observer.get());
			}
			chg = ch2;
		}

		id = observer.get();
		observers.push_back(std::move(observer));
	} else {
		std::unique_lock<std::recursive_mutex> _(lock);
		id = observer.get();
		observers.push_back(std::move(observer));
	}

	return id;
}
ChangesDistributor::RegistrationID ChangesDistributor::add(std::unique_ptr<IChangeEventObserver> &&observer) {
	return add(PObserver(observer.release(), &stdDeleteObserver), {});
}
ChangesDistributor::RegistrationID ChangesDistributor::add(std::unique_ptr<IChangeEventObserver> &&observer, const FilterDef &flt) {
	return add(PObserver(observer.release(), &stdDeleteObserver), flt);
}


void ChangesDistributor::remove(RegistrationID regid) {
	std::unique_lock<std::recursive_mutex> _(lock);
	auto iter = std::remove_if(observers.begin(), observers.end(), [&](const auto &b) {
		return b.get() == regid;
	});
	observers.erase(iter, observers.end());

}



void ChangesDistributor::broadcast(const ChangeEvent &doc) {

	std::vector<RegistrationID> toRemove;
	std::unique_lock<std::recursive_mutex> _(lock);


	if (filterOut.empty()) {

		for (auto &&x : observers) {
			bool r =x->onEvent(doc);
			if (!r) {
				RegistrationID reg = x.get();
				toRemove.push_back(reg);
			}
		}
	} else {
		std::vector<const IChangeEventObserver *> empty, *flt;
		auto iter = filterOut.find(json::Value({doc.id, doc.revisions}));
		if (iter != filterOut.end()) flt = &iter->second;
		else flt = &empty;
		for (auto &&x : observers) {
			if (std::find(flt->begin(), flt->end(), x.get()) == flt->end()) {
				bool r =x->onEvent(doc);
				if (!r) {
					RegistrationID reg = x.get();
					toRemove.push_back(reg);
				}
			}
		}
	}
	for (auto &&x: toRemove) {
		remove(x);
	}
}

void ChangesDistributor::run() {

	while (!feedState.canceled.load()) {
		try {
			Changes chg(db.receiveChanges(feedState));
			std::lock_guard _(lock);
			if (feedState.canceled.load()) {
				break;
			}else if (chg.empty()) {
				if (enable_idle) {
					ChangeEvent ev(ChangeEvent::_IdleEvent::idleEvent, feedState.since);
					try {
						broadcast(ev);
					} catch (...) {
						onException();
					}
				}
			} else {
				while (chg.hasItems()) {
					try {
						broadcast(chg.getNext());
					} catch (...) {
						onException();
					}
				}
			}
			filterOut.clear();
		} catch (...) {
			onException();
			std::this_thread::sleep_for(std::chrono::seconds(1));
		}
	}
}

void ChangesDistributor::onException() {
	try {
		throw;
	} catch (std::exception &e) {
		logError("Couchit change distributor - Exception: $1", e.what());
	}
}

void ChangesDistributor::sync() {
	//does nothing, because sync is called during connection
}


void ChangesDistributor::runService() {
	thr = std::unique_ptr<std::thread> (
		new std::thread([=]{
			run();
	}));
}


ChangesDistributor::~ChangesDistributor() {
	stopService();
}


void ChangesDistributor::stopService() {
	exit = true;
	if (thr != nullptr) {
		db.abortReceiveChanges(feedState);
		thr->join();
		thr = nullptr;
	}
}

ChangesDistributor::ChangesDistributor(CouchDB &db, CouchDB::ChangeFeedState &&feedState, bool enable_idle)
	:db(db),feedState(std::move(feedState)), enable_idle(enable_idle)
{

}

ChangesDistributor::ChangesDistributor(CouchDB &db, bool include_docs, bool enable_idle)
	:db(db), enable_idle(enable_idle)
{
	feedState.limit = 1;
	feedState.descending = true;
	feedState.timeout = 0;
	feedState.conflicts = true;
	db.receiveChanges(feedState);
	feedState.limit = ~0;
	feedState.descending = false;
	feedState.include_docs = include_docs;
	feedState.timeout = 50000;

}

CouchDB::ChangeFeedState::ChangeFeedState(ChangeFeedState && other)
:since(std::move(other.since))
,filter_spec(std::move(other.filter_spec))
,extra_args(std::move(other.extra_args))
,filter(std::move(other.filter))
,include_docs (std::move(other.include_docs ))
,conflicts (std::move(other.conflicts ))
,attachments(std::move(other.attachments))
,descending (std::move(other.descending ))
,timeout (std::move(other.timeout ))
,limit (std::move(other.limit  ))
,batching(other.batching)
,poll_interval(other.poll_interval)
,connection (std::move(other.connection ))
,canceled(false)
,last_request_time(other.last_request_time)
{}

CouchDB::ChangeFeedState::ChangeFeedState(const ChangeFeedState & other)
:since(other.since)
,filter_spec(other.filter_spec)
,extra_args(other.extra_args)
,filter(other.filter)
,include_docs (other.include_docs )
,conflicts (other.conflicts )
,attachments(other.attachments)
,descending (other.descending )
,timeout (other.timeout )
,limit (other.limit  )
,batching(other.batching)
,poll_interval(other.poll_interval)
,connection (nullptr )
,canceled(false)
,last_request_time(std::chrono::system_clock::from_time_t(0))
{}
CouchDB::ChangeFeedState::ChangeFeedState()
:last_request_time(std::chrono::system_clock::from_time_t(0))
{}

}
