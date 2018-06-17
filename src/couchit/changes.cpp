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
	:couchdb(couchdb), outlimit(((std::size_t)-1)),timeout((std::size_t)-1),iotimeout(120000),canceled(false)
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
	,iotimeout(std::move(other.iotimeout))
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

static void stdDeleteObserver(IChangeObserver *obs) {
	delete obs;
}

static void stdLeaveObserver(IChangeObserver *) {

}


void ChangesDistributor::add(IChangeObserver* observer, bool ownership) {
	if (ownership) {
		static std::function<void(IChangeObserver *)> obsDel (&stdDeleteObserver);
		add(Observer(observer, obsDel));
	} else {
		static std::function<void(IChangeObserver *)> obsLeave (&stdLeaveObserver);
		add(Observer(observer, obsLeave));
	}
}

void ChangesDistributor::add(IChangeObserver& observer) {
	add(&observer, false);
}

void ChangesDistributor::add(IChangeObserver* observer, const Deleter & deleter) {
	add(Observer(observer,deleter));
}

IChangeObserver* ChangesDistributor::add(Observer&& observer) {
	std::unique_lock<std::mutex> _(initLock);
	IChangeObserver* ret = observer.get();
	observers.push_back(std::move(observer));
	return ret;
}

void ChangesDistributor::remove(IChangeObserver* observer) {
	std::unique_lock<std::mutex> _(initLock);
	auto e = observers.end();
	for (auto b = observers.begin(); b != e; ++b) {
		if (b->get() == observer) {
			observers.erase(b);
			break;
		}
	}

}

void ChangesDistributor::remove(IChangeObserver& observer) {
	remove(&observer);
}

Value ChangesDistributor::getInitialUpdateSeq() const {
	Value z;

	std::unique_lock<std::mutex> _(initLock);
	for (auto &&x: observers) {
		Value a = x->getLastKnownSeqID();
		if (a.defined()) {
			if (a.isNull()) return "0";
			if (a.defined()) {
				SeqNumber seq_cur(z);
				SeqNumber seq_now(a);
				if (seq_cur < seq_now) {
					z = a;
				}
			}
		}
	}
	if (z.defined()) return z; else return "now";
}

void ChangesDistributor::run() {

	Value u = getInitialUpdateSeq();
	if (u.defined()) this->since(u);

	this->operator >>([&](const ChangedDoc &doc) {
		std::unique_lock<std::mutex> _(initLock);
		for (auto &&x : observers) x->onChange(doc);
		return true;
	});
}

bool ChangesDistributor::sync(Value seqNum, unsigned int timeoutms) {

	class LocalObserver: public IChangeObserver {
	public:
		SeqNumber seqNum;
		std::condition_variable &condvar;
		LocalObserver(const SeqNumber &seqNum, std::condition_variable &condvar)
			:seqNum(seqNum),condvar(condvar) {}

		virtual void onChange(const ChangedDoc &doc) {
			seqNum = doc.seqId;
			condvar.notify_all();
		}
		virtual Value getLastKnownSeqID() const {
			return Value();
		}
	};
	SeqNumber curSeq, reqSeqNum(seqNum);
	{
		std::unique_lock<std::mutex> _(initLock);
		curSeq = this->seqNumber;
	}
	std::condition_variable condvar;
	LocalObserver obs(curSeq, condvar);
	add(&obs,false);
	{
		std::unique_lock<std::mutex> lock(initLock);
		auto predicate = [&]{
			return reqSeqNum<=obs.seqNum;
		};
		if (timeoutms == (unsigned int)-1) {
			condvar.wait(lock,  predicate);
		} else {
			if (!condvar.wait_for(lock, std::chrono::milliseconds(timeoutms),predicate)) return false;
		}
	}
	remove(&obs);
	return true;
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
