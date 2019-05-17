/*
 * objcache.cpp
 *
 *  Created on: 10. 5. 2019
 *      Author: ondra
 */




#include "doccache.h"

#include <imtjson/fnv.h>

namespace couchit {

class DocCache::Update: public IChangeEventObserver {
public:

	virtual ~Update() {
		owner.unreg();
	}

	explicit Update(DocCache &owner):owner(owner) {}

	virtual bool onEvent(const ChangeEvent &doc) {
		owner.update(doc);
		return true;
	}

	virtual json::Value getLastKnownSeqID() const {
		return Value();
	}


protected:
	DocCache &owner;
};

DocCache::DocCache(CouchDB& db, ChangesDistributor* observer,
		Config config)
:db(db), chdist(observer), config(std::move(config))
{
	regid = chdist->add(std::unique_ptr<IChangeEventObserver>(new Update(*this)));
}

DocCache::~DocCache() {
	if (chdist) chdist->remove(regid);
	lock.lock();
	lock.unlock();
}

Value DocCache::get(StrViewA name) {
	Sync _(lock);
	auto f = dataMap.find(name);
	if (f == dataMap.end()) {
		_.unlock();
		Value doc = db.get(name, CouchDB::flgNullIfMissing|
				(config.conflicts?CouchDB::flgConflicts:0)|
				(config.revisions?CouchDB::flgRevisions:0));

		if (doc == nullptr) {
			if (config.missing) put_missing(name);
			return nullptr;
		} else {
			put(doc);
			return doc;
		}
	} else {
		Item &itm = f->second;
		if (!itm.accessed && itm.lru < 4) {
			itm.accessed = true;
			itm.lru++;
		}
		return itm.data;
	}
}

void DocCache::update(const ChangeEvent &ev) {
	Sync _(lock);
	auto f = dataMap.find(ev.id);
	if (f != dataMap.end()) {
		if (ev.deleted) {
			if (config.missing) {
				f->second.data = nullptr;
			} else {
				dataMap.erase(f);
			}
		} else {
			if (ev.doc.defined()) {
				f->second.data = ev.doc;
			} else {
				dataMap.erase(f);
			}
		}
	}
}

void DocCache::put(Value doc) {
	Sync _(lock);
	String id = doc["_id"].toString();
	if (doc["_deleted"].getBool()) {
		if (config.missing) put_missing(id);
		else erase(id);
	} else {
		auto f = dataMap.find(id);
		if (f == dataMap.end()) {
			if (config.limit && config.limit<=dataMap.size()) {
				rungc();
			}
			dataMap[id].data = doc;
		} else {
			f->second.data = doc;
		}
	}
}

void DocCache::put_missing(String id) {
	Sync _(lock);
	auto f = dataMap.find(id);
	if (f == dataMap.end()) {
		if (config.limit && config.limit<=dataMap.size()) {
			rungc();
		}
		dataMap[id].data = nullptr;
	} else {
		f->second.data = nullptr;
	}
}

void DocCache::erase(String id) {
	Sync _(lock);
	dataMap.erase(id);
}

void DocCache::unreg() {
	chdist=nullptr;
}

void DocCache::rungc() {
	Sync _(lock);

	std::vector<StrViewA> delKeys;
	for (auto &&x: dataMap) {
		if (x.second.lru == 0) delKeys.push_back(x.first);
		else {
			x.second.accessed = false;
			x.second.lru--;
		}
	}
	for (auto &&x: delKeys) {
		dataMap.erase(x);
	}
}

std::size_t DocCache::Hash::operator ()(StrViewA data) const {
	std::size_t val = 0;
	FNV1a<sizeof(std::size_t)> fnv(val);
	for (auto &&k: data) fnv(k);
	return val;
}


}

