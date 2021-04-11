/*
 * objcache.cpp
 *
 *  Created on: 10. 5. 2019
 *      Author: ondra
 */




#include "doccache.h"

#include <imtjson/fnv.h>
#include "shared/logOutput.h"

using ondra_shared::logInfo;

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
	gc_queue.resize(config.limit);
	regid = chdist->add(std::unique_ptr<IChangeEventObserver>(new Update(*this)));
}

DocCache::DocCache(CouchDB& db, Config config):db(db), chdist(nullptr), config(std::move(config)) {
		gc_queue.resize(config.limit);
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
		itm.accessed = true;
		return itm.data;
	}
}

void DocCache::update(const ChangeEvent &ev) {
	if (ev.idle) return;
	Sync _(lock);
	auto f = dataMap.find(ev.id);
	if (config.precache || f != dataMap.end()) {
		put_lk(ev.doc, f);
	}
}

void DocCache::put_lk(Value doc, DataMap::iterator &f) {
	String id = doc["_id"].toString();
	bool deleted = doc["_deleted"].getBool();
	Value tostore = deleted?Value(nullptr):doc;
	Revision rev(doc["_rev"]);
	if (f == dataMap.end()) {
		allocSlot(id);
		dataMap[id].data = tostore;
		dataMap[id].rev = rev;
	} else {
		if (rev > f->second.rev) {
			f->second.data = tostore;
			f->second.rev = rev;
		} else {
			ondra_shared::logWarning("Old version cannot rewrite new");
		}
	}
}

void DocCache::put(Value doc) {
	Sync _(lock);
	auto f = dataMap.find(doc["_id"].getString());
	put_lk(doc, f);
}

void DocCache::put_missing(String id) {
	Sync _(lock);
	auto f = dataMap.find(id);
	if (f == dataMap.end()) {
		dataMap[id].data = nullptr;
		allocSlot(id);
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


std::size_t DocCache::Hash::operator ()(StrViewA data) const {
	std::size_t val = 0;
	FNV1a<sizeof(std::size_t)> fnv(val);
	for (auto &&k: data) fnv(k);
	return val;
}

std::size_t DocCache::allocSlot(const String new_id) {
	if (gc_queue.empty()) return 0;
	while (true) {
		auto pos = gc_queue_index;
		auto id = gc_queue[gc_queue_index];
		gc_queue_index = (gc_queue_index+1) % gc_queue.size();
		auto iter = dataMap.find(id);
		if (iter == dataMap.end()) {
			gc_queue[pos] = new_id;
			return pos;
		} else if (iter->second.accessed) {
			iter->second.accessed = false;
		} else {
			dataMap.erase(iter);
			gc_queue[pos] = new_id;
			return pos;
		}
	}
}

}

