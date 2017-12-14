#include "memview.h"

#include "iterrange.h"


namespace couchit {

SeqNumber MemView::load(const Query& q) {

	USync _(updateLock);

	clear();
	Result res = q.exec();

	for (Row rw : res) {
		addDoc(rw.doc,rw.key, rw.value);
	}

	return updateSeq = res.getUpdateSeq();

}

SeqNumber MemView::load(CouchDB& db, const View& view) {
	return load(db.createQuery(view));
}

bool MemView::haveDoc(const String &docId) const {
	SharedSync _(lock);
	return docToKeyMap.find(docId) != docToKeyMap.end();
}

void MemView::eraseDoc(const String& docId) {
	if (haveDoc(docId)) {
		Sync _(lock);
		eraseDocLk(docId);
	}
}
void MemView::eraseDocLk(const String& docId) {
	IterRange<DocToKey::const_iterator> rng ( docToKeyMap.equal_range(docId));
	for (auto &&itm : rng) {
		keyToValueMap.erase(KeyAndDocId(itm.second, itm.first));
	}
	docToKeyMap.erase(docId);
}

void MemView::addDoc(const Value& doc, const Value& key, const Value& value) {
	Sync _(lock);
	addDocLk(doc,key, value);
}

Value MemView::getDocument(const String& docId) const {
	SharedSync _(lock);
	auto it = docToKeyMap.find(docId);
	if (it == docToKeyMap.end()) return Value();
	auto it2 = keyToValueMap.find(KeyAndDocId(it->second, docId));
	if (it2 == keyToValueMap.end()) return Value();
	return it2->second.doc;
}

Query MemView::createQuery(std::size_t viewFlags) const {
	View v(String(),viewFlags);
	return Query(v, queryable);
}

Query MemView::createQuery(std::size_t viewFlags, View::Postprocessing fn, CouchDB* db) const {
	View v(String(),viewFlags,fn);
	return Query(v, queryable);
}

void MemView::clear() {
	Sync _(lock);
	docToKeyMap.clear();
	keyToValueMap.clear();
	updateSeq = SeqNumber(0);
}


bool MemView::empty() const {
	SharedSync _(lock);
	return keyToValueMap.empty();
}


MemView::Queryable::Queryable(const MemView& mview):mview(mview) {
}

Value MemView::Queryable::executeQuery(const QueryRequest& r) {

	return mview.runQuery(r);

}

int MemView::KeyAndDocId::compare(const KeyAndDocId& other) const {
	int c = compareJson(key,other.key);
	if (c == 0) {
		return compareStringsUnicode(docId,other.docId);
	} else {
		return c;
	}
}

Value MemView::runQuery(const QueryRequest& r) const {

	SharedSync _(lock);

	Value out;
	switch (r.mode) {
	case qmAllItems:
		out = getAllItems();
		break;
	case qmKeyList:
		out = getItemsByKeys(r.keys);
		break;
	case qmStringPrefix: {
		Value k = r.keys[0];
		Value from = k;
		Value to;
		if (k.type() == json::array) {
			Array hlp(k);
			String tail ({hlp[hlp.size()-1].toString(), Query::maxString});
			hlp.erase(hlp.size()-1);
			hlp.push_back(tail);
			to = hlp;
		}
		out = getItemsByRange(from,to,false,false);
		} break;
	case qmKeyRange:
		out = getItemsByRange(r.keys[0],r.keys[1],r.exclude_end,r.docIdFromGetKey);
		break;
	case qmKeyPrefix: {
		Array from (r.keys[0]);
		Array to (r.keys[0]);
		to.push_back(Query::maxKey);
		out = getItemsByRange(from,to,false,false);
		} break;
	}

	if (r.reversedOrder) {
		out = out.reverse();
	}

	if (viewDef.listFn != nullptr) out = viewDef.listFn(out);


	return Object("rows",out)("total", keyToValueMap.size());

}

template<typename Iter>
static Value resultToValue(const Iter &itm) {
	return Object("id",itm.first.docId)
					("key",itm.first.key)
					("value",itm.second.value)
					("doc",itm.second.doc);
}

Value MemView::getAllItems() const {
	Array out;
	out.reserve(keyToValueMap.size());

	for (auto &&itm : keyToValueMap) {
		out.push_back(resultToValue(itm));
	}
	return out;
}

Value MemView::getItemsByKeys(const json::Array& keys) const {
	Array out;
	for (Value k : keys) {
		IterRange<KeyToValue::const_iterator> r(
			keyToValueMap.lower_bound(KeyAndDocId(k,String())),
			keyToValueMap.upper_bound(KeyAndDocId(k,Query::maxString)));
		for (auto &&itm : r) {
			out.push_back(resultToValue(itm));
		}
	}
	return out;
}

void MemView::mapDoc(const Value& document, const EmitFn& emitFn) {
	viewDef.mapFn(document,emitFn);
}

void MemView::defaultMapFn(const Value& document, const EmitFn& emitFn) {
	emitFn(document["_id"],document["_rev"]);
}

void MemView::onChange(const ChangedDoc& doc) {
	USync _(updateLock);
	{
		SharedSync _(lock);
		String docId(doc["id"]);
		auto iter  = docToKeyMap.find(docId);
		if (iter != docToKeyMap.end()) {
			_.unlock();
			eraseDoc(docId);
		} else {
			_.unlock();
		}
		if (!doc.deleted) {
			Value d = doc.doc;
			addDoc(d);
		}

	}
	updateSeq = doc.seqId;
	onUpdate(updateSeq);
}

void MemView::addDoc(const Value& doc) {
	if (doc["_id"].type() == json::string) {

		class Emit: public EmitFn {
		public:
			Emit(MemView &owner, const Value &doc, bool &mapped)
				:sync(owner.lock, std::defer_lock), mapped(mapped), owner(owner),doc(doc){}
			virtual void operator()(const Value &key, const Value &value) const {
				if (!mapped) {
					mapped = true;
					sync.lock();
					owner.eraseDocLk(String(doc["_id"]));
				}
				owner.addDocLk(doc, key, value);
			}

		protected:
			mutable Sync sync;
			MemView &owner;
			const Value &doc;
			bool &mapped;
		};

		bool mapped = false;
		mapDoc(doc, Emit(*this, doc, mapped));
		if (!mapped) {
			String id(doc["_id"]);
			Value e = getDocument(id);
			if (e.defined()) {
				eraseDoc(id);
			}
		}
		//todo erase doc if no addition
	}
}

void MemView::addDocLk(const Value& doc, const Value& key, const Value& value) {
	String id (doc["_id"]);
	Value idocs = flags & flgIncludeDocs? doc : Value(nullptr);

	keyToValueMap.insert(std::make_pair(KeyAndDocId(key,id),ValueAndDoc(value,idocs)));
	docToKeyMap.insert(std::make_pair(String(id),key));
}

void MemView::update(CouchDB& db) {
	USync _(updateLock);
	updateLk(db);
}

void MemView::setCheckpointFile(const PCheckpoint& checkpointFile, std::size_t saveInterval) {
	chkpStore = checkpointFile;
	clear();
	Result res = chkpStore->load();

	USync _(updateLock);
	Sync __(lock);
	updateSeq = res.getUpdateSeq();

	for (Row r : res) {
		addDocLk(r.doc, r.key, r.value);
	}
	chkpInterval = saveInterval;
	chkpNextUpdate = SeqNumber(updateSeq).getRevId() + chkpInterval;

}

void MemView::onUpdate(const Value &seqNum) {
	if (chkpStore == nullptr) return;
	std::size_t curUpdate = SeqNumber(seqNum).getRevId();
	if (curUpdate > chkpNextUpdate) {
		Value out;
		{
			SharedSync _(lock);
			out = getAllItems();
		}
		Result r(out,0,0,seqNum);
		chkpStore->store(r);
		chkpNextUpdate = curUpdate+chkpInterval;
	}
}

void MemView::updateLk(CouchDB& db) {
	ChangesFeed feed = db.createChangesFeed();
	feed.includeDocs(true).since(updateSeq);
	Changes chs = feed.exec();
	for (ChangedDoc cdoc : chs) {
		eraseDoc(String(cdoc["id"]));
		addDoc(cdoc.doc);
	}
	updateSeq = feed.getLastSeq();
	onUpdate(updateSeq);
}

bool MemView::updateIfNeeded(CouchDB &db, bool wait) {
	USync _(updateLock, std::defer_lock);
	if (wait) {
		_.lock();
	} else {
		if (!_.try_lock()) return false;
	}
	SeqNumber lastKnown = db.getLastKnownSeqNumber();
	if (SeqNumber(updateSeq) < lastKnown || lastKnown.isOld()) {
		updateLk(db);
	}
	return true;
}

Value MemView::getItemsByRange(const json::Value& from, const json::Value& to, bool exclude_end, bool extractDocIDs) const {

	Array out;

	String minDoc = extractDocIDs?String(from.getKey()):String();
	String maxDoc = extractDocIDs?String(to.getKey()):exclude_end?String():Query::maxString;

	IterRange<KeyToValue::const_iterator> r(
		keyToValueMap.lower_bound(KeyAndDocId(from,minDoc)),
		exclude_end?keyToValueMap.lower_bound(KeyAndDocId(to,maxDoc))
				   :keyToValueMap.upper_bound(KeyAndDocId(to,maxDoc)));


	out.reserve(std::distance(r.begin(),r.end()));
	for (auto &&itm: r) {
		out.push_back(resultToValue(itm));
	}

	return out;
}

}
