#include <unordered_set>
#include "memview.h"

#include "iterrange.h"


namespace couchit {


MemView::RRow MemView::makeRow(String id, Value key, Value value, Value doc) {
	Object out;
	out.set("id",id)
			("key",key)
			("value",value)
			("doc",doc);
	return RRow(out);

}

SeqNumber MemView::load(const Query& q) {

	USync _(updateLock);

	clear();
	Result res = q.exec();

	for (Row rw : res) {
		addDoc(String(rw.id), rw.doc,rw.key, rw.value);
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
		fireKeyChangeEvent(itm.second);
	}
	docToKeyMap.erase(docId);
}

void MemView::addDoc(const String &id, const Value& doc, const Value& key, const Value& value) {
	Sync _(lock);
	addDocLk(id,doc,key, value);
}

Value MemView::getDocument(const String& docId) const {
	SharedSync _(lock);
	auto it = docToKeyMap.find(docId);
	if (it == docToKeyMap.end()) return Value();
	auto it2 = keyToValueMap.find(KeyAndDocId(it->second, docId));
	if (it2 == keyToValueMap.end()) return Value();
	return it2->second["doc"];
}

Query MemView::createQuery(std::size_t viewFlags) const {
	View v(String(),viewFlags);
	return Query(v, queryable);
}

Query MemView::createDocQuery(std::size_t viewFlags) const {
	View v(String(),viewFlags);
	return Query(v, queryableDocs);
}

Query MemView::createQuery(std::size_t viewFlags, View::Postprocessing fn, CouchDB* ) const {
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
MemView::QueryableDocs::QueryableDocs(const MemView& mview):mview(mview) {
}

Value MemView::Queryable::executeQuery(const QueryRequest& r) {

	return mview.runQuery(r);

}
Value MemView::QueryableDocs::executeQuery(const QueryRequest& r) {

	return mview.runDocQuery(r);

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

Value MemView::runDocQuery(const QueryRequest& r) const {

	SharedSync _(lock);

	Value out;
	switch (r.mode) {
	case qmAllItems:
		out = getAllItems();
		break;
	case qmKeyList:
		out = getItemsByDocs(r.keys);
		break;
	case qmStringPrefix: {
		String k = r.keys[0].toString();
		String from = k;
		String to({from, Query::maxString});
		out = getItemsByDocsRange(from,to,false);
		} break;
	case qmKeyRange:
		out = getItemsByDocsRange(r.keys[0].toString(),r.keys[1].toString(),r.exclude_end);
		break;
	default:
		break;
	}

	if (r.reversedOrder) {
		out = out.reverse();
	}

	if (viewDef.listFn != nullptr) out = viewDef.listFn(out);


	return Object("rows",out)("total", keyToValueMap.size());

}

template<typename Iter>
static Value resultToValue(const Iter &itm) {
	return itm.second;
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

void MemView::onChange(const ChangeEvent& doc) {
	String docId(doc["id"]);
	USync _(updateLock);
	{
		if (!doc.deleted) {
			Value d = doc.doc;
			addDoc(d);
		} else {
			eraseDoc(docId);
		}

	}
	updateSeq = doc.seqId;
	onUpdate();
}

void MemView::addDoc(const Value& doc) {
	Value vid = doc["_id"];
	if (vid.type() == json::string) {

		String id(vid);
		if ((flags &  flgIncludeDesignDocs) == 0 && id.substr(0,1) == "_") return;

		class Emit: public EmitFn {
		public:
			Emit(MemView &owner, const String &id, const Value &doc, bool &mapped)
				:sync(owner.lock, std::defer_lock), owner(owner),doc(doc),mapped(mapped), id(id) {}
			virtual void operator()(const Value &key, const Value &value) const {
				if (!mapped) {
					mapped = true;
					sync.lock();
					owner.eraseDocLk(id);
				}
				owner.addDocLk(id, doc, key, value);
			}

		protected:
			mutable Sync sync;
			MemView &owner;
			const Value &doc;
			bool &mapped;
			const String &id;
		};

		bool mapped = false;
		mapDoc(doc, Emit(*this, id, doc, mapped));
		if (!mapped) {
			eraseDoc(String(doc["_id"]));
		}
		//todo erase doc if no addition
	}
}

void MemView::addDocLk(const String &id, const Value& doc, const Value& key, const Value& value) {
	Value idocs = flags & flgIncludeDocs? doc.stripKey() : Value();

	Value skey = key.stripKey();
	Value svalue = value.stripKey();

	if (id == skey.getString()) skey = id;

	keyToValueMap.insert(std::make_pair(KeyAndDocId(skey,id),makeRow(id,skey,svalue,idocs)));
	docToKeyMap.insert(std::make_pair(id,skey));
	fireKeyChangeEvent(skey);
}

void MemView::addDocLk(const RRow &rw) {
	Value key(rw["key"]);
	String id(rw["id"]);
	keyToValueMap.insert(std::make_pair(KeyAndDocId(key,id),rw));
	docToKeyMap.insert(std::make_pair(id,key));
	fireKeyChangeEvent(key);
}

void MemView::update(CouchDB& db) {
	USync _(updateLock);
	updateLk(db);
}

void MemView::setCheckpointFile(const PCheckpoint& checkpointFile,  Value serialNr, std::size_t saveInterval) {
	USync _(updateLock);
	chkpStore = checkpointFile;
	clear();
	Value res = chkpStore->load();


	Sync __(lock);
	if (!res.defined()) {
		updateSeq = "0";
	} else {

		updateSeq = res["updateSeq"];
		Value sr = res["serial"];
		if (sr != serialNr) {
			updateSeq = "0";
		} else {
			Value index = res["rows"];
			Value prevKey;

			for (RRow kv : index) {
				addDocLk(kv);
			}
		}
	}

	chkpInterval = saveInterval;
	chkpNextUpdate = SeqNumber(updateSeq).getRevId() + chkpInterval;
	chkSrNr = serialNr;

}

Value MemView::getUpdateSeq() const {
	USync _(updateLock);
	return updateSeq;

}

void MemView::makeCheckpoint() {
	makeCheckpoint(chkpStore);
}

void MemView::makeCheckpoint(PCheckpoint chkpStore) {
	Array out;
	{
		SharedSync _(lock);
		out.reserve(keyToValueMap.size());


		for (auto &&itm : keyToValueMap) {
			out.push_back(itm.second);
		}
	}

	Value d  = Object("updateSeq",updateSeq)("rows",out)("serial",chkSrNr);
	chkpStore->store(d);

}

Value MemView::getItemsByDocsRange(const json::String& from,
		const json::String& to, bool exclude_end) const {

	Array out;
	auto iter1 = docToKeyMap.lower_bound(from);
	auto iter2 = exclude_end?docToKeyMap.lower_bound(to):docToKeyMap.upper_bound(to);
	for (auto &&kk: range(std::pair(std::move(iter1),std::move(iter2)))) {
		Value key = kk.second;
		auto iter2 = keyToValueMap.find(KeyAndDocId(key,kk.first));
		out.push_back(resultToValue(*iter2));
	}
	return out;


}

Value MemView::getItemsByDocs(const json::Array& keys) const {
	Array out;
	for (Value k : keys) {
		String docid = k.toString();
		auto iter = docToKeyMap.equal_range(docid);
		for (auto &&kk: range(std::move(iter))) {
			Value key = kk.second;
			auto iter2 = keyToValueMap.find(KeyAndDocId(key,docid));
			out.push_back(resultToValue(*iter2));
		}
	}
	return out;

}

void MemView::onUpdate() {
	if (chkpStore == nullptr) return;
	std::size_t curUpdate = SeqNumber(updateSeq).getRevId();
	if (curUpdate > chkpNextUpdate) {
		makeCheckpoint();
		chkpNextUpdate = curUpdate+chkpInterval;
	}
}

void MemView::updateLk(CouchDB& db) {
	ChangesFeed feed = db.createChangesFeed();
	feed.includeDocs(true).since(updateSeq);
	Changes chs = feed.exec();
	for (ChangeEvent cdoc : chs) {
		if (cdoc.deleted) {
			eraseDoc(String(cdoc["id"]));
		} else {
			addDoc(cdoc.doc);
		}
	}
	updateSeq = feed.getLastSeq();
	onUpdate();
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

Value MemView::getLastKnownSeqID() const {
	USync _(updateLock);
	return updateSeq;
}

void MemView::fireKeyChangeEvent(Value key) {
	auto iter = keyAlterListeners.begin();
	auto end = keyAlterListeners.end();
	//process all listeners
	while (iter != end) {
		//if listener is to remove
		if (!(*iter)->event(key)) {
			//now this is new way
			//mark this position
			auto pb = iter;
			//move to next
			++iter;
			//until it is end
			while (iter != end) {
				//process each next
				if ((*iter)->event(key)) {
					//copy its pointer to previous place to cover hole
					*pb = *iter;
					//move push back iterator next
					++pb;
				}
				//move to next item
				++iter;
			}
			//erase till end
			keyAlterListeners.erase(pb,end);
			//now stop
			break;
		} else {
			++iter;
		}
	}
}




MemView::~MemView() {
	for (auto &&k: keyAlterListeners) k->release();
}

void MemView::reg(IKeyAlterListener* cb) {
	Sync _(lock);
	keyAlterListeners.push_back(cb);
}

void MemView::unreg(IKeyAlterListener* cb) {
	Sync _(lock);
	auto iter = std::find(keyAlterListeners.begin(), keyAlterListeners.end(), cb);
	if (iter != keyAlterListeners.end()) keyAlterListeners.erase(iter);
}

MemReduce::MemReduce(MemView& srcmap, ReduceFn&& reduceFn)
	:srcmap(&srcmap), reduceFn(std::move(reduceFn))
{
	srcmap.reg(this);
}

MemReduce::~MemReduce() {
	if (srcmap) srcmap->unreg(this);

}

MemReduce::DirectAccess MemReduce::direct() const {
	const_cast<MemReduce *>(this)->update();
	return MemView::direct();
}

Query MemReduce::createQuery(std::size_t viewFlags) const {
	const_cast<MemReduce *>(this)->update();
	return MemView::createQuery(viewFlags);
}

void MemReduce::update() {
	USync _(updateLock);

	if (srcmap) {

		if (!updatedKeys.empty()) {

			std::unordered_set<Value> wrk;
			std::swap(updatedKeys, wrk);
			for (auto &&v : wrk) {
				Query q = srcmap->createQuery(View::includeDocs);
				if (v.type() == json::array) {
					Value slc = v.slice(0,-1);
					if (!slc.empty()) {
						updatedKeys.insert(slc);
					}
					q.prefixKey(v);
				} else {
					q.key(v);
				}
				Result res = q.exec();
				Value reduced = reduceFn(res, false);
				KeyAndDocId kdi(v, String());
				if (reduced.defined()) {
					keyToValueMap[kdi] = RRow(Object("value",reduced)("key",v));
				} else {
					keyToValueMap.erase(kdi);
				}
				fireKeyChangeEvent(v);
			}

			while (!updatedKeys.empty()) {
				std::unordered_set<Value> wrk;
				std::swap(updatedKeys, wrk);
				for (auto &&v : wrk) {
					Query q = MemView::createQuery(View::includeDocs);
					Value slc = v.slice(0,-1);
					if (!slc.empty()) {
						updatedKeys.insert(slc);
					}
					KeyAndDocId kdi(v, String());
					q.prefixKey(v);
					Result res = q.exec();
					if (!res.empty() && res[0]["key"] == v) {
						res = res.slice(1);
					}
					Sync _(lock);
					Value reduced = reduceFn(res, true);
					if (reduced.defined()) {
						keyToValueMap[kdi] = RRow(Object("value",reduced)("key",v));
					}
				}
			}
		}
	}
}

void MemReduce::release() {
	srcmap = nullptr;
}

bool MemReduce::event(Value v) {
	updatedKeys.insert(v);
	return true;
}


}
