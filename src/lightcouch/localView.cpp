/*
 * localView.cpp
 *
 *  Created on: 5. 6. 2016
 *      Author: ondra
 */

#include "localView.h"

#include "collation.h"
#include "couchDB.h"

#include "json.h"
#include "queryServerIfc.h"

namespace LightCouch {


template<typename T>
class IterRange {
public:
	IterRange(std::pair<T,T> &&range):range(std::move(range)) {}
	IterRange(T &&lower, T &&upper):range(std::move(lower),std::move(upper)) {}
	const T &begin() const {return range.first;}
	const T &end() const {return range.second;}

protected:
	std::pair<T,T> range;
};

template<typename T>
IterRange<T> iterRange(std::pair<T,T> &&range) {return IterRange<T>(std::move(range));}
template<typename T>
IterRange<T> iterRange(T &&lower, T &&upper) {return IterRange<T>(std::move(lower),std::move(upper));}

class AllDocView: public AbstractViewMapOnly<0> {
public:
	virtual void map(const Document &, IEmitFn &emit) override {
		emit();
	}
};

static AllDocView allDocView;

LocalView::LocalView():queryable(*this),includeDocs(false),linkedView(&allDocView){

}



LocalView::LocalView(std::size_t flags)
	:queryable(*this)
	,includeDocs((flags & View::includeDocs) != 0)
	,linkedView(&allDocView)
{

}

LocalView::LocalView(AbstractViewBase *view, std::size_t flags)
	:queryable(*this)
	,includeDocs((flags & View::includeDocs) != 0)
	,linkedView(view)
{

}


LocalView::~LocalView() {
	if (linkedView != 0 && linkedView != &allDocView) {
		delete linkedView;
	}
}


void LocalView::updateDoc(const Value& doc) {
	Exclusive _(lock);
	updateDocLk(doc);
}
void LocalView::updateDocLk(const Value& doc) {
	String docId = doc["_id"].getString();
	eraseDocLk(docId);
	Value delFlag = doc["_deleted"];
	if (!delFlag.defined() || delFlag.getBool() == false) {
		curDoc=doc;
		map(doc);
		curDoc = Value();
	}
}

class LocalView::Emitor: public IEmitFn {
public:
	Emitor(LocalView &owner):owner(owner) {}
	virtual void operator()() {
		owner.emit();
	}
	virtual void operator()(const Value &key) {
		owner.emit(key);
	}
	virtual void operator()(const Value &key, const Value &value) {
		owner.emit(key,value);
	}

protected:
	LocalView &owner;

};

void LocalView::map(const Document &doc)  {
	Emitor e(*this);
	linkedView->map(doc,e);

}


void LocalView::emit(const Value& key, const Value& value) {
	addDocLk(String(curDoc["_id"]),curDoc, key, value);
}

void LocalView::emit(const Value& key) {
	addDocLk(String(curDoc["_id"]),curDoc, key,nullptr);
}

void LocalView::emit() {
	addDocLk(String(curDoc["_id"]),curDoc,nullptr, nullptr);
}

void LocalView::loadFromQuery(const Query& q) {
	Exclusive _(lock);

	Result res(q.exec());
	for (auto &&v : res) {
		Row rw(v);
		addDocLk(String(rw.id),rw.doc,rw.key,rw.value);
	}
}

void LocalView::clear() {
	Exclusive _(lock);
	keyToValueMap.clear();
	docToKeyMap.clear();
}

bool LocalView::empty() const {
	Shared _(lock);
	return keyToValueMap.empty();
}

void LocalView::eraseDocLk(const String &docId) {
	auto range = iterRange(docToKeyMap.equal_range(docId));
	for (auto &v : range) {
		keyToValueMap.erase(KeyAndDocId(v.second,docId));
	}
	docToKeyMap.erase(range.begin(), range.end());
}

void LocalView::loadFromView(CouchDB& db, const View& view, bool runMapFn) {
	LightCouch::Query q = db.createQuery(view);
	LightCouch::Result res = q.exec();

	Exclusive _(lock);

	while (res.hasItems()) {
		const Row &row = res.getNext();

		Value doc = row.doc;

		//runMapFn is true and there is document
		if (runMapFn && doc.defined()) {
			//process through the map function
			updateDocLk(doc);
		} else {
			//add this document
			addDocLk(String(row.id),doc,row.key,row.value);
		}
	}

}

void LocalView::eraseDoc(const String &docId) {

	Exclusive _(lock);
	eraseDocLk(docId);

}

void LocalView::addDoc(const Value& doc, const Value& key,
		const Value& value) {

	Exclusive _(lock);
	addDocLk(String(doc["_id"]),doc, key, value);
}

Value LocalView::getDocument(const String &docId) const {
	Shared _(lock);
	auto it = docToKeyMap.find(docId);
	if (it == docToKeyMap.end()) return Value();
	auto it2 = keyToValueMap.find(KeyAndDocId(it->second, docId));
	if (it2 == keyToValueMap.end()) return Value();
	return it2->second.doc;
}


Value LocalView::reduce(const RowsWithKeys &r) const {
	return linkedView->reduce(r);
}

Value LocalView::rereduce(const ReducedRows &r) const {
	return linkedView->rereduce(r);
}

void LocalView::addDocLk(const String &docId, const Value &doc, const Value& key, const Value& value) {

	auto r = keyToValueMap.insert(std::pair<KeyAndDocId, ValueAndDoc>(
			KeyAndDocId(key,docId), ValueAndDoc(value,includeDocs?doc:Value())));
	if (r.second) {
		docToKeyMap.insert(std::pair<String,Value>(docId,key));
	}

}



int LocalView::KeyAndDocId::compare(const KeyAndDocId& other) const {
	int c = compareJson(key,other.key);
	if (c == 0) {
		return compareStringsUnicode(docId,other.docId);
	} else {
		return c;
	}
}

Value LocalView::searchKeys(const Value &keys, std::size_t groupLevel) const {

	Shared _(lock);

	std::vector<RowWithKey> group;

	if (groupLevel == 0) {
		for (auto &&key : keys) {
			for (auto &kv : iterRange(keyToValueMap.lower_bound(KeyAndDocId(key, Query::minString)),
						   keyToValueMap.upper_bound(KeyAndDocId(key, Query::maxString)))) {

				group.push_back(RowWithKey(kv.first.docId, kv.first.key,kv.second.value));
			}
		}
		return Object("rows",Array().add(
					Object("key",nullptr)
			      	  ("value",reduce(StringView<RowWithKey>(group.data(),group.size())))
				));
	} else if (groupLevel != ((std::size_t)-1)) {
		Array rows;
		for (auto &&key : keys) {
			for (auto &kv : iterRange(keyToValueMap.lower_bound(KeyAndDocId(key, Query::minString)),
						   keyToValueMap.upper_bound(KeyAndDocId(key, Query::maxString)))) {

				group.push_back(RowWithKey(kv.first.docId, kv.first.key,kv.second.value));
			}
			rows.add(Object("key",key)
						("value",reduce(StringView<RowWithKey>(group.data(),group.size()))));
			group.clear();
		}
		return Object("rows",rows);
	} else {
		Array rows;
		for (auto &&key : keys) {
			for (auto &kv : iterRange(keyToValueMap.lower_bound(KeyAndDocId(key, Query::minString)),
						   keyToValueMap.upper_bound(KeyAndDocId(key, Query::maxString)))) {

				rows.add(Object("key",kv.first.key)
							("value",kv.second.value)
							("id",kv.first.docId)
							("doc",kv.second.doc));
			}
		}
		return Object("rows",rows)
		 	 ("total_rows",keyToValueMap.size());
	}

}

Query LocalView::createQuery(std::size_t viewFlags) const {
	View v(String(),viewFlags);
	return Query(v, queryable);
}

Query LocalView::createQuery(std::size_t viewFlags, PostProcessFn fn) const {
	View v(String(),viewFlags,fn);
	return Query(v, queryable);
}


static bool canGroupKeys(const Value &subj, const Value &sliced) {
	if (!sliced.defined()) return false;
	if (subj.type() == json::array) {
		std::size_t cnt = subj.size();
		if (cnt >= sliced.size()) {
			cnt = sliced.size();
		} else {
			return false;
		}

		for (std::size_t i = 0; i < cnt; i++) {
			if (compareJson(subj[i],sliced[i]) != 0) return false;
		}
		return true;
	} else {
		return compareJson(subj,sliced) == 0;
	}
}

static Value sliceKey(const Value &key, std::size_t groupLevel) {
	if (key.type() == json::array) {
		if (key.size() <= groupLevel) return key;
		Array out;
		for (std::size_t i = 0; i < groupLevel; i++)
			out.add(key[i]);
		return out;
	} else {
		return key;
	}

}

template<typename T>
class ReversedIterator: public T {
public:
	ReversedIterator(T &&iter):T(std::move(iter)) {
	}
	ReversedIterator(const T &iter):T(iter) {}
	ReversedIterator &operator++() {
		T::operator--();return *this;
	}
	ReversedIterator operator++(int x) {
		return ReversedIterator<T>(T::operator--(x));
	}
	ReversedIterator &operator--() {
		T::operator++();return *this;
	}
	ReversedIterator operator--(int x) {
		return ReversedIterator<T>(T::operator++(x));
	}
	auto operator *() -> decltype(T::operator *()) const {
		T x = *this;
		--x;
		return *x;
	}
};

template<typename T>
ReversedIterator<T> reversedIterator(T &&iter) {
	return ReversedIterator<T>(std::move(iter));
}

template<typename R>
Value LocalView::searchRange2(R &&range, std::size_t groupLevel, std::size_t offset, std::size_t limit) const {

	std::size_t totalLimit = limit+offset<offset?((std::size_t)-1):limit+offset;

	std::size_t p = 0;
	Array rows;


	//no grouping - perform standard iteration
	if (groupLevel == ((std::size_t)-1)) {
		//process range
		for (auto &&kv : range) {
			//if reached totalLimit, exit iteration
			if (p >= totalLimit) break;
			//if reached offset, start to add items to the result
			if (p >= offset) {
				//add item
				rows.add(Object("id",kv.first.docId)
					    ("key",kv.first.key)
						("value",kv.second.value)
						("doc",kv.second.doc));
			}
			//count processed rows
			++p;
		}
		//finished - generate result
		return Object("rows",rows)
				("total_rows",keyToValueMap.size());
	} else {
		//grouping active, prepare empty group
		std::vector<RowWithKey> group;
		//prepare grouping key
		Value grKey;
		//iterator rows
		for (auto &&kv : range) {
			//stop iterate when limit reached
			if (p >= totalLimit) break;
			//whether keys cannot be grouped
			if (!canGroupKeys(kv.first.key, grKey)) {
				//skip empty groups
				if (!group.empty()) {
					//skip if offset is not reached
					if (p >= offset) {
						//add row after running reduce
						rows.push_back(Object("key",grKey)
								("value",reduce(StringView<RowWithKey>(group.data(),group.size()))));
					}
					//clear the group
					group.clear();
					//increase emited results
					++p;
				}
				//update group key
				grKey = sliceKey(kv.first.key, groupLevel);
			}
			//add item to group
			group.push_back(RowWithKey(kv.first.docId,kv.first.key, kv.second.value));
		}
		//finalize last group
		//when we are in limit, when group is not empty and when offset was reached
		if (p < totalLimit && !group.empty() && p >= offset) {
			rows.add(Object("key",grKey)
					("value",reduce(StringView<RowWithKey>(group.data(),group.size()))));
		}
		//finalise result
		return Object("rows",rows);
	}
}


Value LocalView::searchRange(const Value &startKey, const Value &endKey,
std::size_t groupLevel, bool descending, std::size_t offset, std::size_t limit,
bool excludeEnd) const {

	Shared _(lock);

	String startDoc = startKey.getKey();
	String endDoc = endKey.getKey();
	if (endDoc.empty() && !excludeEnd) endDoc = Query::maxString;

	KeyAndDocId start(startKey,startDoc);
	KeyAndDocId end(endKey,endDoc);
	if (start.compare(end) > 0) {
		return Object("rows",json::array)
				("total_rows",keyToValueMap.size());
	}

	if (descending) {
		return searchRange2(
			iterRange(reversedIterator(
							keyToValueMap.upper_bound(end)),
					  reversedIterator(
							keyToValueMap.lower_bound(start))),
			groupLevel,
			offset,
			limit);
	} else {
		return searchRange2(
			iterRange(keyToValueMap.lower_bound(start),
						keyToValueMap.upper_bound(end)),
			groupLevel,
			offset,
			limit
		);
	}


}

LocalView::Queryable::Queryable(const LocalView& lview):lview(lview) {

}

/*
 *
 * Result LocalView::Query::exec() const {
	finishCurrent();
	Value result;
	if (keys == null || keys.empty()) {
		result = lview.searchRange(startkey,endkey, groupLevel, descent, offset, maxlimit,offset_doc,(viewFlags & View::exludeEnd) != 0);
	} else {
		result = lview.searchKeys(keys,groupLevel);
	}
	if (ppfn) result = ppfn( args, result);
	return Result(result);
}
 */
Value LocalView::Queryable::executeQuery(const QueryRequest& r) {

	bool descend = ((r.view.flags & View::reverseOrder) != 0) != r.reversedOrder;
	std::size_t groupLevel;
	bool reduce;
	Value result;



	switch(r.reduceMode) {
	case rmDefault:
		reduce = (r.view.flags & View::reduce) != 0;
		groupLevel = (r.view.flags & View::groupLevelMask)/View::groupLevel;
		break;
	case rmGroup:
		reduce = true;
		groupLevel = 256;
		break;
	case rmNoReduce:
		reduce = false;
		break;
	case rmGroupLevel:
		reduce = true;
		groupLevel = r.groupLevel;
		break;
	case rmReduce:
		reduce = true;
		groupLevel = 0;
		break;
	}

	if (reduce == false) groupLevel = ((std::size_t)-1);
	switch (r.mode) {
	case qmAllItems:
		result = lview.searchRange(Query::minKey,Query::maxKey,groupLevel,descend,r.offset,r.limit,false);
		break;
	case qmKeyList:
		result = lview.searchKeys(r.keys,groupLevel);
		break;
	case qmKeyRange:
		result = lview.searchRange(r.keys[0],r.keys[1],groupLevel,descend,r.offset,r.limit,r.exclude_end);
		break;
	case qmKeyPrefix: {
			result = lview.searchRange(addToArray(r.keys[0],Query::minKey),
						addToArray(r.keys[0],Query::maxKey)
					,groupLevel,descend,r.offset,r.limit,false);
		}
		break;
	case qmStringPrefix: {
		result = lview.searchRange(addSuffix(r.keys[0],Query::minString),
				addSuffix(r.keys[0],Query::maxString)
				,groupLevel,descend,r.offset,r.limit,false);
		}

	}
	if (r.view.postprocess) result = r.view.postprocess(0, r.ppargs, result);
	return result;



}




} /* namespace LightCouch */

