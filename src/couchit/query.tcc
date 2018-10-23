/*
 * query.tcc
 *
 *  Created on: 15. 9. 2016
 *      Author: ondra
 */

#ifndef LIGHTCOUCH_QUERY_TCC_
#define LIGHTCOUCH_QUERY_TCC_

#include "query.h"
#include "changeset.h"

namespace couchit {

#if 0

template<typename Fn>
struct SortResultCmp {
	Fn fn;
	SortResultCmp(const Fn &fn, bool descending):fn(fn),needVal(descending?1:-1) {}

	template<typename X>
	bool operator()(const X &a, const X &b) {
		return needVal*fn(a,b) >= 0;
	}
	int needVal;
};

template<typename Fn>
Result couchit::Result::sort(Fn compareRowsFunction,	bool descending) const {


	AutoArray<Value> sortingRows;
	sortingRows.reserve(this->size());
	HeapSort<AutoArray<Value>, SortResultCmp<Fn> > heapSort(sortingRows, SortResultCmp<Fn>(compareRowsFunction,descending));
	for(auto &&item: *this) {
		sortingRows.add(item);
		heapSort.push();
	}
	Array out;
	while (!sortingRows.empty()) {
		out.add(heapSort.top());
		heapSort.pop();
		sortingRows.resize(heapSort.getSize());
	}
	return Result(Object("rows",out)("total_rows",total)("offset",offset));
}

template<typename CmpFn, typename ReduceFn>
inline Result couchit::Result::group(CmpFn compareRowsFunction,
		ReduceFn reduceFn, bool descending) const {

	AutoArray<Value> sortingRows;
	std::size_t cnt = this->size();
	sortingRows.reserve(cnt);
	HeapSort<AutoArray<Value>, SortResultCmp<CmpFn> > heapSort(sortingRows, SortResultCmp<CmpFn>(compareRowsFunction,descending));
	for(auto &&item: *this) {
		sortingRows.add(item);
		heapSort.push();
	}
	heapSort.sortHeap();
	std::size_t startPos = 0;
	std::size_t insertPos = 0;
	Array out;
	for (std::size_t i = 1; i < cnt; i++) {
		if (compareRowsFunction(sortingRows[startPos],sortingRows[i]) != 0) {
			ConstStringT<Value> block = sortingRows.mid(startPos, i-startPos);
			Value res = reduceFn(block);
			if (res.defined())
				out.add(res);
			startPos = i;
		}
	}
	if (startPos < cnt) {
		ConstStringT<Value> block = sortingRows.mid(startPos, cnt-startPos);
		Value res = reduceFn(block);
		if (res.defined())
			sortingRows(insertPos++) = res;
	}
	return Result(Object("rows",out));
}

template<typename MergeFn>
inline Result couchit::Result::merge(const Result& other, MergeFn mergeFn) const {

	std::size_t leftPos = 0;
	std::size_t rightPos = 0;
	std::size_t leftCnt = size();
	std::size_t rightCnt = other.size();
	Array rows;

	while (leftPos < leftCnt && rightPos < rightCnt) {
		Value left = (*this)[leftPos];
		Value right = other[rightPos];
		Value res = mergeFn(left,right);
		if (res == left) {
			leftPos++;
		} else if (res == right) {
			rightPos++;
		} else {
			leftPos++;
			rightPos++;
		}
		if (res.defined()) {
			rows.add(res);
		}
	}
	while (leftPos < leftCnt) {
		Value left = (*this)[leftPos];
		Value res = mergeFn(left,null);
		leftPos++;
		if (res.defined()) {
			rows.add(res);
		}
	}
	while (rightPos < rightCnt) {
		Value right = (*this)[rightPos];
		Value res = mergeFn(null,right);
			rightPos++;
		if (res.defined()) {
			rows.add(res);
		}
	}

	return Result(Object("rows",out));
}


struct ResultJoinHlp {
	Value baseObject;
	mutable std::vector<Value> joinRows;

	void addResult(Value v) const {
		joinRows.add(v);
	}
	ResultJoinHlp(Value baseObject);
};

template<typename BindFn>
inline Result couchit::Result::join(QueryBase& q, const StrViewA &name, std::size_t flags,  BindFn bindFn)
{
	typedef std::unordered_map<Value, ResultJoinHlp> ResultMap;
	ResultMap map;

	q.reset();

	std::size_t cnt = size();
	for (std::size_t i = 0; i < cnt; i++) {
		Value row = (*this)[i];
		Value fk = bindFn(row);
		ResultMap::ValueList &rows = map(fk);
		if (rows.empty()) {
			q.selectKey(fk);
		}
		rows.add(ResultJoinHlp(row));
	}

	Array output;

	Result fklookup = q.exec();

	for (auto &&row : fklookup) {

		Value key = row["key"];
		Value value = row["value"];
		ResultMap::ListIter list = map.find(key);
		while (list.hasItems()) {
			const ResultJoinHlp &hlp = list.getNext();
			hlp.addResult(value);
		}
	}

	for (auto &&row : map.getFwIter()) {

		if ((flags & joinMissingRows) && row.value.joinRows.empty()) {
			output.add(row.value.baseObject);
		} else if (!row.value.joinRows.empty()) {

			switch (flags & 0x3) {
			case joinFirstRow: output.add(Object(row.value.baseObject)
					(name,row.value.joinRows[0]));
					break;
			case joinLastRow: output.add(Object(row.value.baseObject)
					(name,row.value.joinRows.tail(1)[0]));
					break;
			case joinAllRows: output.add(Object(row.value.baseObject)
					(name,Value(StrViewAT<Value>(row.value.joinRows))));
					break;
			default:
				//skip row
				break;
			}

		}

	}

	return Result(Object("rows",output));
}

#endif

template<typename BindFn, typename AgrFn, typename MergeFn>
void JoinedQuery<BindFn,AgrFn,MergeFn>::QObj::addFk(const Value &v, std::size_t index) {
	if (v.defined()) {
		keyAtIndexMap.insert(KeyAtIndexMap::value_type(v, IndexType(index,0)));
	}
}
template<typename BindFn, typename AgrFn, typename MergeFn>
void JoinedQuery<BindFn,AgrFn,MergeFn>::QObj::addFk(const KeyType &v, std::size_t index) {
	int pos = 0;
	for (const Value &e : v) {
		keyAtIndexMap.insert(KeyAtIndexMap::value_type(e, IndexType(index,pos++)));
	}
}


template<typename BindFn, typename AgrFn, typename MergeFn>
Value JoinedQuery<BindFn,AgrFn,MergeFn>::QObj::executeQuery(const QueryRequest &) {
	Result res = owner.lq.exec();
	keyAtIndexMap.clear();
	resultMap.clear();

	Array out;

	std::size_t sz = res.size();
	resultMap.clear();
	resultMap.reserve(sz);
	keyAtIndexMap.reserve(sz);
	for (std::size_t i = 0; i < sz; i++) {
		Value r = res[i];
		addFk(bindFn(r),i);
		resultMap.push_back(ResultType());
	}

	Array keys;
	keys.reserve(keyAtIndexMap.size());

	for (auto iter=keyAtIndexMap.begin();
	     iter!=keyAtIndexMap.end();
	     iter=keyAtIndexMap.equal_range(iter->first).second){
	  keys.push_back(iter->first);
	}

	if (!keys.empty()) {


		owner.rq.keys(keys);
		Result rside = owner.rq.exec();

		if (!rside.empty()) {
			Array group;
			Value curKey;

			for (Row r : rside) {
				if (r.key != curKey) {
					if (!group.empty()) {
						Value v = agrFn(group);
						auto rang = keyAtIndexMap.equal_range(curKey);
						for (auto iter = rang.first; iter != rang.second;++iter) {
							resultMap[iter->second.first][iter->second.second] = v;
						}
					}
					curKey = r.key;
					group.clear();
				}
				group.push_back(r);
			}
			{
				Value v = agrFn(group);
				auto rang = keyAtIndexMap.equal_range(curKey);
				for (auto iter = rang.first; iter != rang.second;++iter) {
					resultMap[iter->second.first][iter->second.second] = v;
				}
			}
		}
	}

	for (std::size_t i = 0; i < sz; i++) {
		out.push_back(mergeFn(res[i],resultMap[i]));
	}
	return Object("rows",out)
				("total_rows",res.getTotal())
				("offset",res.getOffset());
}



template<typename Fn>
couchit::Changeset couchit::Result::update(Fn &&fn) {
	Changeset ch;
	for (Row rw : *this) {
		Value doc = rw.doc;
		if (doc.defined()) {
			Value udoc = fn(doc);
			if (udoc.defined()) {
				ch.update(Document(udoc));
			}
		}
	}

	return ch;
}



}



#endif /* LIGHTCOUCH_QUERY_TCC_ */
