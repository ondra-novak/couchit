/*
 * query.tcc
 *
 *  Created on: 15. 9. 2016
 *      Author: ondra
 */

#ifndef LIGHTCOUCH_QUERY_TCC_
#define LIGHTCOUCH_QUERY_TCC_

#include "query.h"

#include <lightspeed/base/containers/sort.tcc>
namespace LightCouch {


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
Result LightCouch::Result::sort(Fn compareRowsFunction,	bool descending) const {


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
inline Result LightCouch::Result::group(CmpFn compareRowsFunction,
		ReduceFn reduceFn, bool descending) const {

	AutoArray<Value> sortingRows;
	natural cnt = this->size();
	sortingRows.reserve(cnt);
	HeapSort<AutoArray<Value>, SortResultCmp<CmpFn> > heapSort(sortingRows, SortResultCmp<CmpFn>(compareRowsFunction,descending));
	for(auto &&item: *this) {
		sortingRows.add(item);
		heapSort.push();
	}
	heapSort.sortHeap();
	natural startPos = 0;
	natural insertPos = 0;
	Array out;
	for (natural i = 1; i < cnt; i++) {
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
inline Result LightCouch::Result::merge(const Result& other, MergeFn mergeFn) const {

	natural leftPos = 0;
	natural rightPos = 0;
	natural leftCnt = size();
	natural rightCnt = other.size();
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
	mutable AutoArray<Value, SmallAlloc<4> > joinRows;

	void addResult(Value v) const {
		joinRows.add(v);
	}
	ResultJoinHlp(Value baseObject);
};

template<typename BindFn>
inline Result LightCouch::Result::join(QueryBase& q, const StrView &name, natural flags,  BindFn bindFn)
{
	typedef MultiMap<Value, ResultJoinHlp, JsonIsLess> ResultMap;
	ResultMap map;

	q.reset();

	natural cnt = size();
	for (natural i = 0; i < cnt; i++) {
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
					(name,Value(StrViewT<Value>(row.value.joinRows))));
					break;
			default:
				//skip row
				break;
			}

		}

	}

	return Result(Object("rows",output));
}



}



#endif /* LIGHTCOUCH_QUERY_TCC_ */
