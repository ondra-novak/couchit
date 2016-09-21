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


	AutoArray<ConstValue> sortingRows;
	sortingRows.reserve(this->length());
	HeapSort<AutoArray<ConstValue>, SortResultCmp<Fn> > heapSort(sortingRows, SortResultCmp<Fn>(compareRowsFunction,descending));
	for (natural i = 0, cnt = this->length(); i < cnt; i++) {
		sortingRows.add((*this)[i]);
		heapSort.push();
	}
	heapSort.sortHeap();
	JSON::ConstValue newrows = json.factory->newValue(ConstStringT<ConstValue>(sortingRows));
	return Result(json,json("rows",newrows)("total_rows",total)("offset",offset));
}

template<typename CmpFn, typename ReduceFn>
inline Result LightCouch::Result::group(CmpFn compareRowsFunction,
		ReduceFn reduceFn, bool descending) const {
	AutoArray<ConstValue> sortingRows;
	sortingRows.reserve(this->length());
	HeapSort<AutoArray<ConstValue>, SortResultCmp<CmpFn> > heapSort(sortingRows, SortResultCmp<CmpFn>(compareRowsFunction,descending));
	natural cnt = this->length();
	for (natural i = 0; i < cnt; i++) {
		sortingRows.add((*this)[i]);
		heapSort.push();
	}
	heapSort.sortHeap();
	natural startPos = 0;
	natural insertPos = 0;
	for (natural i = 1; i < cnt; i++) {
		if (compareRowsFunction(sortingRows[startPos],sortingRows[i]) != 0) {
			ConstStringT<ConstValue> block = sortingRows.mid(startPos, i-startPos);
			ConstValue res = reduceFn(block);
			if (res != null)
				sortingRows(insertPos++) = res;
			startPos = i;
		}
	}
	if (startPos < cnt) {
		ConstStringT<ConstValue> block = sortingRows.mid(startPos, cnt-startPos);
		ConstValue res = reduceFn(block);
		if (res != null)
			sortingRows(insertPos++) = res;
	}
	JSON::ConstValue newrows = json.factory->newValue(ConstStringT<ConstValue>(sortingRows.head(insertPos)));
	return Result(json,json("rows",newrows));
}

template<typename MergeFn>
inline Result LightCouch::Result::merge(const Result& other, MergeFn mergeFn) const {

	natural leftPos = 0;
	natural rightPos = 0;
	natural leftCnt = length();
	natural rightCnt = other.length();
	AutoArray<ConstValue> rows;

	while (leftPos < leftCnt && rightPos < rightCnt) {
		ConstValue left = (*this)[leftPos];
		ConstValue right = (*this)[rightPos];
		ConstValue res = mergeFn(left,right);
		if (res == left) {
			leftPos++;
		} else if (res == right) {
			rightPos++;
		} else {
			leftPos++;
			rightPos++;
		}
		if (res != null) {
			rows.add(res);
		}
	}
	while (leftPos < leftCnt) {
		ConstValue left = (*this)[leftPos];
		ConstValue res = mergeFn(left,null);
		leftPos++;
		if (res != null) {
			rows.add(res);
		}
	}
	while (rightPos < rightCnt) {
		ConstValue right = (*this)[rightPos];
		ConstValue res = mergeFn(null,right);
			rightPos++;
		if (res != null) {
			rows.add(res);
		}
	}

	JSON::ConstValue newrows = json.factory->newValue(ConstStringT<ConstValue>(rows));
	return Result(json,json("rows",newrows));
}

template<typename BindFn>
inline Result LightCouch::Result::join(QueryBase& q, ConstStrA name, natural flags,  BindFn bindFn)
{
	typedef MultiMap<ConstValue, Container, JsonIsLess> ResultMap;
	ResultMap map;

	q.reset();

	natural cnt = length();
	for (natural i = 0; i < cnt; i++) {
		ConstValue row = (*this)[i];
		ConstValue fk = bindFn(row);
		ResultMap::ValueList &rows = map(fk);
		if (rows.empty()) {
			q.selectKey(fk);
		}
		Container newrow = row->copy(json.factory,1);

	}

	AutoArray<ConstValue> output;

	Result fklookup = q.exec();

	while (fklookup.hasItems()) {
		ConstValue row = fklookup.getNext();
		ConstValue key = row["key"];
		ConstValue value = row["value"];
		ResultMap::ListIter list = map.find(key);
		switch (flags & 0x3) {
		case joinFirstRow: {
			Container k = list.getNext();
			if (k[name] != null) continue;
			k.set("name", value);
			while (list.hasItems()) {
				Container(list.getNext()).set(name, value);
			}
		}
		break;
		case joinLastRow: {
			while (list.hasItems()) {
				Container(list.getNext()).set(name, value);
			}
		}
		break;
		case joinAllRows: {
			Container k = list.getNext();
			Container values;
			ConstValue curv = k[name];
			if (curv != null) {
				ConstValue cont = k[name];
				values = static_cast<Container &>(cont);
			}
			else {
				values = json.array();
				k.set(name, values);
				while (list.hasItems()) {
					Container(list.getNext()).set(name, value);
				}
			}
			values.add(value);
		}
		break;
		}
	}

	if (flags & joinMissingRows) {
		for (auto iter = map.getFwIter(); iter.hasItems();) {
			output.add(iter.getNext().value);
		}
	} else {
		for (auto iter = map.getFwIter(); iter.hasItems();) {
			const ResultMap::KeyValue &kv = iter.getNext();
			if (kv.value[name] != null) {
				output.add(kv.value);
			}
		}

	}
	JSON::ConstValue newrows = json.factory->newValue(ConstStringT<ConstValue>(output));
	return Result(json,json("rows",newrows));
}



}



#endif /* LIGHTCOUCH_QUERY_TCC_ */
