/*
 * result.h
 *
 *  Created on: 12. 8. 2019
 *      Author: ondra
 */

#ifndef SRC_COUCHIT_SRC_COUCHIT_RESULT_H_
#define SRC_COUCHIT_SRC_COUCHIT_RESULT_H_

namespace couchit {



class Result: public Value {
public:

	Result(const Value &result);
	Result(const Value &resultArray, std::size_t total, std::size_t offset, const Value &updateSeq = json::undefined);

	std::size_t getTotal() const {return total;}
	std::size_t getOffset() const {return offset;}

	bool hasItems() const {return pos < cnt;}
	Value getNext() {return operator[](pos++);}
	Value peek() const {return operator[](pos);}
	void rewind() {pos = 0;}

	///Returns update_seq if available
	Value getUpdateSeq() const {return updateSeq;}




protected:

	std::size_t total;
	std::size_t offset;
	std::size_t pos;
	std::size_t cnt;
	Value updateSeq;

};



class Row: public Value {
public:
	///contains key
	Value key;
	///contains value
	Value value;
	///contains document - will be nil, if documents are not requested in the query
	Value doc;
	///contains source document ID
	Value id;
	///contains error information for this row
	Value error;

	Row(const PValue &jrow);
	Row(const Value &jrow);
	Row() {}

	///Returns 'true' if row exists (it is not error)
	bool exists() const {return error != null;}


};

}



#endif /* SRC_COUCHIT_SRC_COUCHIT_RESULT_H_ */
