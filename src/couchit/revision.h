/*
 * revision.h
 *
 *  Created on: May 28, 2016
 *      Author: ondra
 */

#ifndef LIBS_LIGHTCOUCH_SRC_LIGHTCOUCH_REVISION_H_
#define LIBS_LIGHTCOUCH_SRC_LIGHTCOUCH_REVISION_H_
#include "collation.h"
#include "json.h"


namespace couchit {



class Revision {
public:
	Revision();
	Revision(std::size_t revId,const String &revTag);
	Revision(const String &rev);
	Revision(const Value &rev);

	std::size_t getRevId() const {return revId;}
	StrViewA getTag() const {return StrViewA(revTag).substr(revTagOffs);}

	String toString() const;

	CompareResult compare(const Revision &other) const;

	bool operator == (const Revision &other) const {return compare(other) == 0;}
	bool operator >= (const Revision &other) const {return compare(other) >= 0;}
	bool operator <= (const Revision &other) const {return compare(other) <= 0;}
	bool operator != (const Revision &other) const {return compare(other) != 0;}
	bool operator > (const Revision &other) const {return compare(other) > 0;}
	bool operator < (const Revision &other) const {return compare(other) < 0;}

protected:
	std::size_t revId;
	String revTag;
	int revTagOffs;

};

class SeqNumber: public Revision {
public:

	SeqNumber(const Value &sn):Revision(initRev(sn)),_isOld(false),origVal(sn) {}
	SeqNumber():_isOld(true) {}
	void markOld() {_isOld = true;}
	bool isOld() const {return _isOld;}

	Value toValue() const {return origVal;}
	bool defined() const {return origVal.defined();}
protected:
	bool _isOld;
	Value origVal;

	static Revision initRev(const Value &sn);


};


}

#endif /* LIBS_LIGHTCOUCH_SRC_LIGHTCOUCH_REVISION_H_ */
