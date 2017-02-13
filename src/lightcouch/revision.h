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
	Revision(std::size_t revId, StrViewA tag);
	Revision(StrViewA revStr);

	std::size_t getRevId() const {return revId;}
	StrViewA getTag() const {return StrViewA(tag,tagsize);}

	static std::size_t getRevId(StrViewA rev);
	static StrViewA getTag(StrViewA rev);


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
	std::size_t tagsize;
	char tag[33];


};

}

#endif /* LIBS_LIGHTCOUCH_SRC_LIGHTCOUCH_REVISION_H_ */
