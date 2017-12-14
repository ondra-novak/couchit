#pragma once
#include <imtjson/refcnt.h>




namespace couchit {

using json::RefCntObj;
using json::RefCntPtr;

class Result;

class AbstractCheckpoint: public RefCntObj {
public:

	virtual Result load() const = 0;
	virtual void store(const Result &res) = 0;

};


typedef RefCntPtr<AbstractCheckpoint> PCheckpoint;

}
