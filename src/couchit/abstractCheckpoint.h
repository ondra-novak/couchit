#pragma once
#include <imtjson/refcnt.h>
#include <imtjson/value.h>




namespace couchit {

using json::RefCntObj;
using json::RefCntPtr;
using json::Value;



class AbstractCheckpoint: public RefCntObj {
public:

	virtual Value load() const = 0;
	virtual void store(const Value &res) = 0;
	virtual bool load(const std::function<void(std::istream &stream)> &fn) const = 0;
	virtual void store(const std::function<void(std::ostream &stream)> &fn) = 0;

};


typedef RefCntPtr<AbstractCheckpoint> PCheckpoint;

}
