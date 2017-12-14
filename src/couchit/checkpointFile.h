#pragma once
#include <imtjson/stringview.h>
#include "abstractCheckpoint.h"

#include "exception.h"




namespace couchit {
using json::StrViewA;

PCheckpoint checkpointFile(StrViewA fname);
PCheckpoint asyncCheckpointFile(StrViewA fname);

class CheckpointIOException: public SystemException {
public:
	using SystemException::SystemException;
};


} /* namespace couchit */


