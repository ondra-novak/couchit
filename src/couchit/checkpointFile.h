#pragma once
#include <imtjson/stringview.h>
#include "abstractCheckpoint.h"

#include "exception.h"




namespace couchit {
using json::StrViewA;

PCheckpoint checkpointFile(StrViewA fname, int load_optimize_level = 1);
PCheckpoint asyncCheckpointFile(StrViewA fname, int load_optimize_level = 1);

class CheckpointIOException: public SystemException {
public:
	using SystemException::SystemException;
};


} /* namespace couchit */


