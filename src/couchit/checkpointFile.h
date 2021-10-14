#pragma once
#include "abstractCheckpoint.h"

#include "exception.h"




namespace couchit {


PCheckpoint checkpointFile(std::string_view fname, int load_optimize_level = 1);
PCheckpoint asyncCheckpointFile(std::string_view fname, int load_optimize_level = 1);

class CheckpointIOException: public SystemException {
public:
	using SystemException::SystemException;
};


} /* namespace couchit */


