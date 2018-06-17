#pragma once

namespace couchit {


class ChangedDoc;

class IChangeObserver {
public:

	virtual ~IChangeObserver() {}

	///called on change
	virtual void onChange(const ChangedDoc &doc) = 0;
	///Requests for last known seqID
	/** It is called when the ChangesDistributor needs to know where to start reading
	 *
	 * @retval "seqid" start from given seqid
	 * @retval null start from the beginning
	 * @retval undefined start from "now" (no history)
	 *
	 *
	 * @note even if the function returns valid seqid, it is used as a hint. The
	 * ChangesDistributor can start to read more in history while it distributes
	 * already known recods
	 * */
	virtual Value getLastKnownSeqID() const = 0;

};
}
