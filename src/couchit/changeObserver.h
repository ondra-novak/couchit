#pragma once

#include "changeevent.h"


namespace couchit {


class ChangeEvent;


class IChangeEventObserver {
public:
	virtual ~IChangeEventObserver() {}

	///Called on change
	/**
	 * @param doc change event (contains document and additional metadata
	 * @retval true continue observing
	 * @retval false stop observing, remove the observer
	 *
	 * @note When false is returned, observer is removed and destroyed through the deleter
	 */
	virtual bool onEvent(const ChangeEvent &doc) = 0;


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
	virtual json::Value getLastKnownSeqID() const = 0;
};



class IChangeObserverOld: public IChangeEventObserver  {
public:

	virtual ~IChangeObserverOld() {}

	///called on change (deprecated)
	virtual void onChange(const ChangeEvent &doc) = 0;

	virtual bool onEvent(const ChangeEvent &doc) {
		onChange(doc);
		return true;
	}

};

using IChangeObserver = IChangeObserverOld;

template<typename Fn>
class ChangeObserverFromFn: public IChangeEventObserver {
public:

	ChangeObserverFromFn(Fn &&fn, json::Value since=json::undefined):fn(std::forward<Fn>(fn)),since(since) {}

	virtual bool onEvent(const ChangeEvent &doc) {
		if (doc.seqId.hasValue())
			since = doc.seqId;
		return fn(doc);
	}

	virtual json::Value getLastKnownSeqID() const {
		return since;
	}
protected:
	Fn fn;
	json::Value since;
};

}
