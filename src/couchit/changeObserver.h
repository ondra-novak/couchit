#pragma once

namespace couchit {


class ChangedDoc;

class IChangeObserver {
public:

	virtual ~IChangeObserver() {}

	virtual void onChange(const ChangedDoc &doc) = 0;

};
}
