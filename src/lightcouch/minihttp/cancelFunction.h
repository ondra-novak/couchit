/*
 * cancelFunction.h
 *
 *  Created on: 15. 11. 2016
 *      Author: ondra
 */

#ifndef LIGHTCOUCH_MINIHTTP_CANCELFUNCTION_H_
#define LIGHTCOUCH_MINIHTTP_CANCELFUNCTION_H_
#include <immujson/refcnt.h>

namespace LightCouch {

class ICancelWait: public json::RefCntObj {
public:
	virtual ~ICancelWait() {}
	virtual void cancelWait() = 0;
};

class CancelFunction {
public:
	CancelFunction() {}
	CancelFunction(ICancelWait *impl):impl(impl) {}

	void operator()() {
		if (impl != nullptr) impl->cancelWait();
	}
	bool operator!() const {
		return impl == nullptr;
	}

	operator bool() const {
		return impl != nullptr;
	}

	operator ICancelWait *() const {
		return impl;
	}
protected:
	json::RefCntPtr<ICancelWait> impl;

};

}



#endif /* LIGHTCOUCH_MINIHTTP_CANCELFUNCTION_H_ */
