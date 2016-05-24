/*
 * validator.h
 *
 *  Created on: 22. 5. 2016
 *      Author: ondra
 */

#ifndef LIBS_LIGHTCOUCH_SRC_LIGHTCOUCH_VALIDATOR_H_
#define LIBS_LIGHTCOUCH_SRC_LIGHTCOUCH_VALIDATOR_H_
#include "lightspeed/base/memory/sharedPtr.h"
#include "lightspeed/base/containers/autoArray.h"
#include "lightspeed/base/containers/string.h"
#include "lightspeed/utils/json/json.h"
#include "object.h"

namespace LightCouch {

using namespace LightSpeed;

///Validates document before it is put to the database
/** Similar to validation functions defined by design documents. However. this
 * validation is performed before document is stored in the database. You can define multiple
 * validation function. You can add or remove the function any time during object lifetime. Note
 * that object is not MT safe, so you have to protect it by appropriate synchronization objects.
 */
class Validator {
public:
	class IValidationFn {
	public:

		virtual ~IValidationFn() {}
		virtual bool operator()(const ConstValue &doc) = 0;
		virtual const StringA &getName() const = 0;
	};

	///adds validation function
	/**
	 * @param fn instance of IValidationFn. Ownership of the object is taken by the Validator.
	 * It is automatically deleted when it is no longer needed.
	 * @return reference to *this
	 */
	Validator &add(IValidationFn *fn);

	///creates and adds new validation function.
	/** In C++11, you can use lambda operator to create function in place. Note that
	 * function is copied into the internal object, that keeps the function valid until
	 * it is removed.
	 *
	 * @param fn function to call during validation. Function should return true - validation passed, or false - validation failed. Function can throw an exception
	 * @param name name of function for debuging purposes
	 * @return pointer to internal object which can be used as argument of function remove()
	 */
	template<typename Fn>
	IValidationFn *add(const Fn &fn,const StringA &name);

	///Removes validation function
	/**
	 * @param fn pointer to function retrieved by function add()
	 * @param destroy true to destroy function, false to keep function in the memory.
	 * @retval true succes
	 * @retval false not found;
	 */
	bool remove(IValidationFn *fn, bool destrou = true);


	///Result of validation
	struct Result {
		///true if test passed
		const bool valid;
		///contains name of validation function when test failed
		const StringA failedName;
		///contains details of failure - used only when validation function throws an exception
		const StringA details;

		Result():valid(true) {}
		Result(const StringA &failedName):valid(false), failedName(failedName) {}
		Result(const StringA & failedName, const StringA &details):valid(false), failedName(failedName),details(details) {}

		operator bool() const {return valid;}
		bool operator!() const {return !valid;}
	};

	///Performs validation
	/**
	 * @param document document to validate
	 * @return result of validation
	 */
	Result validateDoc(const ConstValue &document) const;

	typedef SharedPtr<IValidationFn> PValidationFn;
	typedef AutoArray<PValidationFn> FnList;

protected:

	FnList fnList;
};

class ValidationFailedException: public Exception {
public:
	LIGHTSPEED_EXCEPTIONFINAL;

	ValidationFailedException(const ProgramLocation &loc, const Validator::Result &res)
		:Exception(loc),res(res) {}

	const Validator::Result getValidationResult() const {return res;}
	virtual ~ValidationFailedException() throw();

protected:
	Validator::Result res;
	void message(ExceptionMsg &msg) const;

};

template<typename Fn>
inline Validator::IValidationFn* Validator::add(const Fn &fn,const StringA &name) {
	class FnInst: public IValidationFn {
	public:
		FnInst(const Fn &fn,const StringA &name):fn(fn),name(name) {}
		virtual bool operator()(const ConstValue &doc) {return fn(doc);}
		virtual const StringA &getName() const {return name;}

	protected:
		Fn fn;
		StringA name;
	};
	PValidationFn f = new FnInst(fn,name);
	fnList.add(f);
	return f;
}


} /* namespace LightCouch */

#endif /* LIBS_LIGHTCOUCH_SRC_LIGHTCOUCH_VALIDATOR_H_ */
