/*
 * localViewQS.h
 *
 *  Created on: 16. 11. 2016
 *      Author: ondra
 */

#ifndef LIGHTCOUCH_LOCALVIEWQS_H_
#define LIGHTCOUCH_LOCALVIEWQS_H_
#include "localView.h"
#include "queryServerIfc.h"


namespace LightCouch {


///LocalView which is able to use QueryServer's view definition to perform same function localy
class LocalViewQS: public LocalView {
public:


	///Construct local view from queryServer's view
	/**
	 * @param view pointer to the view definition. The object acquires ownership of the view and deletes it during destruction
	 * @param includeDocs set true, to include whole documents to the view. Otherwise, only IDs will be stored
	 */
	LocalViewQS(AbstractViewBase *view, bool includeDocs);


	virtual void map(const Document &doc) override ;
	virtual Value reduce(const RowsWithKeys &items) const override;
	virtual Value rereduce(const ReducedRows &items) const override;

protected:

	class EmitFn: public IEmitFn {
	public:
		EmitFn(LocalViewQS &owner):owner(owner) {}
		virtual void operator()() override;
		virtual void operator()(const Value &key) override ;
		virtual void operator()(const Value &key, const Value &value) override;
	protected:
		LocalViewQS &owner;
	};
	class EmitFnNoDocs: public IEmitFn {
	public:
		EmitFnNoDocs(LocalViewQS &owner):owner(owner) {}
		virtual void operator()() override;
		virtual void operator()(const Value &key) override ;
		virtual void operator()(const Value &key, const Value &value) override;
	protected:
		LocalViewQS &owner;
	};

	EmitFn emitFn;
	EmitFnNoDocs emitFnNoDocs;
	IEmitFn &curEmitFn;

	AllocPointer<AbstractViewBase> view;

};



}


#endif /* LIGHTCOUCH_LOCALVIEWQS_H_ */
