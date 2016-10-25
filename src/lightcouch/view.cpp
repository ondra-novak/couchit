/*
 * view.cpp
 *
 *  Created on: 2. 4. 2016
 *      Author: ondra
 */

#include "view.h"

namespace LightCouch {

View::View(StringA viewPath)
	:viewPath(viewPath),flags(0)

{
}

View::View(StringA viewPath, natural flags,Value args)
:viewPath(viewPath),flags(flags),args(args)

{
}

View::View(StringA viewPath, natural flags,const Postprocessing &ppfunction, Value args)
:viewPath(viewPath),flags(flags),args(args),postprocess(ppfunction)

{
}

Filter::Filter(StringA filter, natural flags, Value args)
	:View(filter,flags,args)
{
}

Filter::Filter(const View& view, bool allRevs)
	:View(view.viewPath, view.flags | (allRevs?Filter::allRevs:0), view.args)
{
}

Filter::Filter(StringA filter):View(filter) {
}


} /* namespace LightCouch */
