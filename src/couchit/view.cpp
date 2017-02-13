/*
 * view.cpp
 *
 *  Created on: 2. 4. 2016
 *      Author: ondra
 */

#include "view.h"

namespace couchit {

View::View(String viewPath)
	:viewPath(viewPath),flags(0)

{
}

View::View(String viewPath, std::size_t flags,Value args)
:viewPath(viewPath),flags(flags),args(args)

{
}

View::View(String viewPath, std::size_t flags,const Postprocessing &ppfunction, Value args)
:viewPath(viewPath),flags(flags),args(args),postprocess(ppfunction)

{
}

Filter::Filter(String filter, std::size_t flags, Value args)
	:View(filter,flags,args)
{
}

Filter::Filter(const View& view, bool allRevs)
	:View(view.viewPath, view.flags | (allRevs?Filter::allRevs:0), view.args)
{
}

Filter::Filter(String filter):View(filter) {
}


} /* namespace couchit */
