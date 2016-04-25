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

View::View(StringA viewPath, natural flags,ConstStringT<ListArg> args)
:viewPath(viewPath),flags(flags),args(args)

{
}

Filter::Filter(StringA filter, natural flags, ConstStringT<ListArg> args)
	:View(filter,flags,args)
{
}

Filter::Filter(const View& view, bool allConflicts)
	:View(view.viewPath, view.flags | (allConflicts?Filter::allConflicts:0), view.args)
{
}

Filter::Filter(StringA filter):View(filter) {
}

View View::addArg(ConstStringT<ListArg> args) const {
	return View(viewPath, flags, StringCore<ListArg>(this->args + args));
}

View View::setArgs(ConstStringT<ListArg> args) const {
	return View(viewPath, flags, args);
}

View View::setFlags(natural flags) const {
	return View(viewPath, flags, args);
}

View View::setPath(StringA path) const {
	return View(path, flags, args);
}

Filter Filter::addArg(ConstStringT<ListArg> args) const {
	return Filter(viewPath, flags, StringCore<ListArg>(this->args + args));
}

Filter Filter::setArgs(ConstStringT<ListArg> args) const {
	return Filter(viewPath, flags, args);
}

Filter Filter::setFlags(natural flags) const {
	return Filter(viewPath, flags, args);
}

Filter Filter::setPath(StringA path) const {
	return Filter(path, flags, args);
}

} /* namespace LightCouch */
