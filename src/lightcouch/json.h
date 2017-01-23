/*
 * json.h
 *
 *  Created on: 14. 10. 2016
 *      Author: ondra
 */

#ifndef LIGHTCOUCH_JSON_H_
#define LIGHTCOUCH_JSON_H_

#include <imtjson/json.h>
#include <lightspeed/base/containers/flatArray.h>
#include <lightspeed/base/containers/stringBase.h>
#include <lightspeed/base/text/textOut.h>
#include <lightspeed/base/text/toString.h>

namespace LightCouch {


typedef json::Value Value;
typedef json::String String;
typedef json::Object Object;
typedef json::Array Array;
typedef json::Path Path;
using json::StringView;


///Contains string compatible with json::StringView and also LightSpeed::FlatArray which can be used as ConstStrA
/** class StrView augments json::StringView with operations from LightSpeed including iterators.
 * It also makes string acceptable by many functions which accepts ConstStrA
 */
class StrView: public json::StringView<char>, public LightSpeed::FlatArrayBase<const char, StrView> {
public:

	///main super class is json::StringView
	typedef json::StringView<char> Super;
	using Super::empty;
	using Super::operator[];
	using Super::StringView;

	StrView() {}
	StrView(const json::String &x):Super(x) {}
	StrView(const StringView<char> &other):json::StringView<char>(other) {}
	template<typename Impl>
	StrView(const LightSpeed::FlatArray<char, Impl> &other)
		:json::StringView<char>(other.data(),other.length()) {}
	template<typename Impl>
	StrView(const LightSpeed::FlatArray<const char, Impl> &other)
		:json::StringView<char>(other.data(),other.length()) {}

	explicit operator std::string () const {
		return std::string(Super::data, Super::length);
	}
//	operator json::String() const {return json::String(*this);}
	const char *data() const {return Super::data;}
	LightSpeed::natural length() const {return Super::length;}
};




template<typename T>
static inline json::StringView<T> convStr(const LightSpeed::ConstStringT<T> &x) {
	return json::StringView<T>(x.data(),x.length());
}
template<typename T>
static inline json::StringView<T> convStr(const LightSpeed::StringCore<T> &x) {
	return json::StringView<T>(x.data(),x.length());
}
template<typename T>
static inline LightSpeed::ConstStringT<T> convStr(const json::StringView<T> &x) {
	return LightSpeed::ConstStringT<T>(x.data,x.length);
}

static inline LightSpeed::ConstStringT<char> convStr(const json::String&x) {
	json::StringView<char> y = x;
	return convStr(y);
}

static inline Value addToArray(Value v, Value add) {
	Array c(v);
	if (c.empty()) c.add(v);
	c.add(add);
	return c;
}

static inline Value addSuffix(Value v,const String &suffix) {
		if (!v.empty()) {
			Array x(v);
			std::size_t sz = v.size()-1;
			x.trunc(sz);
			x.add(v[sz].toString()+suffix);
			return x;
		} else {
			return v.toString() + suffix;
		}
	}

class UIntToStr: public LightSpeed::ToString<LightSpeed::natural>, public json::StringView<char> {
public:
	typedef LightSpeed::ToString<LightSpeed::natural> Super;
	UIntToStr(LightSpeed::natural x, LightSpeed::natural base=10, bool up = false)
		:LightSpeed::ToString<LightSpeed::natural>(x,base,up)
		 ,json::StringView<char>(this->refdata, this->len) {}
};

}



#endif /* LIGHTCOUCH_JSON_H_ */

