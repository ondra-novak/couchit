/*
 * json.h
 *
 *  Created on: 14. 10. 2016
 *      Author: ondra
 */

#ifndef LIGHTCOUCH_JSON_H_
#define LIGHTCOUCH_JSON_H_

#include <immujson/edit.h>
#include <immujson/path.h>
#include <immujson/string.h>
#include <lightspeed/base/containers/flatArray.h>
#include <lightspeed/base/text/textOut.h>

namespace LightCouch {

using json::Value;
using json::Path;
using json::Object;
using json::Array;


template<typename T>
class StringRefT: public json::StringView<T> {
public:
	StringRefT()  {}
	StringRefT(const json::StringView<T> &other):json::StringView<T>(other) {}
	StringRefT(const T *str) :json::StringView<T>(str) {}
	StringRefT(const T *str, std::size_t length):json::StringView<T>(str,length) {}
	StringRefT(const std::basic_string<T> &string):json::StringView<T>(string) {}
	StringRefT(const std::vector<T> &string) :json::StringView<T>(string) {}
	template<typename Impl>
	StringRefT(const LightSpeed::FlatArray<T, Impl> &str):json::StringView<T>(str.data(), str.length()) {}
	template<typename Impl>
	StringRefT(const LightSpeed::FlatArray<const T, Impl> &str):json::StringView<T>(str.data(), str.length()) {}
	operator LightSpeed::ConstStringT<T>() const {return LightSpeed::ConstStringT<T>(this->data, this->length);}

	typedef typename LightSpeed::ConstStringT<T>::Iterator Iterator;
	typedef typename LightSpeed::ConstStringT<T>::SplitIterator SplitIterator;

	Iterator getFwIter() const {return LightSpeed::ConstStringT<T>(*this).getFwIter();}

	explicit operator std::basic_string<T>() const;
};

template<>
class StringRefT<char>: public json::StringView<char> {
public:
	StringRefT()  {}
	StringRefT(const json::String &other):json::StringView<char>(other.operator json::StringView<char>()) {}
	StringRefT(const json::StringView<char> &other):json::StringView<char>(other) {}
	StringRefT(const char *str) :json::StringView<char>(str) {}
	StringRefT(const char *str, std::size_t length):json::StringView<char>(str,length) {}
	StringRefT(const std::basic_string<char> &string):json::StringView<char>(string) {}
	StringRefT(const std::vector<char> &string) :json::StringView<char>(string) {}
	template<typename Impl>
	StringRefT(const LightSpeed::FlatArray<char,Impl> &str):json::StringView<char>(str.data(), str.length()) {}
	template<typename Impl>
	StringRefT(const LightSpeed::FlatArray<const char,Impl> &str):json::StringView<char>(str.data(), str.length()) {}
	operator LightSpeed::ConstStringT<char>() const {return LightSpeed::ConstStringT<char>(this->data, this->length);}

	typedef LightSpeed::ConstStringT<char>::Iterator Iterator;
	typedef typename LightSpeed::ConstStringT<char>::SplitIterator SplitIterator;

	Iterator getFwIter() const {return LightSpeed::ConstStringT<char>(*this).getFwIter();}
	SplitIterator split(char c) const {return LightSpeed::ConstStringT<char>(*this).split(c);}

	explicit operator std::basic_string<char>() const;
};

typedef StringRefT<char> StringRef;
typedef StringRefT<unsigned char> BinaryRef;
typedef json::String String;


}




#endif /* LIGHTCOUCH_JSON_H_ */
