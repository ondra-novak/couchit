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

namespace LightCouch {

using json::Value;
using json::Path;
using json::Object;
using json::Array;

template<typename T>
class StringRefT: public json::StringView<T> {
public:
	StringRefT()  {}
	StringRefT(const T *str) :json::StringView<T>(str) {}
	StringRefT(const T *str, std::size_t length):json::StringView<T>(str,length) {}
	StringRefT(const std::basic_string<T> &string):json::StringView<T>(string) {}
	StringRefT(const std::vector<T> &string) :json::StringView<T>(string) {}
	StringRefT(const LightSpeed::ConstStringT<T> &str):json::StringView<T>(str.data(), str.length()) {}

	operator LightSpeed::ConstStringT<T>() const {return LightSpeed::ConstStringT<T>(this->data, this->length);}
};

typedef StringRefT<char> StringRef;
typedef StringRefT<unsigned char> BinaryRef;
typedef json::String String;

}




#endif /* LIGHTCOUCH_JSON_H_ */
