/*
 * hdrrd.h
 *
 *  Created on: 10. 11. 2016
 *      Author: ondra
 */

#ifndef LIGHTCOUCH_MINIHTTP_HDRRD_H_
#define LIGHTCOUCH_MINIHTTP_HDRRD_H_


#include <lightspeed/base/types.h>
#include "../json.h"

namespace LightCouch {


template<typename Fn>
class HeaderRead {
public:

	HeaderRead(const Fn &input):input(input) {}

	json::Value parseHeaders();

protected:

	Fn input;
	std::vector<char> linebuff;


	StrView readLine();
	StrView crop(const StrView &str);


};

template<typename Fn>
inline json::Value LightCouch::HeaderRead<Fn>::parseHeaders() {

	using namespace LightSpeed;
	json::Object collect;
	do {
		StrView line = readLine();
		if (line.empty()) break;

		natural pos = line.find(':');
		if (pos != naturalNull) {
			StrView field = line.substr(0,pos);
			StrView value = line.substr(pos+1);
			StrView cfield = crop(field);
			StrView cvalue = crop(value);
			collect.set(cfield,cvalue);
		} else if (line.substr(0,6) == "HTTP/1") {
				String strline(line);
				Value starr = strline.split(" ",3);
				int pos = 0;
				starr = starr.map([&](const String &v) -> Value {
					if (v.empty())
						return Value(json::undefined);
					pos++;
					if (pos == 2)
						try {
							return Value::fromString(v);
						} catch (...) {
							return v;
						}
					else
						return v;
				});
				collect("_version",starr[0]);
				collect("_status",starr[1]);
				collect("_message",starr[2]);
		} else {
			return Value();
		}
	} while (true);
	return collect;

}

template<typename Fn>
inline StrView HeaderRead<Fn>::readLine() {
	linebuff.clear();
	int i = input();
	while (i != -1) {
		linebuff.push_back((char)i);
		StrView l(linebuff);
		if (StrView(l.tail(2)) == StrView("\r\n")) return l.crop(0,2);
		i = input();
	}
	return StrView();
}

template<typename Fn>
inline StrView HeaderRead<Fn>::crop(const StrView &str) {
	std::size_t p1 = 0;
	while (p1<str.length() && isspace(str[p1])) p1++;
	std::size_t p2 = str.length();
	while (p2>0 && isspace(str[p2-1])) p2--;
	return str.substr(p1,p2-p1);

}


}


#endif /* LIGHTCOUCH_MINIHTTP_HDRRD_H_ */
