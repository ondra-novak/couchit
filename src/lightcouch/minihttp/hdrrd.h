/*
 * hdrrd.h
 *
 *  Created on: 10. 11. 2016
 *      Author: ondra
 */

#ifndef LIGHTCOUCH_MINIHTTP_HDRRD_H_
#define LIGHTCOUCH_MINIHTTP_HDRRD_H_


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


	StrViewA readLine();
	StrViewA trim(const StrViewA &str);


};

template<typename Fn>
inline json::Value LightCouch::HeaderRead<Fn>::parseHeaders() {

	json::Object collect;
	bool stline = false;
	do {
		StrViewA line = trim(readLine());
		if (line.empty()) {
			if (stline) break;
			else continue;
		}

		std::size_t pos = line.indexOf(":",0);
		if (pos != line.npos) {
			StrViewA field = line.substr(0,pos);
			StrViewA value = line.substr(pos+1);
			StrViewA cfield = trim(field);
			StrViewA cvalue = trim(value);
			collect.set(cfield,cvalue);
		} else if (line.substr(0,6) == "HTTP/1" && !stline) {
				stline = true;
				String strline(line);
				Value starr = strline.split(" ",3);
				int pos = 0;
				starr = starr.map([&](const Value &v) -> Value {
					if (v.getString().empty())
						return Value(json::undefined);
					pos++;
					if (pos == 2)
						try {
							return Value::fromString(v.getString());
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
inline StrViewA HeaderRead<Fn>::readLine() {
	linebuff.clear();
	int i = input();
	while (i != -1) {
		linebuff.push_back((char)i);
		StrViewA l(linebuff);
		if (StrViewA(l.substr(l.length-2,2)) == StrViewA("\r\n"))
			return l.substr(0,l.length-2);
		i = input();
	}
	return StrViewA("EOF");
}

template<typename Fn>
inline StrViewA HeaderRead<Fn>::trim(const StrViewA &str) {
	std::size_t p1 = 0;
	while (p1<str.length && isspace(str[p1])) p1++;
	std::size_t p2 = str.length;
	while (p2>0 && isspace(str[p2-1])) p2--;
	return str.substr(p1,p2-p1);

}


}


#endif /* LIGHTCOUCH_MINIHTTP_HDRRD_H_ */
