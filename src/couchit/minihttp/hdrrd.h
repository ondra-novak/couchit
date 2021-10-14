/*
 * hdrrd.h
 *
 *  Created on: 10. 11. 2016
 *      Author: ondra
 */

#ifndef LIGHTCOUCH_MINIHTTP_HDRRD_H_
#define LIGHTCOUCH_MINIHTTP_HDRRD_H_


#include "../json.h"

namespace couchit {


template<typename Fn>
class HeaderRead {
public:

	HeaderRead(const Fn &input):input(input) {}

	json::Value parseHeaders();

protected:

	Fn input;
	std::string linebuff;


	std::string_view readLine();
	std::string_view trim(const std::string_view &str);


};

template<typename Fn>
inline json::Value couchit::HeaderRead<Fn>::parseHeaders() {

	json::Object collect;
	bool stline = false;
	do {
		std::string_view line = trim(readLine());
		if (line.empty()) {
			if (stline) break;
			else continue;
		}

		std::size_t pos = line.find(':');
		if (pos != line.npos) {
			std::string_view field = line.substr(0,pos);
			std::string_view value = line.substr(pos+1);
			std::string_view cfield = trim(field);
			std::string_view cvalue = trim(value);
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
				collect.set("_version",starr[0]);
				collect.set("_status",starr[1]);
				collect.set("_message",starr[2]);
		} else {
			return Value();
		}
	} while (true);
	return collect;

}

template<typename Fn>
inline std::string_view HeaderRead<Fn>::readLine() {
	linebuff.clear();
	int i = input();
	while (i != -1) {
		linebuff.push_back((char)i);
		std::string_view l(linebuff);
		if (l.substr(l.length()-2,2) == "\r\n")
			return l.substr(0,l.length()-2);
		i = input();
	}
	return std::string_view("EOF");
}

template<typename Fn>
inline std::string_view HeaderRead<Fn>::trim(const std::string_view &str) {
	std::size_t p1 = 0;
	while (p1<str.length() && isspace(str[p1])) p1++;
	std::size_t p2 = str.length();
	while (p2>0 && isspace(str[p2-1])) p2--;
	return str.substr(p1,p2-p1);

}


}


#endif /* LIGHTCOUCH_MINIHTTP_HDRRD_H_ */
