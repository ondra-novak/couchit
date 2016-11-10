/*
 * hdrrd.h
 *
 *  Created on: 10. 11. 2016
 *      Author: ondra
 */

#ifndef LIGHTCOUCH_MINIHTTP_HDRRD_H_
#define LIGHTCOUCH_MINIHTTP_HDRRD_H_


namespace LightCouch {


template<typename Fn>
class HeaderRead {
public:

	HeaderRead(const Fn &input):input(input) {}

	json::Value parseHeaders();

protected:

	Fn input;
	std::vector<char> linebuff;



};

template<typename Fn>
inline json::Value LightCouch::HeaderRead<Fn>::parseHeaders() {

	json::Object collect;
	StrView line = readLine();
	while (!line.empty()) {

		natural pos = line.find(':');
		if (pos != naturalNull) {
			StrView field = line.substr(0,pos);
			StrView value = line.substr(pos+1);
			field = crop(field);
			value = crop(value);
			collect.set(field,value);
		}


	}


}



}


#endif /* LIGHTCOUCH_MINIHTTP_HDRRD_H_ */
