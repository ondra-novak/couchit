/*
 * hdrwr.h
 *
 *  Created on: 10. 11. 2016
 *      Author: ondra
 */

#ifndef LIGHTCOUCH_MINIHTTP_HDRWR_H_
#define LIGHTCOUCH_MINIHTTP_HDRWR_H_


#pragma once

namespace LightCouch {


template<typename Fn>
class HeaderWrite {
public:

	HeaderWrite(const Fn &out):out(out) {}

	void serialize(const json::Value &v) {

		for (auto &&c:v) {

			json::StringView<char> key = v.getKey();
			json::String val = v.toString();

			writeString(key);
			writeSep();
			writeString(val);
			writeNL();
		}

		writeNL();
	}


protected:

	void writeString(const json::StringView<char> str) {
		for (auto &&c:str) {
			if (c < 32) out('.');
			else out(c);
		}
	}

	void writeNL() {
		out('\r');
		out('\n');
	}

	void writeSep() {
		out(':');
		out(' ');
	}

	Fn out;

};



}


#endif /* LIGHTCOUCH_MINIHTTP_HDRWR_H_ */
