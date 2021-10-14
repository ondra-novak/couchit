/*
 * hdrwr.h
 *
 *  Created on: 10. 11. 2016
 *      Author: ondra
 */

#ifndef LIGHTCOUCH_MINIHTTP_HDRWR_H_
#define LIGHTCOUCH_MINIHTTP_HDRWR_H_


#pragma once

namespace couchit {


template<typename Fn>
class HeaderWrite {
public:

	HeaderWrite(const Fn &out):out(out) {}

	void serialize(const json::Value &v) {


		json::String method (v["_method"]);
		json::String uri (v["_uri"]);
		json::String version (v["_version"]);

		writeString(method);
		out(' ');
		writeString(uri);
		out(' ');
		writeString(version);
		writeNL();

		for (auto c:v) {

			auto key = c.getKey();
			if (key.empty() || key[0] == '_') continue;
			json::String val = c.toString();

			writeString(key);
			writeSep();
			writeString(val);
			writeNL();
		}

		writeNL();
	}


protected:

	void writeString(const std::string_view &str) {
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
