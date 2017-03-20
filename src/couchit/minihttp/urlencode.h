/*
 * urlencode.h
 *
 *  Created on: 9. 2. 2017
 *      Author: ondra
 */

#pragma once


class UrlEncoder {
public:

	template<typename Input, typename Output>
	void operator()(Input input, Output output) {
		int c = input();
		while (c != json::eof) {
			if ((c >='A' && c <='Z') || (c >= 'a' && c <= 'z')
						|| (c >='0' && c <='9') || c == '_' || c == '-') {
					output(c);
			} else {
				char hex[] = "0123456789ABCDEF";
				output('%');
				output(hex[(c >> 4) & 0xF]);
				output(hex[c & 0xF]);
			}
			c = input();
		}
	}
};


