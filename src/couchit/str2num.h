/*
 * str2num.h
 *
 *  Created on: 28. 2. 2017
 *      Author: ondra
 */

#pragma once
#include <imtjson/stringview.h>
#include <cstddef>

///Converts string to number
/**
 * @param str string to convert
 * @param base number base
 * @return converted string, or StrViewA::npos if conversion fails
 */
inline std::size_t stringToUnsigned(json::StrViewA str, std::size_t base = 10) {
	std::size_t accum = 0;
	for (char c : str) {
		std::size_t d;
		if (c>='0' && c <='9') {
			d = c - '0';
		} else if (c >= 'A' && c <= 'Z') {
			d = c - 'A'+10;
		} else if (c >= 'a' && c <= 'z') {
			if (base <36 ) d = c - 'a' + 10;
			else d = c - 'a' + 36;
		} else {
			return json::StrViewA::npos;
		}
		if (d >= base)
			return json::StrViewA::npos;
		accum = accum * base + d;
	}
}




