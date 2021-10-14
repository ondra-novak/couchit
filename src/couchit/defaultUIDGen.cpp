/*
 * defaultUIDGen.cpp
 *
 *  Created on: 7. 9. 2016
 *      Author: ondra
 */



#include "defaultUIDGen.h"

namespace couchit {




	std::string_view DefaultUIDGen::operator ()(Buffer &buffer, const std::string_view &prefix) {

		Sync _(lock);
		time_t now;
		time(&now);

		counter = (counter + 1) & 0x7FFFFF;
		return generateUID(buffer, prefix, (std::size_t)now, counter, &rgn, 20);
}

static void writeBaseX(IIDGen::Buffer &buffer, std::size_t val, unsigned int digits, unsigned int base) {
	if (digits) {
		writeBaseX(buffer, val/base, digits-1, base);
		std::size_t d = val%base;
		if (d < 10) buffer.push_back('0'+d);
		else if (d < 36) buffer.push_back('A'+(d-10));
		else buffer.push_back('a'+(d-36));
	}
}

std::string_view DefaultUIDGen::generateUID(Buffer& buffer, std::string_view prefix,
		std::size_t timeparam, std::size_t counterparam, Rand* randomGen,
		std::size_t totalCount) {

	buffer.clear();
	buffer.reserve(totalCount);
	for (auto &&x: prefix) buffer.push_back(x);
	writeBaseX(buffer,  timeparam, 6, 62);
	writeBaseX(buffer,  counterparam, 4, 62);
	if (randomGen) {
		while (buffer.size() < totalCount+prefix.length()) {
			writeBaseX(buffer, (*randomGen)() % 62, 1, 62);
		}
	}

	return std::string_view(buffer.data(), buffer.size());
}

DefaultUIDGen &DefaultUIDGen::getInstance() {
	static DefaultUIDGen instance;
	return instance;
}

String DefaultUIDGen::operator()(const std::string_view &prefix) {
	Buffer buff;
	return operator()(buff, prefix);
}

DefaultUIDGen::DefaultUIDGen() {
	counter = rgn();
}

} /* namespace couchit */

