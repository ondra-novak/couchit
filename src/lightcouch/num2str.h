#pragma once

template<typename Output>
static inline std::size_t unsignedToStringImpl(Output out, std::size_t number, std::size_t count, bool fixedLen = true, std::size_t base = 10) {
	if (count && (fixedLen || number)) {
		std::size_t r = unsignedToStringImpl(out,number/base, count-1, base);
		std::size_t p = number%base;
		if (p < 10) out((char)('0' + p));
		else if (p < 36) out((char)('A'+(p-10)));
		else  out((char)('a' + (p - 36)));
		return r+1;
	} else{
		return 0;
	}
}

template<typename Output>
static inline std::size_t unsignedToString(Output out, std::size_t number, std::size_t limit, std::size_t base = 10) {
	if (number == 0) return unsignedToStringImpl(out,number,1,true,base);
	else return unsignedToStringImpl(out,number,limit,false,base);
}
