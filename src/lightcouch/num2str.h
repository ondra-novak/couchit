#pragma once

static inline char *unsignedToStringImpl(char *str, std::size_t number, std::size_t count, bool fixedLen = true, std::size_t base = 10) {
	if (count && (fixedLen || number)) {
		str = unsignedToStringImpl(str,number/base, count-1, base);
		std::size_t p = number%base;
		if (p < 10) *str = '0' + p;
		else if (p < 36) *str = 'A'+(p-10);
		else *str = 'a' + (p - 36);
		return str+1;
	} else{
		return str;
	}
}

static inline char *unsignedToString(char *str, std::size_t number, std::size_t limit, std::size_t base = 10) {
	if (number == 0) return unsignedToStringImpl(str,number,1,true,base);
	else return unsignedToStringImpl(str,number,limit,false,base);
}
