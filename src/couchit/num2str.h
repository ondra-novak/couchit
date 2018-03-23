#pragma once

#include <cstddef>

template<typename Output>
static inline std::size_t unsignedToStringImpl(Output out, std::size_t number, std::size_t count, bool fixedLen = true, std::size_t base = 10) {
	if (count!=0 && (fixedLen || number != 0)) {
		std::size_t r = unsignedToStringImpl<Output>(out,number/base, count-1, fixedLen, base);
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
	if (number == 0) return unsignedToStringImpl<Output>(out,number,1,true,base);
	else return unsignedToStringImpl<Output>(out,number,limit,false,base);
}

class BufferOutput {
public:
	BufferOutput(char *p):p(p) {}
	void operator()(char c) { *p++ = c;}
protected:
	char *p;
};

inline BufferOutput outputToBuffer(char *p) {return BufferOutput(p);}
