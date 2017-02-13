/*
 * fnv.h
 *
 *  Created on: 27. 10. 2016
 *      Author: ondra
 */

#ifndef LIGHTCOUCH_FNV_H_
#define LIGHTCOUCH_FNV_H_

#include <cstdint>

template<int bits>
class FNV1a;


template<>
class FNV1a<4> {
public:
	template<typename Fn>
	static std::uint32_t hash(const Fn &fn);
};

template<>
class FNV1a<8> {
public:
	template<typename Fn>
	static std::uint64_t hash(const Fn &fn);
};


template<typename Fn, typename Type, Type offset, Type prime>
inline Type runFNV1aCalc(Fn fn) {
	Type acc = offset;
	int c = fn();
	while (c != -1) {
		unsigned char b = (unsigned char)c;
		acc = acc ^ b;
		acc = acc * prime;
		c = fn();
	}
	return acc;
}


template<typename Fn>
inline std::uint32_t FNV1a<4>::hash(const Fn &fn)
{
	return runFNV1aCalc<Fn,std::uint32_t, 2166136261, 16777619>(fn);
}

template<typename Fn>
inline std::uint64_t FNV1a<8>::hash(const Fn &fn)
{
	return runFNV1aCalc<Fn,std::uint64_t, 14695981039346656037UL, 1099511628211UL>(fn);
}


#endif /* LIGHTCOUCH_FNV_H_ */
