/*
 * defaultUIDGen.h
 *
 *  Created on: 7. 9. 2016
 *      Author: ondra
 */

#ifndef LIGHTCOUCH_DEFAULTUIDGEN_H_
#define LIGHTCOUCH_DEFAULTUIDGEN_H_

#include <lightspeed/base/streams/random.h>
#include <lightspeed/mt/fastlock.h>
#include "iidgen.h"

using LightSpeed::Rand;

namespace LightCouch {

///Default UID generator
/** The UID consists from three parts
 * - a timestamp (in seconds) - 6 characters
 * - a counter - 4 characters
 * - a random padding - 10 characters
 *
 *  Example: I3kd9ZZjdmeprPOk92kd
 *
 *  Above example consists
 *  - "I3kd9Z" timestamp (base 62),
 *  - "Zjdm" counter (base 62)
 *  - "eprPOk92kd" random (62 charset)
 *
 *  The timestamp contains time of object creation. The counter increments for every new object. This allows to
 *  create up 8mil objects per second without possibility of collision.
 *
 *  The random part reduces possible collision in replicated network, when two or more server nodes can
 *  create objects without connection to each other.
 *
 *  You can put prefix before the ID, for eaxmple "user-I3kd9ZZjdmeprPOk92kd". This defines the ID of
 *  an anonymous user
 *
 */
class DefaultUIDGen: public IIDGen {
public:
	DefaultUIDGen();
	virtual StrViewA operator()(AutoArray<char> &buffer, StrViewA prefix);

	///Generates UID statically
	/**
	 * @param buffer buffer where UID will be put
	 * @param prefix prefix put before result
	 * @param timeparam argument represents the time - recomended 35 bit
	 * @param counterparam argument represents the counter - recommended 23 bits
	 * @param randomGen random generator (can be NULL)
	 * @param totalCount specifies total length of UID. Function will calculate padding to
	 *   match requested length. It requires to have randomGen not null
	 * @return result is string reference to the buffer
	 */
	static StrViewA generateUID(AutoArray<char> &buffer,
			StrViewA prefix,
			natural timeparam, natural counterparam,
			Rand *randomGen, natural totalCount=20);

	static DefaultUIDGen &getInstance();

	virtual String operator()(StrViewA prefix);

protected:
	natural counter;
	Rand rgn;
	FastLock lock;



};

} /* namespace LightCouch */

#endif /* LIGHTCOUCH_DEFAULTUIDGEN_H_ */
