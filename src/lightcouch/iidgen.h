/*
 * iidgen.h
 *
 *  Created on: 7. 9. 2016
 *      Author: ondra
 */

#ifndef LIGHTCOUCH_IIDGEN_H_
#define LIGHTCOUCH_IIDGEN_H_
#include <lightspeed/base/containers/constStr.h>
#include "lightspeed/base/containers/autoArray.h"
#include "json.h"


namespace LightCouch {

using namespace LightSpeed;

	///custom UID generator
	/** You can define own UID generator, which can be referenced in CouchDB configuration structure
	 * Note that generator must be MT safe, because it fill be shared between many instances
	 */
	class IIDGen{
	public:
		virtual ~IIDGen() {};

		///Generates new ID
		/**
		 * @param buffer a buffer supplied by the CouchDB connection instance. The
		 *  buffer should be used to store result or internal state of the generator and
		 *  it persists between each call
		 *  @param prefix allows specify prefix - it is used to encode type of the object
		 * @return const-string contains newly generated UID. It can be created inside of the buffer.
		 */
		virtual StrViewA operator()(AutoArray<char> &buffer, StrViewA prefix) = 0;
	};


}


#endif /* LIGHTCOUCH_IIDGEN_H_ */
