/*
 * common.h
 *
 *  Created on: 10. 3. 2016
 *      Author: ondra
 */

#ifndef LIBS_LIGHTCOUCH_SRC_TESTS_TEST_COMMON_H_
#define LIBS_LIGHTCOUCH_SRC_TESTS_TEST_COMMON_H_
#include "../couchit/couchDB.h"

namespace couchit {

Config getTestCouch();


json::String UIntToStr(std::size_t id, int base);
}




#endif /* LIBS_LIGHTCOUCH_SRC_TESTS_TEST_COMMON_H_ */
