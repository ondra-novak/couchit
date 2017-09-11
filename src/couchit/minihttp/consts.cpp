/*
 * consts.cpp
 *
 *  Created on: Sep 10, 2017
 *      Author: ondra
 */

#include <imtjson/stringview.h>

#include "abstractio.h"

namespace couchit {


static unsigned char buff[1];

const json::BinaryView AbstractInputStream::eofConst(buff,0);



}

