/** @file
 * Copyright (c) 2010, Seznam.cz, a.s.
 * All Rights Reserved.
 * 
 * $Id: cfghelper.h 1608 2011-02-04 15:53:07Z ondrej.novak $
 *
 * DESCRIPTION
 * Reads config using config parser.
 * 
 * AUTHOR
 * Ondrej Novak <ondrej.novak@firma.seznam.cz>
 *
 */


#ifndef _COUCHDB_CFGHELPER_H_
#define _COUCHDB_CFGHELPER_H_

#include "couchDBPool.h"
#include <lightspeed/utils/configParser.h>


namespace LightCouch {


	void readConfig(CouchDBPool::Config &cfg, const LightSpeed::IniConfig::Section &parser);
	void readConfig(Config &cfg, const LightSpeed::IniConfig::Section &parser);


}

#endif /* _COUCHDB_CFGHELPER_H_ */
