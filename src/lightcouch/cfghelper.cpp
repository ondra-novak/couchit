/*
 * cfgparser.cpp
 *
 *  Created on: Sep 24, 2012
 *      Author: ondra
 */

#include "cfghelper.h"
#include <lightspeed/utils/configParser.tcc>



void LightCouch::readConfig(LightCouch::CouchDBPool::Config& cfg,
		const LightSpeed::IniConfig::Section& parser) {

	LightCouch::Config &bcfg = cfg;
	readConfig(bcfg,parser);
	parser.required(cfg.limit,"maxConnections");
	parser.required(cfg.waitTimeout,"waitTimeout");
	parser.required(cfg.resTimeout,"resTimeout");

}

void LightCouch::readConfig(LightCouch::Config& cfg,const LightSpeed::IniConfig::Section& parser) {
	StringA str;
	parser.required(str,"url");
	cfg.baseUrl = StringRef(str);
	parser.required(str,"dbname");
	cfg.databaseName = StringRef(str);
	LightSpeed::natural iotimeout;
	if (parser.get(iotimeout,"iotimeout")) {
		cfg.iotimeout = iotimeout;
	}
}


