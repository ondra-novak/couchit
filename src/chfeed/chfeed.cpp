/*
 * chfeed.cpp
 *
 *  Created on: 23. 12. 2019
 *      Author: ondra
 *
 *
 *  A tool which monitors database and sends results to std output. Similar to curl to changes
 *  feed, but this is safer and allows more customizations
 *
 *  When tool it started, it expects JSON on stdin with configuration. Once the configuration
 *  is supplied, it starts to monitor the database. It doesn't stop when connection is temporary
 *  lost, and continues after reconnect
 *
 *  Configuration
 *  {
 *  "dburl": "<database url>",
 *  "dbname":"<database name>",
 *  "auth": {
 *  		"login":"<username>",
 *  		"password":"<password>"
 *  		},
 *  "statfile":"<file wher last seqid is stored>",
 *  "since":"<seqid if statfile doesnt exists>",
 *  "include_docs":true/false,
 *  "filter":<filter>,
 *  "query":<filter query>,
 *  "output":[
 *			{
 *			 "path":"...path....",
 *			 "encoding":"text/url/base64/base64url",
 *			 "prefix":"text...",
 *			 "suffix":"text..."
 *			 }, ... ]
 *  }
 */
#include <fcntl.h>
#include <unistd.h>
#include <csignal>
#include <exception>
#include <fstream>
#include <thread>
#include <sys/file.h>

#include <imtjson/value.h>
#include <imtjson/object.h>
#include "../couchit/changeevent.h"
#include "../couchit/changes.h"
#include "../couchit/couchDB.h"
#include "../couchit/view.h"
#include "../imtjson/src/imtjson/object.h"
#include "../shared/stringview.h"

using couchit::CouchDB;
using couchit::ChangeEvent;
using json::Value;
using json::Object;
using ondra_shared::BinaryView;
using ondra_shared::StrViewA;

static Value loadConfig(std::string fname) {
	std::ifstream in(fname);
	if (!in) std::runtime_error("Unable to open configuration file");
	return Value::fromStream(in);
}

static Value getStat(const std::string &fname, Value defStat) {
	if (fname.empty()) return defStat;

	std::ifstream in(fname);
	if (!in) return defStat;

	return Value::fromStream(in)["since"];
}

static int lockStatFile(const std::string &fname) {
	if (fname.empty()) return 0;
	int fd = ::open(fname.c_str(), O_WRONLY|O_CREAT);
	if (flock(fd,LOCK_EX|LOCK_NB)) {
		::close(fd);
		throw std::runtime_error("Unable to lock status file");
	}
	return fd;
}

static void setStat(int fd, Value stat) {
	if (!fd) return;

	Value s = Object("since", stat);

	lseek(fd,0,SEEK_SET);
	ftruncate(fd,0);
	auto data = s.stringify();
	::write(fd,data.c_str(),data.length());
	ftruncate(fd, lseek(fd,0,SEEK_CUR));
	fsync(fd);
}

static couchit::Config loadDBConfig(Value js) {
	couchit::Config cfg;
	Value jauth = js["auth"];
	if (jauth.hasValue()) {
		couchit::AuthInfo auth;
		auth.password = jauth["password"].getString();
		auth.username = jauth["login"].getString();
		cfg.authInfo = auth;
	}
	cfg.baseUrl = js["dburl"].getString();
	cfg.databaseName = js["dbname"].getString();
	return cfg;
}

static void processChange(Value ev, Value cfg) {
	Value output = cfg["output"];
	if (output.type() == json::array) {
		for (Value item: output) {
			Value path = item["path"];
			Value root = ev;
			if (path.hasValue()) {
				auto ppath = json::PPath::fromValue(path);
				root = root[ppath];
			}
			StrViewA encoding = item["encoding"].getValueOrDefault("json");
			StrViewA prefix = item["prefix"].getString();
			StrViewA suffix = item["suffix"].getString();
			std::cout<<prefix;
			if (encoding == "text") std::cout << root.toString();
			else if (encoding == "url") std::cout << json::urlEncoding->encodeBinaryValue(
					BinaryView(root.toString().str())).toString();
			else if (encoding == "base64") std::cout << json::base64->encodeBinaryValue(
					BinaryView(root.toString().str())).toString();
			else if (encoding == "base64url") std::cout << json::base64url->encodeBinaryValue(
					BinaryView(root.toString().str())).toString();
			else {
				root.toStream(std::cout);
			}
			std::cout << suffix;
			std::cout << std::endl;
		}
	} else {
		ev.toStream(std::cout);
		std::cout << std::endl;
	}
}

std::function<void()> cancelFn;

static void markSignal(int) {
	if (cancelFn != nullptr) cancelFn();
}

int main(int argc, char **argv) {

	if (argc < 2) {
		std::cerr << "Needs argument - config pathname" << std::endl;
		return 1;
	}
	try {

		Value cfg = loadConfig(argv[1]);
		std::string statfile = cfg["statfile"].getString();
		Value since = getStat(statfile,cfg["since"]);
		int fd = lockStatFile(statfile);


		auto dbcfg = loadDBConfig(cfg);
		CouchDB db(dbcfg);

		auto feed = db.createChangesFeed();
		feed.since(since);
		feed.includeDocs(cfg["include_docs"].getBool());
		Value flt = cfg["filter"];
		if (flt.hasValue()) {
			feed.setFilter(couchit::Filter(flt.getString()));
			Value query = cfg["query"];
			if (query.type() == json::object) {
				for (Value v: query) {
					feed.arg(v.getKey(),v);
				}
			}
		}

		bool nosign = true;
		cancelFn = [&]{
			nosign = false;
			feed.cancelWait();
		};

		signal(SIGTERM, &markSignal);
		signal(SIGHUP, &markSignal);
		signal(SIGINT, &markSignal);

		feed.setTimeout(50000);
		while (nosign) {
			try {
				auto chg = feed.exec();
				for (Value ev: chg) {
					processChange(ev, cfg);
				}
				setStat(fd,feed.getLastSeq());
			} catch (std::exception &e) {
				std::cerr << "Read feed exception: " << e.what() << std::endl;
				std::this_thread::sleep_for(std::chrono::seconds(10));
			}
		}


		close(fd);
		return 0;

	} catch (std::exception &e) {
		std::cerr << "ERROR: " << e.what()<< std::endl;
		return 253;
	}



}


