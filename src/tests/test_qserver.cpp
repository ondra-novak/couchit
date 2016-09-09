/*
 * test_basics.cpp
 *
 *  Created on: 19. 3. 2016
 *      Author: ondra
 */
#include "../lightcouch/attachment.h"
#include "../lightcouch/document.h"
#include "../lightcouch/queryCache.h"
#include "../lightcouch/changeset.h"
#include <lightspeed/base/text/textstream.tcc>
#include "../lightcouch/couchDB.h"
#include "../lightcouch/query.h"
#include "../lightcouch/changedDoc.h"
#include "../lightcouch/queryServer.tcc"
#include "lightspeed/base/framework/testapp.h"

#include "test_common.h"

#include "lightspeed/base/containers/autoArray.tcc"

#include "lightspeed/base/containers/set.tcc"

#include "lightspeed/base/countof.h"

#include "lightspeed/mt/thread.h"
namespace LightCouch {
using namespace LightSpeed;
using namespace BredyHttpClient;

#define DATABASENAME "lightcouch_unittest"



static void prepareQueryServer(QueryServer &qserver) {
	Json &json = qserver.json;
	qserver.regMapFn("testview/by_name", 1, [&](const Document &doc,  QueryServer::IEmitFn &emit) {
		emit( (json,doc["name"]), (json,doc["age"],doc["height"]));
	});
	qserver.regMapFn("testview/by_age", 1, [&](const Document &doc,  QueryServer::IEmitFn &emit) {
			emit(doc["age"], doc["name"]);
	});
	qserver.regMapFn("testview/by_age_group", 1, [&](const Document &doc,  QueryServer::IEmitFn &emit) {
		emit( (json, doc["age"].getUInt()/10, doc["age"]), doc["name"]);
	});
	qserver.regMapReduceFn("testview/age_group_height", 1,
			[&](const Document &doc,  QueryServer::IEmitFn &emit) {
					emit( (json,doc["age"].getUInt()/10,doc["age"]), doc["height"]);
			},
			[&](const ConstValue &kvlist) {
				return json(kvlist.length());
			},
			[&](const ConstValue &values) {
				return json(values.length());
	});
}


static void rawCreateDB(PrintTextA &) {
	CouchDB db(getTestCouch());
	db.use(DATABASENAME);
	db.createDatabase();
	QueryServer qserver(DATABASENAME);
	prepareQueryServer(qserver);
	qserver.syncDesignDocuments(qserver.generateDesignDocuments(),db);
}

static void deleteDB(PrintTextA &) {
	CouchDB db(getTestCouch());
	db.use(DATABASENAME);
	db.deleteDatabase();
}


void runQueryServer() {
	QueryServer qserver(DATABASENAME);
	prepareQueryServer(qserver);
	qserver.runDispatchStdIO();
}

//randomly generated data
static const char *strdata="[[\"Kermit Byrd\",76,184],[\"Odette Hahn\",44,181],"
		"[\"Scarlett Frazier\",43,183],[\"Pascale Burt\",46,153],"
		"[\"Urielle Pennington\",21,166],[\"Bevis Bowen\",47,185],"
		"[\"Dakota Shepherd\",52,165],[\"Ramona Lang\",23,190],"
		"[\"Nicole Jordan\",75,150],[\"Owen Dillard\",80,151],"
		"[\"Daniel Cochran\",36,170],[\"Kenneth Meyer\",42,156]]";


static View by_name("_design/testview/_view/by_name");
static View by_name_cacheable("_design/testview/_view/by_name", View::forceGETMethod|View::includeDocs);
static View by_age_group("_design/testview/_view/by_age_group");
static View by_age("_design/testview/_view/by_age");
static View age_group_height("_design/testview/_view/age_group_height");


static void couchLoadData(PrintTextA &print) {
	CouchDB db(getTestCouch());
	db.use(DATABASENAME);


	AutoArray<Document, SmallAlloc<50> > savedDocs;

	Changeset chset(db.createChangeset());
	natural id=10000;
	JSON::Value data = db.json.factory->fromString(strdata);
	for (JSON::Iterator iter = data->getFwIter(); iter.hasItems();) {
		const JSON::KeyValue &kv= iter.getNext();
		Document doc;
		doc.edit(db.json)
				("name",kv[0])
				("age",kv[1])
				("height",kv[2])
				("_id",ToString<natural>(id,16));
		id+=14823;
		savedDocs.add(doc);
		chset.update(doc);
	}
	chset.commit(false);
	Set<StringA> uuidmap;

	for (natural i = 0; i < savedDocs.length(); i++) {
		StringA uuid = savedDocs[i]["_id"]->getStringUtf8();
//		print("%1\n") << uuid;
		uuidmap.insert(uuid);
	}
	print("%1") << uuidmap.size();

}


static void couchFindWildcard(PrintTextA &a) {

	CouchDB db(getTestCouch());
	db.use(DATABASENAME);

	Query q(db.createQuery(by_name));
	Result res = q("K")(Query::isArray)(Query::wildcard).exec();
	while (res.hasItems()) {
		Row row = res.getNext();
		a("%1,%2,%3 ") << row.key[0]->getStringUtf8()
				<<row.value[0]->getUInt()
				<<row.value[1]->getUInt();
	}
}

static void couchFindGroup(PrintTextA &a) {

	CouchDB db(getTestCouch());
	db.use(DATABASENAME);

	Query q(db.createQuery(by_age_group));
	Result res = q(40)(Query::any).exec();
	while (res.hasItems()) {
		Row row = res.getNext();
		a("%1 ") << row.value->getStringUtf8();
	}
}

static void couchFindRange(PrintTextA &a) {

	CouchDB db(getTestCouch());
	db.use(DATABASENAME);

	Query q(db.createQuery(by_age));
	Result res = q.from(20).to(40).reverseOrder().exec();
	while (res.hasItems()) {
		Row row = res.getNext();
		a("%1 ") << row.value->getStringUtf8();
	}
}

static void couchFindKeys(PrintTextA &a) {

	CouchDB db(getTestCouch());
	db.use(DATABASENAME);

	Query q(db.createQuery(by_name));
	Result res = q.select("Kermit Byrd")(Query::isArray)
					 .select("Owen Dillard")
					 .select("Nicole Jordan")
					 .exec();
	while (res.hasItems()) {
		Row row = res.getNext();
		a("%1,%2,%3 ") << row.key[0]->getStringUtf8()
				<<row.value[0]->getUInt()
				<<row.value[1]->getUInt();
	}
}

static void couchReduce(PrintTextA &a) {

	CouchDB db(getTestCouch());
	db.use(DATABASENAME);

	Query q(db.createQuery(age_group_height));
	Result res = q.group(1).exec();

	while (res.hasItems()) {
		Row row = res.getNext();
		a("%1:%2 ") << row.key[0]->getUInt()
				<<(row.value["sum"]->getUInt()/row.value["count"]->getUInt());
	}
}



defineTest qtest_couchCreateDB("couchdb.qserver.createDB","",&rawCreateDB);
defineTest qtest_couchLoadData("couchdb.qserver.loadData","12",&couchLoadData);
defineTest qtest_couchFindWildcard("couchdb.qserver.findWildcard","Kenneth Meyer,42,156 Kermit Byrd,76,184 ",&couchFindWildcard);
defineTest qtest_couchFindGroup("couchdb.qserver.findGroup","Kenneth Meyer Scarlett Frazier Odette Hahn Pascale Burt Bevis Bowen ",&couchFindGroup);
defineTest qtest_couchFindRange("couchdb.qserver.findRange","Daniel Cochran Ramona Lang Urielle Pennington ",&couchFindRange);
defineTest qtest_couchFindKeys("couchdb.qserver.findKeys","Kermit Byrd,76,184 Owen Dillard,80,151 Nicole Jordan,75,150 ",&couchFindKeys);
defineTest qtest_couchReduce("couchdb.qserver.reduce","20:178 30:170 40:171 50:165 70:167 80:151 ",&couchReduce);
defineTest qtest_couchDeleteDB("couchdb.qserver.deleteDB","",&deleteDB);
}



