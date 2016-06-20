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

static void couchConnect(PrintTextA &print) {

	CouchDB db(getTestCouch());

	ConstValue v = db.requestGET("/");
		print("%1") << v["couchdb"]->getStringUtf8();
}


static void rawCreateDB(PrintTextA &) {
	CouchDB db(getTestCouch());
	db.use(DATABASENAME);
	db.createDatabase();
}

static void deleteDB(PrintTextA &) {
	CouchDB db(getTestCouch());
	db.use(DATABASENAME);
	db.deleteDatabase();
}

//randomly generated data
static const char *strdata="[[\"Kermit Byrd\",76,184],[\"Odette Hahn\",44,181],"
		"[\"Scarlett Frazier\",43,183],[\"Pascale Burt\",46,153],"
		"[\"Urielle Pennington\",21,166],[\"Bevis Bowen\",47,185],"
		"[\"Dakota Shepherd\",52,165],[\"Ramona Lang\",23,190],"
		"[\"Nicole Jordan\",75,150],[\"Owen Dillard\",80,151],"
		"[\"Daniel Cochran\",36,170],[\"Kenneth Meyer\",42,156]]";

static const char *designs[]={
		"{\"_id\":\"_design/testview\",\"language\":\"javascript\",\"views\":{"
		"\"by_name\":{\"map\":\"function (doc) {\n\t\t\t\t\temit([doc.name], [doc.age,doc.height]);\n\t\t\t\t}\"},"
		"\"by_age\":{\"map\":\"function (doc) {\n\t\t\t\t\temit(doc.age, doc.name);\n\t\t\t\t}\"},"
		"\"by_age_group\":{\"map\":\"function (doc) {\n\t\t\t\t\temit([Math.floor(doc.age/10)*10, doc.age], doc.name);\n\t\t\t\t}\"},"
		"\"age_group_height\":{\"map\":\"function (doc) {\n\t\t\t\t\temit([Math.floor(doc.age/10)*10, doc.age], doc.height);\n\t\t\t\t}\",\"reduce\":\"_stats\"}}}"
};

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
	JSON::Value data = db.json.factory->fromString(strdata);
	for (JSON::Iterator iter = data->getFwIter(); iter.hasItems();) {
		const JSON::KeyValue &kv= iter.getNext();
		Document doc;
		doc.edit(db.json)
				("name",kv[0])
				("age",kv[1])
				("height",kv[2]);
		savedDocs.add(doc);
		chset.update(doc);
	}
	chset.commit(db);
	Set<StringA> uuidmap;

	for (natural i = 0; i < savedDocs.length(); i++) {
		StringA uuid = savedDocs[i]["_id"]->getStringUtf8();
//		print("%1\n") << uuid;
		uuidmap.insert(uuid);
	}
	print("%1") << uuidmap.size();

}

static void couchConflicted(PrintTextA &print) {
	CouchDB db(getTestCouch());
	db.use(DATABASENAME);


	try {
		Changeset chset(db.createChangeset());
		for (natural i = 0; i < countof(designs); i++) {

			Document doc(db.json.factory->fromString(designs[i]));
			chset.update(doc);
		}
		chset.commit(db,false);
		print("failed");
	} catch (Changeset::UpdateException &e) {
		print("%1") << e.getErrors().length();
	}

}


static void couchLoadDesign(PrintTextA &) {
	CouchDB db(getTestCouch());
	db.use(DATABASENAME);

	Changeset chset(db.createChangeset());
	for (natural i = 0; i < countof(designs); i++) {
		Document doc(db.json.factory->fromString(designs[i]));
		chset.update(doc);
	}
	chset.commit(db);
}

static void couchFindWildcard(PrintTextA &a) {

	CouchDB db(getTestCouch());
	db.use(DATABASENAME);

	Query q(db.createQuery(by_name));
	Query::Result res = q("K")(Query::isArray)(Query::wildcard).exec();
	while (res.hasItems()) {
		Query::Row row = res.getNext();
		a("%1,%2,%3 ") << row.key[0]->getStringUtf8()
				<<row.value[0]->getUInt()
				<<row.value[1]->getUInt();
	}
}

static void couchFindGroup(PrintTextA &a) {

	CouchDB db(getTestCouch());
	db.use(DATABASENAME);

	Query q(db.createQuery(by_age_group));
	Query::Result res = q(40)(Query::any).exec();
	while (res.hasItems()) {
		Query::Row row = res.getNext();
		a("%1 ") << row.value->getStringUtf8();
	}
}

static void couchFindRange(PrintTextA &a) {

	CouchDB db(getTestCouch());
	db.use(DATABASENAME);

	Query q(db.createQuery(by_age));
	Query::Result res = q.from(20).to(40).reverseOrder().exec();
	while (res.hasItems()) {
		Query::Row row = res.getNext();
		a("%1 ") << row.value->getStringUtf8();
	}
}

static void couchFindKeys(PrintTextA &a) {

	CouchDB db(getTestCouch());
	db.use(DATABASENAME);

	Query q(db.createQuery(by_name));
	Query::Result res = q.select("Kermit Byrd")(Query::isArray)
					 .select("Owen Dillard")
					 .select("Nicole Jordan")
					 .exec();
	while (res.hasItems()) {
		Query::Row row = res.getNext();
		a("%1,%2,%3 ") << row.key[0]->getStringUtf8()
				<<row.value[0]->getUInt()
				<<row.value[1]->getUInt();
	}
}

static void couchCaching(PrintTextA &a) {

	QueryCache cache;
	Config cfg = getTestCouch();
	cfg.cache = &cache;
	CouchDB db(cfg);
	db.use(DATABASENAME);

	for (natural i = 0; i < 3; i++) {
		Query q(db.createQuery(by_name_cacheable));
		Query::Result res = q.select("Kermit Byrd")(Query::isArray)
						 .select("Owen Dillard")
						 .select("Nicole Jordan")
						 .exec();
		while (res.hasItems()) {
			Query::Row row = res.getNext();
			a("%1,%2,%3 ") << row.key[0]->getStringUtf8()
					<<row.value[0]->getUInt()
					<<row.value[1]->getUInt();

			Container &mutval = const_cast<Container &>(static_cast<const Container &>(row.value));
			//modify cache, so we can detect, that caching is working
			mutval.erase(0);
			mutval.add(db.json(100));
		}
	}

}

static void couchCaching2(PrintTextA &a) {

	QueryCache cache;
	Config cfg = getTestCouch();
	cfg.cache = &cache;
	CouchDB db(cfg);
	db.use(DATABASENAME);
	db.trackSeqNumbers();
	ConstStrA killDocName = "Owen Dillard";
	Document killDoc;


	for (natural i = 0; i < 3; i++) {

		if (i == 2) {
			//make a change during second run
			Changeset cset = db.createChangeset();
			killDoc.setDeleted(db.json);
			cset.update(killDoc);
			cset.commit(db);
			//also calls listenChanges
			db.getLastSeqNumber();
		}

		Query q(db.createQuery(by_name_cacheable));
		Query::Result res = q.select("Kermit Byrd")(Query::isArray)
						 .select("Owen Dillard")
						 .select("Nicole Jordan")
						 .exec();
		while (res.hasItems()) {
			Query::Row row = res.getNext();
			a("%1,%2,%3 ") << row.key[0]->getStringUtf8()
					<<row.value[0]->getUInt()
					<<row.value[1]->getUInt();

			//modify cache, so we can detect, that caching is working
			Container &mutval = const_cast<Container &>(static_cast<const Container &>(row.value));
			mutval.erase(0);
			mutval.add(db.json(100));
			//remember values of what to erase
			if (killDocName == row.key[0]->getStringUtf8()) {
				killDoc = row.doc;
			}
		}
	}

}


static void couchReduce(PrintTextA &a) {

	CouchDB db(getTestCouch());
	db.use(DATABASENAME);

	Query q(db.createQuery(age_group_height));
	Query::Result res = q.group(1).exec();

	while (res.hasItems()) {
		Query::Row row = res.getNext();
		a("%1:%2 ") << row.key[0]->getUInt()
				<<(row.value["sum"]->getUInt()/row.value["count"]->getUInt());
	}
}


static natural lastId = 0;

static void couchChangeSetOneShot(PrintTextA &a) {

	CouchDB db(getTestCouch());
	db.use(DATABASENAME);

	db.listenChanges(0,0,CouchDB::lmOneShot, [&](const ChangedDoc &doc) {
		lastId = doc.seqId;
		return true;
	});

	a("%1") << (lastId > 10);
}

static void loadSomeDataThread(ConstStrA locId) {
	CouchDB db(getTestCouch());
	db.use(DATABASENAME);

	Thread::sleep(1000);
	Changeset chset = db.createChangeset();
	Document doc;
	doc.edit(chset.json)("_id",locId)("aaa",100);
	chset.update(doc);
	chset.commit(db);
}

static void couchChangeSetWaitForData(PrintTextA &a) {
	CouchDB db(getTestCouch());
	db.use(DATABASENAME);

	UID uid = db.getUID();
	bool isok = false;

	Thread thr;
	thr.start(ThreadFunction::create(&loadSomeDataThread,uid));


	db.listenChanges(lastId,0,CouchDB::lmForever,[&](const ChangedDoc &doc) {
		if (doc.id == uid && !doc.deleted) {
			isok = true;
			return false;
		} else {
			return true;
		}
	});
	if (isok) a("ok"); else a("fail");


}

static void loadSomeDataThread3(ConstStrA locId) {
	CouchDB db(getTestCouch());
	db.use(DATABASENAME);

	for (natural i = 0; i < 3; i++) {
		Changeset chset = db.createChangeset();
		Document doc;
		doc.edit(chset.json)("_id",locId)("aaa",100);
		chset.update(doc);
		chset.commit(db);
		locId = locId.offset(1);
		Thread::sleep(nil);
	}
}

static void couchChangeSetWaitForData3(PrintTextA &a) {
	CouchDB db(getTestCouch());
	db.use(DATABASENAME);

	UID uid = db.getUID();
	bool isok = false;
	int counter=0;

	Thread thr;
	thr.start(ThreadFunction::create(&loadSomeDataThread3,uid));


	db.listenChanges(lastId,0,CouchDB::lmForever,[&](const ChangedDoc &doc) {
		if (doc.id == uid && !doc.deleted) {
			counter++;
			thr.wakeUp();
			return true;
		} else if (counter) {
			isok=true;
			counter++;
			thr.wakeUp();
			return (counter < 3);
		} else
			return true;
	});
	if (isok) a("ok"); else a("fail");


}

static void couchChangesStopWait(PrintTextA &a) {
	CouchDB db(getTestCouch());
	db.use(DATABASENAME);
	Thread thr;

	thr.start(ThreadFunction::create([&db]() {
		Thread::sleep(1000);
		db.stopListenChanges();
	}));
	db.listenChanges(0,0,CouchDB::lmForever,[](const ChangedDoc &) {
		return true;
	});

	ConstValue v = db.requestGET("/");
	a("%1") << v["couchdb"]->getStringUtf8();
}

static void couchGetSeqNumber(PrintTextA &a) {

	CouchDB db(getTestCouch());
	db.use(DATABASENAME);
	natural cnt = db.getLastSeqNumber();

	a("%1") << ((cnt > 10)?"ok":"failed");

}

static void couchRetrieveDocument(PrintTextA &a) {
	CouchDB db(getTestCouch());
	db.use(DATABASENAME);

	Query q(db.createQuery(by_name));
	Query::Result res = q.select("Kermit Byrd")(Query::isArray).exec();
	Query::Row row = res.getNext();

	ConstValue doc = db.retrieveDocument(row.id, CouchDB::flgSeqNumber);
	Container r = db.json.object(doc);
	r.unset("_id");
	//this is random - cannot be tested
	r.unset("_rev");
	a("%1") << db.json.factory->toString(*r);
}

static void couchStoreAndRetrieveAttachment(PrintTextA &a) {
	CouchDB db(getTestCouch());
	db.use(DATABASENAME);

	Document doc = db.newDocument(".data");
	db.uploadAttachment(doc,"testAttachment","text/plain",[](SeqFileOutput wr) {
		SeqTextOutA txtwr(wr);
		ConstStrA sentence("The quick brown fox jumps over the lazy dog");
		txtwr.blockWrite(sentence,true);
	});

	Document doc2 = db.retrieveDocument(doc.getID());


	AttachmentData data = db.downloadAttachment(doc2,"testAttachment");

	a("%1-%2") << data.contentType << ConstStrA(reinterpret_cast<const char *>(data.data()),data.length());


}

defineTest test_couchConnect("couchdb.connect","Welcome",&couchConnect);
defineTest test_couchCreateDB("couchdb.createDB","",&rawCreateDB);
defineTest test_couchLoadData("couchdb.loadData","12",&couchLoadData);
defineTest test_couchLoadDesign("couchdb.loadDesign","",&couchLoadDesign);
defineTest test_couchConflict("couchdb.detectConflict","1",&couchConflicted);
defineTest test_couchFindWildcard("couchdb.findWildcard","Kenneth Meyer,42,156 Kermit Byrd,76,184 ",&couchFindWildcard);
defineTest test_couchFindGroup("couchdb.findGroup","Kenneth Meyer Scarlett Frazier Odette Hahn Pascale Burt Bevis Bowen ",&couchFindGroup);
defineTest test_couchFindRange("couchdb.findRange","Daniel Cochran Ramona Lang Urielle Pennington ",&couchFindRange);
defineTest test_couchFindKeys("couchdb.findKeys","Kermit Byrd,76,184 Owen Dillard,80,151 Nicole Jordan,75,150 ",&couchFindKeys);
defineTest test_couchRetrieveDocument("couchdb.retrieveDoc","{\"_local_seq\":1,\"age\":76,\"height\":184,\"name\":\"Kermit Byrd\"}",&couchRetrieveDocument);
defineTest test_couchCaching("couchdb.caching","Kermit Byrd,76,184 Owen Dillard,80,151 Nicole Jordan,75,150 Kermit Byrd,184,100 Owen Dillard,151,100 Nicole Jordan,150,100 Kermit Byrd,100,100 Owen Dillard,100,100 Nicole Jordan,100,100 ",&couchCaching);
defineTest test_couchReduce("couchdb.reduce","20:178 30:170 40:171 50:165 70:167 80:151 ",&couchReduce);
defineTest test_couchCaching2("couchdb.caching2","Kermit Byrd,76,184 Owen Dillard,80,151 Nicole Jordan,75,150 Kermit Byrd,184,100 Owen Dillard,151,100 Nicole Jordan,150,100 Kermit Byrd,76,184 Nicole Jordan,75,150 ",&couchCaching2);
defineTest test_couchChangesOneShot("couchdb.changesOneShot","1",&couchChangeSetOneShot);
defineTest test_couchChangesWaiting("couchdb.changesWaiting","ok",&couchChangeSetWaitForData);
defineTest test_couchChangesWaiting3("couchdb.changesWaitingForThree","ok",&couchChangeSetWaitForData3);
defineTest test_couchChangesStopWait("couchdb.changesStopWait","Welcome",&couchChangesStopWait);
defineTest test_couchGetSeqNumber("couchdb.getSeqNumber","ok",&couchGetSeqNumber);
defineTest test_couchAttachments("couchdb.attachments","text/plain-The quick brown fox jumps over the lazy dog",&couchStoreAndRetrieveAttachment);
defineTest test_couchDeleteDB("couchdb.deleteDB","",&deleteDB);
}



