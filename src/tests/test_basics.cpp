/*
 * test_basics.cpp

 *
 *  Created on: 19. 3. 2016
 *      Author: ondra
 */
#include <iostream>
#include <set>
#include <vector>
#include <thread>
#include <chrono>
#include <condition_variable>
#include "../lightcouch/attachment.h"
#include "../lightcouch/document.h"
#include "../lightcouch/queryCache.h"
#include "../lightcouch/changeset.h"
#include "../lightcouch/couchDB.h"
#include "../lightcouch/query.h"
#include "../lightcouch/changes.h"
#include "../lightcouch/json.h"
#include "../lightcouch/num2str.h"
#include "test_common.h"
#include "testClass.h"

namespace couchit {

#define DATABASENAME "lightcouch_unittest"

static void couchConnect(std::ostream &print) {

	CouchDB db(getTestCouch());

	CouchDB::PConnection conn = db.getConnection("/");
	Value v = db.requestGET(conn);
	print << v["couchdb"].getString();
}


static void rawCreateDB(std::ostream &) {
	CouchDB db(getTestCouch());
	db.setCurrentDB(DATABASENAME);
	db.createDatabase();
}

static void deleteDB(std::ostream &) {
	CouchDB db(getTestCouch());
	db.setCurrentDB(DATABASENAME);
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
		"\"by_name\":{\"map\":function (doc) {if (doc.name) emit([doc.name], [doc.age,doc.height]);}},"
		"\"by_age\":{\"map\":function (doc) {if (doc.name) emit(doc.age, doc.name);\n\t\t\t\t}},"
		"\"by_age_group\":{\"map\":function (doc) {if (doc.name) emit([Math.floor(doc.age/10)*10, doc.age], doc.name);}},"
		"\"age_group_height\":{\"map\":function (doc) {if (doc.name) emit([Math.floor(doc.age/10)*10, doc.age], doc.height);},\"reduce\":\"_stats\"}},"
		"\"dummy\":false}"
};

static View by_name("_design/testview/_view/by_name");
static View by_name_cacheable("_design/testview/_view/by_name", View::includeDocs);
static View by_age_group("_design/testview/_view/by_age_group");
static View by_age("_design/testview/_view/by_age");
static View age_group_height("_design/testview/_view/age_group_height");

json::String UIntToStr(std::size_t id, int base) {
	return String(21, [&](char *c) {
		return unsignedToStringImpl([&](char z) {
			*c++ = z;
		},id,8,true,base);
	});
}

static void couchLoadData(std::ostream &print) {
	CouchDB db(getTestCouch());
	db.setCurrentDB(DATABASENAME);


	std::vector<Document> savedDocs;

	Changeset chset(db.createChangeset());
	std::size_t id=10000;
	Value data = Value::fromString(strdata);
	for(auto &&item : data) {
		Document doc;
		doc("name",item[0])
			("age",item[1])
			("height",item[2])
			("_id",UIntToStr(id,16));
		id+=14823;
		savedDocs.push_back(doc);
		chset.update(doc);
	}

	chset.commit(false);
	std::set<String> uuidmap;

	for (std::size_t i = 0; i < savedDocs.size(); i++) {
		StrViewA uuid = savedDocs[i]["_id"].getString();
		uuidmap.insert(uuid);
	}
	print << uuidmap.size();

}

static void couchConflicted(std::ostream &print) {
	CouchDB db(getTestCouch());
	db.setCurrentDB(DATABASENAME);


	try {
		couchLoadData(print);
	} catch (UpdateException &e) {
		print << "conflicts-" << e.getErrors().length;
	}

}

#define countof(x) (sizeof(x)/sizeof(x[0]))

static void couchLoadDesign(std::ostream &) {
	CouchDB db(getTestCouch());
	db.setCurrentDB(DATABASENAME);


	for (std::size_t i = 0; i < countof(designs); i++) {
		db.putDesignDocument(designs[i],strlen(designs[i]));
	}

	//try twice
	for (std::size_t i = 0; i < countof(designs); i++) {
		db.putDesignDocument(designs[i],strlen(designs[i]));
	}

}

static void couchFindWildcard(std::ostream &a) {

	CouchDB db(getTestCouch());
	db.setCurrentDB(DATABASENAME);

	Query q(db.createQuery(by_name));
	Result res = q.prefixString({"K"}).exec();
	while (res.hasItems()) {
		Row row = res.getNext();
		a  << row.key[0].getString() << ","
				<<row.value[0].getUInt() << ","
				<<row.value[1].getUInt() << " ";
	}
}

static void couchFindGroup(std::ostream &a) {

	CouchDB db(getTestCouch());
	db.setCurrentDB(DATABASENAME);

	Query q(db.createQuery(by_age_group));
	Result res = q.prefixKey(40).exec();
	while (res.hasItems()) {
		Row row = res.getNext();
		a << row.value.getString() << " ";
	}
}

static void couchFindRange(std::ostream &a) {

	CouchDB db(getTestCouch());
	db.setCurrentDB(DATABASENAME);

	Query q(db.createQuery(by_age));
	Result res = q.range(20,40).reversedOrder().exec();
	while (res.hasItems()) {
		Row row = res.getNext();
		a << row.value.getString() << " ";
	}
}

static void couchFindKeys(std::ostream &a) {

	CouchDB db(getTestCouch());
	db.setCurrentDB(DATABASENAME);

	Query q(db.createQuery(by_name));
	Result res = q.keys({
				{"Kermit Byrd"},
				{"Owen Dillard"},
				{"Nicole Jordan"}
					}).exec();
	while (res.hasItems()) {
		Row row = res.getNext();
		a << row.key[0].getString() << ","
				<<row.value[0].getUInt() << ","
				<<row.value[1].getUInt() << " ";
	}
}

static void couchCaching(std::ostream &a) {

	QueryCache cache;
	Config cfg = getTestCouch();
	cfg.cache = &cache;
	CouchDB db(cfg);
	db.setCurrentDB(DATABASENAME);
	json::PValue v;

	for (std::size_t i = 0; i < 3; i++) {
		Query q(db.createQuery(by_name_cacheable));
		Value r = q.keys({
			{"Kermit Byrd"},
			{"Owen Dillard"},
			{"Nicole Jordan"}
				}).exec();
		bool cached = r.getHandle() == v;
		Result res(r);
		while (res.hasItems()) {
			Row row = res.getNext();
			a << row.key[0].getString() << ","
					<<row.value[0].getUInt() << ","
					<<row.value[1].getUInt() << ":"
					<< cached << " ";

		}
		v = r.getHandle();
	}

}
/*
static void couchCaching2(std::ostream &a) {

	QueryCache cache;
	Config cfg = getTestCouch();
	cfg.cache = &cache;
	CouchDB db(cfg);
	db.use(DATABASENAME);
	db.trackSeqNumbers();
	String killDocName = "Owen Dillard";
	Document killDoc;
	json::PValue vhandle;


	for (std::size_t i = 0; i < 3; i++) {

		if (i == 2) {
			//make a change during second run
			Changeset cset = db.createChangeset();
			killDoc.setDeleted();
			cset.update(killDoc);
			cset.commit(db);
			//also calls listenChanges
			db.getLastSeqNumber();
		}

		Query q(db.createQuery(by_name_cacheable));
		Result res = q.select("Kermit Byrd")(Query::isArray)
						 .select("Owen Dillard")
						 .select("Nicole Jordan")
						 .exec();
		while (res.hasItems()) {
			Row row = res.getNext();
			a("%1,%2,%3,%4 ") << row.key[0].getString()
					<<row.value[0].getUInt()
					<<row.value[1].getUInt()
					<<(vhandle==res.getHandle()?"true":"false");

			//remember values of what to erase
			if (row.key[0] == killDocName) {
				killDoc = row.doc;
			}
		}
		vhandle = res.getHandle();
	}

}
*/

static void couchReduce(std::ostream &a) {

	CouchDB db(getTestCouch());
	db.setCurrentDB(DATABASENAME);

	Query q(db.createQuery(age_group_height));
	Result res = q.groupLevel(1).exec();

	while (res.hasItems()) {
		Row row = res.getNext();
		a << row.key[0].getUInt() << ":"
				<<(row.value["sum"].getUInt()/row.value["count"].getUInt()) << " ";
	}
}


static Value lastId;

static void couchChangeSetOneShot(std::ostream &a) {

	CouchDB db(getTestCouch());
	db.setCurrentDB(DATABASENAME);

	ChangesSink chsink (db.createChangesSink());
	Changes chngs = chsink.exec();
	std::size_t count = 0;
	while (chngs.hasItems()) {
		ChangedDoc doc(chngs.getNext());
		count++;
	}

	a << (count > 10);
}

static void loadSomeDataThread(CouchDB &db,StrViewA locId) {

	std::this_thread::sleep_for(std::chrono::seconds(1));
	Changeset chset = db.createChangeset();
	Document doc;
	doc("_id",locId)
	   ("aaa",100);
	chset.update(doc);
	chset.commit(db);
}

static void couchChangeSetWaitForData(std::ostream &a) {
	CouchDB db(getTestCouch());
	db.setCurrentDB(DATABASENAME);

	StrViewA uid = db.genUID();

	std::thread thr([&]{loadSomeDataThread(db,uid);});

	ChangesSink chsink (db.createChangesSink());
	chsink.setTimeout(10000);
	chsink.fromSeq(lastId);
	try {
		chsink >> [&](const ChangedDoc &doc) {
			if (doc.id == uid && !doc.deleted) {
				throw CanceledException();

			}
		};
		a << "fail";
	} catch (CanceledException &) {
		a << "ok";
	}
	thr.join();
}

class Event {
	std::condition_variable condVar;
	std::mutex mutex;
	bool ready = false;
public:
	void wait() {
		std::unique_lock<std::mutex> _(mutex);
		condVar.wait(_, [&]{return ready;});
		ready = false;
	}
	void notify() {
		std::unique_lock<std::mutex> _(mutex);
		ready = true;
		condVar.notify_one();
	}
};

static void loadSomeDataThread3(CouchDB &db, String locId, Event &event) {

	for (std::size_t i = 0; i < 3; i++) {
		Changeset chset = db.createChangeset();
		Document doc;
		doc("_id",locId.substr(i))
		   ("aaa",100);
		chset.update(doc);
		chset.commit(db);
		event.wait();
	}
}

static void couchChangeSetWaitForData3(std::ostream &a) {
	CouchDB db(getTestCouch());
	db.setCurrentDB(DATABASENAME);

	String uid = db.genUID();
	int counter=0;

	Event event;

	std::thread thr([&]{loadSomeDataThread3(db,uid, event);});

	ChangesSink chsink (db.createChangesSink());
	chsink.setTimeout(10000);
	chsink.fromSeq(lastId);
	try {
		chsink >> [&](const ChangedDoc &doc) {
			if (doc.id == uid && !doc.deleted) {
				counter++;
				event.notify();
			} else if (counter) {
				counter++;
				event.notify();
				if (counter == 3) throw CanceledException();
			}
		};
		a << "fail";
	} catch (CanceledException &e) {
		a << "ok";
	}
	thr.join();


}

static void couchChangesStopWait(std::ostream &a) {
	CouchDB db(getTestCouch());
	db.setCurrentDB(DATABASENAME);

	ChangesSink chsink (db.createChangesSink());
	chsink.setTimeout(10000);


	std::thread thr([&chsink]() {
		std::this_thread::sleep_for(std::chrono::seconds(1));
		chsink.cancelWait();
	});

	try {
		chsink >> [](const ChangedDoc &) {};
		a << "fail";
	} catch (CanceledException &) {
		CouchDB::PConnection conn = db.getConnection("/");
		Value v = db.requestGET(conn);
		a << v["couchdb"].getString();
	}
	thr.join();

}

static void couchGetSeqNumber(std::ostream &a) {

	CouchDB db(getTestCouch());
	db.setCurrentDB(DATABASENAME);
	Value cnt = db.getLastSeqNumber();

	a << (cnt != null ?"ok":"failed");

}

static void couchRetrieveDocument(std::ostream &a) {
	CouchDB db(getTestCouch());
	db.setCurrentDB(DATABASENAME);

	Query q(db.createQuery(by_name));
	Result res = q.key({"Kermit Byrd"}).exec();
	Row row = res.getNext();

	Document doc = db.get(row.id.getString(), CouchDB::flgSeqNumber);
	//this is random - cannot be tested
	doc.unset("_id").unset("_rev");
	a << Value(doc).toString();
}

static void couchStoreAndRetrieveAttachment(std::ostream &a) {
	CouchDB db(getTestCouch());
	db.setCurrentDB(DATABASENAME);

	Document doc = db.newDocument("data-");
	Upload upl = db.putAttachment(doc,"testAttachment","text/plain");
	StrViewA sentence("The quick brown fox jumps over the lazy dog");
	upl.write(sentence.data,sentence.length);
	upl.finish();

	Document doc2 = db.get(doc.getID());


	AttachmentData data = db.getAttachment(doc2,"testAttachment");

	a << data.contentType << "-" << StrViewA(data);


}

void runTestBasics(TestSimple &tst) {

tst.test("couchdb.connect","Welcome") >> &couchConnect;
tst.test("couchdb.createDB","") >> &rawCreateDB;
tst.test("couchdb.loadData","12") >> &couchLoadData;
tst.test("couchdb.loadDesign","") >> &couchLoadDesign;
tst.test("couchdb.detectConflict","conflicts-12") >> &couchConflicted;
tst.test("couchdb.findWildcard","Kenneth Meyer,42,156 Kermit Byrd,76,184 ") >> &couchFindWildcard;
tst.test("couchdb.findGroup","Kenneth Meyer Scarlett Frazier Odette Hahn Pascale Burt Bevis Bowen ") >> &couchFindGroup;
tst.test("couchdb.findRange","Daniel Cochran Ramona Lang Urielle Pennington ") >> &couchFindRange;
tst.test("couchdb.findKeys","Kermit Byrd,76,184 Owen Dillard,80,151 Nicole Jordan,75,150 ") >> &couchFindKeys;
tst.test("couchdb.retrieveDoc","{\"_local_seq\":1,\"age\":76,\"height\":184,\"name\":\"Kermit Byrd\"}") >> &couchRetrieveDocument;
tst.test("couchdb.caching","Kermit Byrd,76,184:0 Owen Dillard,80,151:0 Nicole Jordan,75,150:0 Kermit Byrd,76,184:1 Owen Dillard,80,151:1 Nicole Jordan,75,150:1 Kermit Byrd,76,184:1 Owen Dillard,80,151:1 Nicole Jordan,75,150:1 ") >> &couchCaching;
tst.test("couchdb.reduce","20:178 30:170 40:171 50:165 70:167 80:151 ") >>  &couchReduce;
//defineTest test_couchCaching2("couchdb.caching2","Kermit Byrd,76,184 Owen Dillard,80,151 Nicole Jordan,75,150 Kermit Byrd,184,100 Owen Dillard,151,100 Nicole Jordan,150,100 Kermit Byrd,76,184 Nicole Jordan,75,150 ",&couchCaching2);
tst.test("couchdb.changesOneShot","1") >> &couchChangeSetOneShot;
tst.test("couchdb.changesWaiting","ok") >> &couchChangeSetWaitForData;
tst.test("couchdb.changesWaitingForThree","ok") >> &couchChangeSetWaitForData3;
tst.test("couchdb.changesStopWait","Welcome") >> &couchChangesStopWait;
tst.test("couchdb.getSeqNumber","ok") >> &couchGetSeqNumber;
tst.test("couchdb.attachments","text/plain-The quick brown fox jumps over the lazy dog") >> &couchStoreAndRetrieveAttachment;
tst.test("couchdb.deleteDB","") >> &deleteDB;
}




}
