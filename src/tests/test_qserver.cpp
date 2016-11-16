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
#include "../lightcouch/queryServer.tcc"
#include "lightspeed/base/framework/testapp.h"

#include "test_common.h"

#include "lightspeed/base/containers/autoArray.tcc"

#include "lightspeed/base/containers/set.tcc"

#include "lightspeed/base/countof.h"

#include "lightspeed/mt/thread.h"

#include "../lightcouch/changes.h"
namespace LightCouch {
using namespace LightSpeed;
using namespace BredyHttpClient;

#define DATABASENAME "lightcouch_unittest"



static void prepareQueryServer(QueryServer &qserver) {

	class ByName: public AbstractViewMapOnly<1> {
		virtual void map(const Document &doc, IEmitFn &emit) override {
			emit(Value({doc["name"]}), {doc["age"],doc["height"]});
		}
	};
	qserver.regView("testview/by_name", new ByName);

	class ByAge: public AbstractViewMapOnly<1> {
		virtual void map(const Document &doc, IEmitFn &emit) override {
			emit(doc["age"], doc["name"]);
		}
	};

	qserver.regView("testview/by_age", new ByAge);

	class ByAgeGroup: public AbstractViewMapOnly<1> {
		virtual void map(const Document &doc, IEmitFn &emit) override {
			emit({doc["age"].getUInt()/10*10, doc["age"]}, doc["name"]);
		}
	};
	qserver.regView("testview/by_age_group", new ByAgeGroup);

	class ByGroupHeight: public AbstractView<2> {
		virtual void map(const Document &doc, IEmitFn &emit) override {
			emit({doc["age"].getUInt()/10*10,doc["age"]}, doc["height"]);
		}
		virtual Value reduce(const RowsWithKeys &rows) override {
			natural sum = 0;
			for (natural i = 0; i < rows.length(); i++) {
				sum+=rows[i].value.getUInt();
			}
			return Object("sum",sum)("count",rows.length());
		}
		virtual Value rereduce(const ReducedRows &rows) override {
			natural count = 0;
			natural sum = 0;
			for (natural i = 0; i < rows.length(); i++) {
				sum+=rows[i].value["sum"].getUInt();
				count+=rows[i].value["count"].getUInt();
			}
			return Object("sum",sum)("count",rows.length());
		}
	};
	qserver.regView("testview/age_group_height", new ByGroupHeight);

	class ByGroupHeight2: public AbstractViewBuildin<2, AbstractViewBase::rmStats> {
		virtual void map(const Document &doc, IEmitFn &emit) override {
			emit( {doc["age"].getUInt()/10*10,doc["age"]}, doc["height"]);
		}
	};
	qserver.regView("testview/age_group_height2", new ByGroupHeight2);

	class TestList: public AbstractList<1> {
		virtual void run(IListContext &list, Value ) {
			list.start(Object("headers",Object("Content-Type","application/json")));
			Value row;
			list.send("{\"blabla\":\"ahoj\",\"rows\":[");
			bool second = false;
			while ((row = list.getRow()) != null) {
				if (second) list.send(",");
				list.send(row);
				second = true;
			}
			list.send("]}");
		}

	};
	qserver.regList("testview/rowcopy", new TestList);

	class FilterView_Young: public AbstractViewMapOnly<1> {
		virtual void map(const Document &doc, IEmitFn &emit) override {
			if (doc.getID().substr(0,8) == "_design/") return;
			if (doc["age"].getUInt() < 40) emit();
		}
	};
	qserver.regView("testview/young", new FilterView_Young);
	class Filter_AgeRange: public AbstractFilter<1> {
		virtual bool run(const Document &doc, Value request) {
			if (doc.getID().substr(0,8) == "_design/") return false;
			Value q = request["query"];
			natural agemin = q["agemin"].getUInt();
			natural agemax = q["agemax"].getUInt();
			natural age = doc["age"].getUInt();
			return age >= agemin && age <= agemax;
		}
	};
	qserver.regFilter("testview/young", new Filter_AgeRange);
}


static void rawCreateDB(PrintTextA &) {
	CouchDB db(getTestCouch());
	db.use(DATABASENAME);
	db.createDatabase();
	QueryServer qserver(DATABASENAME,AppBase::current().getAppPathname());
	prepareQueryServer(qserver);
	qserver.syncDesignDocuments(qserver.generateDesignDocuments(),db);
}

static void deleteDB(PrintTextA &) {
	CouchDB db(getTestCouch());
	db.use(DATABASENAME);
	db.deleteDatabase();
}


void runQueryServer() {
	QueryServer qserver(DATABASENAME,AppBase::current().getAppPathname());
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
static View by_name_cacheable("_design/testview/_view/by_name", View::includeDocs);
static View by_age_group("_design/testview/_view/by_age_group");
static View by_age("_design/testview/_view/by_age");
static View by_age_list("_design/testview/_list/rowcopy/by_age");
static View age_group_height("_design/testview/_view/age_group_height");
static View age_group_height2("_design/testview/_view/age_group_height2");
static Filter age_range("_design/testview/young",0);
static View age_range_view("_design/testview/_view/young",0);


static void couchLoadData(PrintTextA &print) {
	CouchDB db(getTestCouch());
	db.use(DATABASENAME);


	AutoArray<Document, SmallAlloc<50> > savedDocs;

	Changeset chset(db.createChangeset());
	natural id=10000;
	Value data = Value::fromString(strdata);
	for(auto &&item : data) {
		Document doc;
		doc("name",item[0])
			("age",item[1])
			("height",item[2])
			("_id",UIntToStr(id,16));
		id+=14823;
		savedDocs.add(doc);
		chset.update(doc);
	}

	chset.commit(false);
	Set<String> uuidmap;

	for (natural i = 0; i < savedDocs.length(); i++) {
		StrView uuid = savedDocs[i]["_id"].getString();
		uuidmap.insert(uuid);
	}
	print("%1") << uuidmap.size();

}


static void couchFindWildcard(PrintTextA &a) {

	CouchDB db(getTestCouch());
	db.use(DATABASENAME);

	Query q(db.createQuery(by_name));
	Result res = q.prefixString({"K"}).exec();
	while (res.hasItems()) {
		Row row = res.getNext();
		a("%1,%2,%3 ") << row.key[0].getString()
				<<row.value[0].getUInt()
				<<row.value[1].getUInt();
	}
}

static void couchFindGroup(PrintTextA &a) {

	CouchDB db(getTestCouch());
	db.use(DATABASENAME);

	Query q(db.createQuery(by_age_group));
	Result res = q.prefixKey(40).exec();
	while (res.hasItems()) {
		Row row = res.getNext();
		a("%1 ") << row.value.getString();
	}
}

static void couchFindRange(PrintTextA &a) {

	CouchDB db(getTestCouch());
	db.use(DATABASENAME);

	Query q(db.createQuery(by_age));
	Result res = q.range(20,40).reversedOrder().exec();
	while (res.hasItems()) {
		Row row = res.getNext();
		a("%1 ") << row.value.getString();
	}
}

static void couchFindRangeList(PrintTextA &a) {

	CouchDB db(getTestCouch());
	db.use(DATABASENAME);

	Query q(db.createQuery(by_age_list));
	Result res = q.range(20,40).reversedOrder().exec();
	while (res.hasItems()) {
		Row row = res.getNext();
		a("%1 ") << row.value.getString();
	}
}

static void couchFindKeys(PrintTextA &a) {

	CouchDB db(getTestCouch());
	db.use(DATABASENAME);

	Query q(db.createQuery(by_name));
	Result res = q.keys({
				{"Kermit Byrd"},
				{"Owen Dillard"},
				{"Nicole Jordan"}
					}).exec();
	while (res.hasItems()) {
		Row row = res.getNext();
		a("%1,%2,%3 ") << row.key[0].getString()
				<<row.value[0].getUInt()
				<<row.value[1].getUInt();
	}
}

static void couchReduce(PrintTextA &a) {

	CouchDB db(getTestCouch());
	db.use(DATABASENAME);

	Query q(db.createQuery(age_group_height));
	Result res = q.groupLevel(1).exec();

	while (res.hasItems()) {
		Row row = res.getNext();
		a("%1:%2 ") << row.key[0].getUInt()
				<<(row.value["sum"].getUInt()/row.value["count"].getUInt());
	}
}

static void couchReduce2(PrintTextA &a) {

	CouchDB db(getTestCouch());
	db.use(DATABASENAME);

	Query q(db.createQuery(age_group_height2));
	Result res = q.groupLevel(1).exec();

	while (res.hasItems()) {
		Row row = res.getNext();
		a("%1:%2 ") << row.key[0].getUInt()
					<<(row.value["sum"].getUInt()/row.value["count"].getUInt());
	}
}


static void couchFilterView(PrintTextA &a) {

	CouchDB db(getTestCouch());
	db.use(DATABASENAME);

	ChangesSink chsink(db.createChangesSink());
	chsink.setFilter(Filter(age_range_view,false));
	Changes changes = chsink.exec();

	a("%1") << changes.length();
}

static void couchFilter(PrintTextA &a) {

	CouchDB db(getTestCouch());
	db.use(DATABASENAME);

	ChangesSink chsink(db.createChangesSink());
	chsink.setFilter(age_range).arg("agemin",40).arg("agemax",60);
	Changes changes = chsink.exec();

	a("%1") << changes.length();
}


defineTest qtest_couchCreateDB("couchdb.qserver.createDB","",&rawCreateDB);
defineTest qtest_couchLoadData("couchdb.qserver.loadData","12",&couchLoadData);
defineTest qtest_couchFindKeys("couchdb.qserver.findKeys","Kermit Byrd,76,184 Owen Dillard,80,151 Nicole Jordan,75,150 ",&couchFindKeys);
defineTest qtest_couchFindWildcard("couchdb.qserver.findWildcard","Kenneth Meyer,42,156 Kermit Byrd,76,184 ",&couchFindWildcard);
defineTest qtest_couchFindGroup("couchdb.qserver.findGroup","Kenneth Meyer Scarlett Frazier Odette Hahn Pascale Burt Bevis Bowen ",&couchFindGroup);
defineTest qtest_couchFindRange("couchdb.qserver.findRange","Daniel Cochran Ramona Lang Urielle Pennington ",&couchFindRange);
defineTest qtest_couchFindRangeList("couchdb.qserver.findRangeList","Daniel Cochran Ramona Lang Urielle Pennington ",&couchFindRangeList);
defineTest qtest_couchReduce("couchdb.qserver.reduce","20:178 30:170 40:171 50:165 70:167 80:151 ",&couchReduce);
defineTest qtest_couchReduce2("couchdb.qserver.reduce2","20:178 30:170 40:171 50:165 70:167 80:151 ",&couchReduce2);
defineTest qtest_couchFilterView("couchdb.qserver.filterView","3",&couchFilterView);
defineTest qtest_couchFilter("couchdb.qserver.filter","6",&couchFilter);
defineTest qtest_couchDeleteDB("couchdb.qserver.deleteDB","",&deleteDB);
}



