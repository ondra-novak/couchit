/*
 * test_basics.cpp
 *
 *  Created on: 19. 3. 2016
 *      Author: ondra
 */

#include <set>

#include "../lightcouch/changes.h"

#include "../lightcouch/couchDB.h"
#include "../lightcouch/changeset.h"
#include "../lightcouch/num2str.h"
#include "test_common.h"
#include "../lightcouch/queryServer.h"
#include "testClass.h"

namespace couchit {

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
			std::size_t sum = 0;
			for (std::size_t i = 0; i < rows.length; i++) {
				sum+=rows[i].value.getUInt();
			}
			return Object("sum",sum)("count",rows.length);
		}
		virtual Value rereduce(const ReducedRows &rows) override {
			std::size_t count = 0;
			std::size_t sum = 0;
			for (std::size_t i = 0; i < rows.length; i++) {
				sum+=rows[i].value["sum"].getUInt();
				count+=rows[i].value["count"].getUInt();
			}
			return Object("sum",sum)("count",rows.length);
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
			std::size_t agemin = q["agemin"].getUInt();
			std::size_t agemax = q["agemax"].getUInt();
			std::size_t age = doc["age"].getUInt();
			return age >= agemin && age <= agemax;
		}
	};
	qserver.regFilter("testview/young", new Filter_AgeRange);
}


static void rawCreateDB(std::ostream &) {
	CouchDB db(getTestCouch());
	db.setCurrentDB(DATABASENAME);
	db.createDatabase();
	QueryServer qserver(DATABASENAME);
	prepareQueryServer(qserver);
	qserver.syncDesignDocuments(qserver.generateDesignDocuments(),db);
}

static void deleteDB(std::ostream &) {
	CouchDB db(getTestCouch());
	db.setCurrentDB(DATABASENAME);
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
static View by_name_cacheable("_design/testview/_view/by_name", View::includeDocs);
static View by_age_group("_design/testview/_view/by_age_group");
static View by_age("_design/testview/_view/by_age");
static View by_age_list("_design/testview/_list/rowcopy/by_age");
static View age_group_height("_design/testview/_view/age_group_height");
static View age_group_height2("_design/testview/_view/age_group_height2");
static Filter age_range("_design/testview/young",0);
static View age_range_view("_design/testview/_view/young",0);




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


static void couchFindWildcard(std::ostream &a) {

	CouchDB db(getTestCouch());
	db.setCurrentDB(DATABASENAME);

	Query q(db.createQuery(by_name));
	Result res = q.prefixString({"K"}).exec();
	while (res.hasItems()) {
		Row row = res.getNext();
		a << row.key[0].getString() << ","
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

static void couchFindRangeList(std::ostream &a) {

	CouchDB db(getTestCouch());
	db.setCurrentDB(DATABASENAME);

	Query q(db.createQuery(by_age_list));
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

static void couchReduce2(std::ostream &a) {

	CouchDB db(getTestCouch());
	db.setCurrentDB(DATABASENAME);

	Query q(db.createQuery(age_group_height2));
	Result res = q.groupLevel(1).exec();

	while (res.hasItems()) {
		Row row = res.getNext();
		a << row.key[0].getUInt()<<":"
					<<(row.value["sum"].getUInt()/row.value["count"].getUInt()) << " ";
	}
}


static void couchFilterView(std::ostream &a) {

	CouchDB db(getTestCouch());
	db.setCurrentDB(DATABASENAME);

	ChangesSink chsink(db.createChangesSink());
	chsink.setFilter(Filter(age_range_view,false));
	Changes changes = chsink.exec();

	a << changes.length();
}

static void couchFilter(std::ostream &a) {

	CouchDB db(getTestCouch());
	db.setCurrentDB(DATABASENAME);

	ChangesSink chsink(db.createChangesSink());
	chsink.setFilter(age_range).arg("agemin",40).arg("agemax",60);
	Changes changes = chsink.exec();

	a << changes.length();
}

void runTestQueryServer(TestSimple &tst) {

	tst.test("couchdb.qserver.createDB","") >> &rawCreateDB;
	tst.test("couchdb.qserver.loadData","12") >> &couchLoadData;
	tst.test("couchdb.qserver.findKeys","Kermit Byrd,76,184 Owen Dillard,80,151 Nicole Jordan,75,150 ") >> &couchFindKeys;
	tst.test("couchdb.qserver.findWildcard","Kenneth Meyer,42,156 Kermit Byrd,76,184 ") >> &couchFindWildcard;
	tst.test("couchdb.qserver.findGroup","Kenneth Meyer Scarlett Frazier Odette Hahn Pascale Burt Bevis Bowen ") >> &couchFindGroup;
	tst.test("couchdb.qserver.findRange","Daniel Cochran Ramona Lang Urielle Pennington ") >> &couchFindRange;
	tst.test("couchdb.qserver.findRangeList","Daniel Cochran Ramona Lang Urielle Pennington ") >> &couchFindRangeList;
	tst.test("couchdb.qserver.reduce","20:178 30:170 40:171 50:165 70:167 80:151 ") >> &couchReduce;
	tst.test("couchdb.qserver.reduce2","20:178 30:170 40:171 50:165 70:167 80:151 ") >> &couchReduce2;
	tst.test("couchdb.qserver.filterView","3") >> &couchFilterView;
	tst.test("couchdb.qserver.filter","6") >> &couchFilter;
	tst.test("couchdb.qserver.deleteDB","") >> &deleteDB;
}



}
