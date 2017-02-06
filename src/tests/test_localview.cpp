/*
 * test_localview.cpp
 *
 *  Created on: 7. 6. 2016
 *      Author: ondra
 */

#include "../lightcouch/document.h"
#include "../lightcouch/localView.h"
#include "../lightcouch/defaultUIDGen.h"
#include "lightspeed/base/framework/testapp.h"

#include "test_common.h"
#include <lightspeed/base/text/textstream.tcc>

namespace LightCouch {


static const char *strdata="[[\"Kermit Byrd\",76,184],[\"Odette Hahn\",44,181],"
		"[\"Scarlett Frazier\",43,183],[\"Pascale Burt\",46,153],"
		"[\"Urielle Pennington\",21,166],[\"Bevis Bowen\",47,185],"
		"[\"Dakota Shepherd\",52,165],[\"Ramona Lang\",23,190],"
		"[\"Nicole Jordan\",75,150],[\"Owen Dillard\",80,151],"
		"[\"Daniel Cochran\",36,170],[\"Kenneth Meyer\",42,156]]";


class LocalViewByName: public LocalView {
public:
	virtual void map(const Document &doc) override {
		Value k ({doc["name"]});
		Value v ({doc["age"] ,doc["height"]});
		emit(k,v);
	}

};


class LocalViewAgeByGroup: public LocalView {
public:
	virtual void map(const Document &doc) override {
		emit({doc["age"].getUInt()/10 * 10 ,doc["age"]},doc["name"]);
	}

};

class LocalViewByAge: public LocalView {
public:
	virtual void map(const Document &doc) override {
		emit(doc["age"],doc["name"]);
	}
};

class LocalView_age_group_height: public LocalView {
public:
	virtual void map(const Document &doc) override {
		emit({doc["age"].getUInt()/10 * 10 ,doc["age"]},doc["height"]);
	}
	virtual Value reduce(const RowsWithKeys &rwk) const override {
		std::size_t sum = 0;
		std::size_t count = rwk.length;
		for(std::size_t i = 0; i<count;i++) sum+=rwk[i].value.getUInt();
		return Object("sum",sum)("count",count);
	}
};


static void loadData(LocalView &view) {

	DefaultUIDGen &gen = DefaultUIDGen::getInstance();
	AutoArray<char> buffer;

	AutoArray<Document, SmallAlloc<50> > savedDocs;

	Value data = Value::fromString(strdata);
	for (auto &&kv: data) {
		Document doc;
		doc("name",kv[0])
			("age",kv[1])
			("height",kv[2])
			("_id",gen(buffer,""));
		view.updateDoc(doc);
	}

}

static void localView_ByName(PrintTextA &print) {

	LocalViewByName view;
	loadData(view);

	Query q = view.createQuery(0);
	Result res = q.keys({
		{"Kermit Byrd"},
		{"Owen Dillard"},
		{"Nicole Jordan"}
			}).exec();
	while (res.hasItems()) {
		Row row = res.getNext();
		print("%1,%2,%3 ") << row.key[0].getString()
				<<row.value[0].getUInt()
				<<row.value[1].getUInt();
	}
}


static void localView_wildcard(PrintTextA &print) {
	LocalViewByName view;
	loadData(view);

	Query q = view.createQuery(0);
	Result res = q.prefixString({"K"}).exec();
	while (res.hasItems()) {
		Row row = res.getNext();
		print("%1,%2,%3 ") << row.key[0].getString()
				<<row.value[0].getUInt()
				<<row.value[1].getUInt();
	}

}

static void localView_FindGroup(PrintTextA &a) {

	LocalViewAgeByGroup view;
	loadData(view);

	Query q = view.createQuery(0);
	Result res = q.prefixKey(40).exec();
	while (res.hasItems()) {
		Row row = res.getNext();
		a("%1 ") << row.value.getString();
	}
}

static void localView_FindRange(PrintTextA &a) {

	LocalViewByAge view;
	loadData(view);

	Query q = view.createQuery(0);
	Result res = q.range(20,40).reversedOrder().exec();
	while (res.hasItems()) {
		Row row = res.getNext();
		a("%1 ") << row.value.getString();
	}
}

static void localView_couchReduce(PrintTextA &a) {

	LocalView_age_group_height view;
	loadData(view);

	Query q = view.createQuery(0);
	Result res = q.groupLevel(1).exec();

	while (res.hasItems()) {
		Row row = res.getNext();
		a("%1:%2 ") << row.key[0].getUInt()
				<<(row.value["sum"].getUInt()/row.value["count"].getUInt());
	}
}

static void localView_couchReduceAll(PrintTextA &a) {

	LocalView_age_group_height view;
	loadData(view);

	Query q = view.createQuery(0);
	Result res = q.reduceAll().exec();

	while (res.hasItems()) {
		Row row = res.getNext();
		a("%1:%2 ") << row.key[0].getUInt()
				<<(row.value["sum"].getUInt()/row.value["count"].getUInt());
	}
}



defineTest test_localView_byName("couchdb.localview.byName","Kermit Byrd,76,184 Owen Dillard,80,151 Nicole Jordan,75,150 ",&localView_ByName);
defineTest test_localView_wildcard("couchdb.localview.wildcard","Kenneth Meyer,42,156 Kermit Byrd,76,184 ",&localView_wildcard);
defineTest test_localView_findgroup("couchdb.localview.findgroup","Kenneth Meyer Scarlett Frazier Odette Hahn Pascale Burt Bevis Bowen ",&localView_FindGroup);
defineTest test_localView_findrange("couchdb.localview.findrange","Daniel Cochran Ramona Lang Urielle Pennington ",&localView_FindRange);
defineTest test_localview_couchReduce("couchdb.localview.reduce","20:178 30:170 40:171 50:165 70:167 80:151 ",&localView_couchReduce);
defineTest test_localview_couchReduceAll("couchdb.localview.reduceAll","0:169 ",&localView_couchReduceAll);


}




