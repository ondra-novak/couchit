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
	virtual void map(const Value &doc) override {
		emit(doc["name"],{doc["age"] ,doc["height"]} );
	}

};


class LocalViewAgeByGroup: public LocalView {
public:
	virtual void map(const Value &doc) override {
		emit({doc["age"].getUInt()/10 * 10 ,doc["age"]},doc["name"]);
	}

};

class LocalViewByAge: public LocalView {
public:
	virtual void map(const Value &doc) override {
		emit(doc["age"],doc["name"]);
	}
};

class LocalView_age_group_height: public LocalView {
public:
	virtual void map(const Value &doc) override {
		emit({doc["age"].getUInt()/10 * 10 ,doc["age"]},doc["height"]);
	}
	virtual Value reduce(const ConstStringT<KeyAndDocId>  &, const ConstStringT<Value> &values, bool rereduce) const override {
		if (rereduce) throwUnsupportedFeature(THISLOCATION,this,"rereduce not implemented");
		natural sum = 0;
		natural count = values.length();
		for(natural i = 0; i<count;i++) sum+=values[i].getUInt();
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
			("_id",StringRef(gen(buffer,"")));
		view.updateDoc(doc);
	}

}

static void localView_ByName(PrintTextA &print) {

	LocalViewByName view;
	loadData(view);

	LocalView::Query q = view.createQuery(0);
	Result res = q.select("Kermit Byrd")
					 .select("Owen Dillard")
					 .select("Nicole Jordan")
					 .exec();
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

	LocalView::Query q = view.createQuery(0);
	Result res = q("K")(Query::wildcard).exec();
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

	LocalView::Query q = view.createQuery(0);
	Result res = q(40)(Query::any).exec();
	while (res.hasItems()) {
		Row row = res.getNext();
		a("%1 ") << row.value.getString();
	}
}

static void localView_FindRange(PrintTextA &a) {

	LocalViewByAge view;
	loadData(view);

	LocalView::Query q = view.createQuery(0);
	Result res = q.from(20).to(40).reverseOrder().exec();
	while (res.hasItems()) {
		Row row = res.getNext();
		a("%1 ") << row.value.getString();
	}
}

static void localView_couchReduce(PrintTextA &a) {

	LocalView_age_group_height view;
	loadData(view);

	LocalView::Query q = view.createQuery(0);
	Result res = q.group(1).exec();

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


}




