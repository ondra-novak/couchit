/*
 * test_conflict.cpp
 *
 *  Created on: 11. 6. 2016
 *      Author: ondra
 */


#include "../lightcouch/changeset.h"
#include "../lightcouch/document.h"
#include "lightspeed/base/framework/testapp.h"

#include "test_common.h"
#include <lightspeed/base/text/textstream.tcc>
#include <string.h>

#include "../lightcouch/attachment.h"
#include "../lightcouch/conflictResolver.h_"
using LightCouch::getTestCouch;

#define DATABASENAME "lightcouch_unittest_conflicts"


namespace LightCouch {
using namespace LightSpeed;


static const char *baseRev="{"
		            "   \"_id\":\"test\","
					"   \"_rev\":\"10\","
					"   \"commonVal\":\"aaa\","
		            "   \"delVal1\":\"bbb\","
		            "   \"delVal2\":\"ccc\","
		            "   \"delConflict\":\"ddd\","
		            "   \"modVal1\":10,"
		            "   \"modVal2\":20,"
		            "   \"modValConflict\":30,"
		            "   \"subobject\": {"
							"   \"sub_commonVal\":\"aaa\","
							"   \"sub_delVal1\":\"bbb\","
							"   \"sub_delVal2\":\"ccc\","
							"   \"sub_delConflict\":\"ddd\","
							"   \"sub_modVal1\":10,"
							"   \"sub_modVal2\":20,"
							"   \"sub_modValConflict\":30"
					"     },"
					"    \"arr\":[\"ahoj\",40,true]"
					"}";

static const char *curRev="{"
		            "   \"_id\":\"test\","
					"   \"_rev\":\"11\","
					"   \"commonVal\":\"aaa\","
		            "   \"delVal2\":\"ccc\","
		            "   \"modVal1\":110,"
		            "   \"modVal2\":20,"
		            "   \"modValConflict\":150,"
					"   \"insert1\":\"ins1\","
					"   \"insertConflict\":\"xxx\","
					"   \"bothInsert\":\"zzz\","
		            "   \"subobject\": {"
							"   \"sub_commonVal\":\"aaa\","
							"   \"sub_delVal2\":\"ccc\","
							"   \"sub_modVal1\":121,"
							"   \"sub_modVal2\":20,"
							"   \"sub_modValConflict\":60"
					"     },"
					"    \"arr\":[\"ahoj\",true,false]"
					"}";

static const char *conflictedRev="{"
		            "   \"_id\":\"test\","
					"   \"_rev\":\"11\","
					"   \"commonVal\":\"aaa\","
		            "   \"delVal1\":\"bbb\","
		            "   \"modVal1\":10,"
		            "   \"modVal2\":25,"
		            "   \"modValConflict\":98,"
					"   \"insert2\":\"ins2\","
				    "   \"insertConflict\":\"yyy\","
					"   \"bothInsert\":\"zzz\","
		            "   \"subobject\": {"
							"   \"sub_commonVal\":\"aaa\","
							"   \"sub_delVal1\":\"bbb\","
							"   \"sub_modVal1\":10,"
							"   \"sub_modVal2\":55,"
							"   \"sub_modValConflict\":77"
					"     },"
					"    \"arr\":[\"ahoj\",40,true,\"added\"]"
					"}";



static void resolveConflicts(PrintTextA &a) {

	CouchDB cdb(getTestCouch());

	class TestResolver: public ConflictResolver {
	public:
		TestResolver(CouchDB &cdb,PrintTextA &a):ConflictResolver(cdb),a(a) {}

		virtual Value resolveConflict(Document &doc, const Path &path, const Value &leftValue, const ConstValue &rightValue) {
			a("Conflict: %1, ") << path.getKey();
			return ConflictResolver::resolveConflict(doc,path,leftValue,rightValue);
		}

	protected:
		PrintTextA &a;
	};

	ConstValue baseRevVal = cdb.json.factory->fromString(baseRev);
	ConstValue curRevVal = cdb.json.factory->fromString(curRev);
	ConstValue conflRevVal = cdb.json.factory->fromString(conflictedRev);

	TestResolver resolver(cdb,a);

	ConstValue result = resolver.merge3w(curRevVal,conflRevVal, baseRevVal);

	a("%1") << cdb.json.factory->toString(*result);


}

defineTest test_resolveConflicts("couchdb.conflicts.resolver","Conflict: arr, Conflict: insertConflict, "
		"Conflict: modValConflict, Conflict: sub_modValConflict, "
		"{\"_id\":\"test\",\"_rev\":\"10\",\"arr\":[\"ahoj\",true,false],\"bothInsert\":\"zzz\","
		"\"commonVal\":\"aaa\",\"insert1\":\"ins1\",\"insert2\":\"ins2\",\"insertConflict\":\"xxx\","
		"\"modVal1\":110,\"modVal2\":25,\"modValConflict\":150,\"subobject\":{\"sub_commonVal\":\"aaa\","
		"\"sub_modVal1\":121,\"sub_modVal2\":55,\"sub_modValConflict\":60}}",&resolveConflicts);


static void resolveConflictsFromDB(PrintTextA &a) {

	CouchDB cdb(getTestCouch());
	cdb.use(DATABASENAME);
	cdb.createDatabase();

	Changeset chset = cdb.createChangeset();

	Document doc1 = chset.json("_id","conflictTest");

	doc1.edit(cdb.json)
			("commonVal",10)
			("notChanged",true)
			("delval",14);


	chset.update(doc1);
	chset.commit();

	Document doc2 = cdb.retrieveDocument(doc1.getID(),View::attachments);
	doc2.edit(cdb.json)
			("change1",20);
	chset.update(doc2);
	chset.commit();
	doc2.revert();
	doc2.edit(cdb.json)
			("change2",40);
	doc2.unset("delval");

	chset.update(doc2);
	chset.commit();

	doc2.revert();
	doc2.edit(cdb.json)
			("change3",cdb.json("inner",true))
			("commonVal",11);
	doc2.unset("delval");

	chset.update(doc2);
	chset.commit();


	ConflictResolver resolver(cdb);
	Document doc3 = resolver.resolve(doc1.getID());
	doc3.resolveConflicts();
	chset.update(doc3);
	chset.commit();

	ConstValue res = cdb.retrieveDocument(doc1.getID(),CouchDB::flgConflicts);
	a("%1") << cdb.json.factory->toString(*res);
	cdb.deleteDatabase();
}

static void resolveAttachmentsConflicts(PrintTextA &a) {

	ConstStrA common("Common");
	ConstStrA att1("Attachment1");
	ConstStrA att2("Attachment2");
	StringA textplain("text/plain");

	CouchDB cdb(getTestCouch());
	cdb.use(DATABASENAME);
	cdb.createDatabase();

	Changeset chset = cdb.createChangeset();

	Document doc1 = chset.json("_id","attchtest");

	doc1.edit(cdb.json)
			("commonVal",10)
			("notChanged",true);
	doc1.inlineAttachment(cdb.json,"common",AttachmentDataRef(ConstBin(common.data(),common.length()), textplain));

	chset.update(doc1);
	chset.commit();

	Document doc2 = cdb.retrieveDocument(doc1.getID(),CouchDB::flgAttachments);

	doc2.edit(cdb.json);
	doc2.inlineAttachment(cdb.json,"test1",AttachmentDataRef(ConstBin(att1.data(),att1.length()),textplain));
	chset.update(doc2);
	chset.commit();
	doc2.revert();

	doc2.edit(cdb.json);
	doc2.inlineAttachment(cdb.json,"test2",AttachmentDataRef(ConstBin(att2.data(),att2.length()),textplain));
	chset.update(doc2);
	chset.commit();

	doc2.edit(cdb.json);
	doc2.deleteAttachment("common");

	chset.update(doc2);
	chset.commit();

	ConflictResolver resolver(cdb,true);
	Document doc3 = resolver.resolve(doc1.getID());
	//a("%1") << cdb.json.factory->toString(*doc3);
	doc3.resolveConflicts();
	chset.update(doc3);
	chset.commit();

	ConstValue res = cdb.retrieveDocument(doc1.getID(),CouchDB::flgConflicts);
	a("%1") << cdb.json.factory->toString(*res);
	cdb.deleteDatabase();
}

defineTest test_resolveConflictsFromDB("couchdb.conflicts.resolver2","{\"_id\":\"conflictTest\",\"_rev\":\"3-5c4e02bdc4cb4b6d35b09498036905d5\",\"change1\":20,\"change2\":40,\"change3\":{\"inner\":true},\"commonVal\":11,\"notChanged\":true}",&resolveConflictsFromDB);
defineTest test_resolveConflictsAttachments("couchdb.conflicts.attachments","{\"_attachments\":{\"test1\":"
		"{\"content_type\":\"text/plain\",\"digest\":\"md5-bpfsF2f8e1f3QFpaFBwGxQ==\",\"length\":11,"
		"\"revpos\":4,\"stub\":true},\"test2\":{\"content_type\":\"text/plain\",\"digest\":\""
		"md5-l1TAl/iENhopzRj5/Aoq2Q==\",\"length\":11,\"revpos\":4,\"stub\":true}},\"_id\":\""
		"attchtest\",\"_rev\":\"4-5c6214114a198eaa111cbb44e856a4c1\",\"commonVal\""
				":10,\"notChanged\":true}",&resolveAttachmentsConflicts);

}


