#include "lightspeed/base/framework/testapp.h"
#include "lightspeed/base/text/textstream.tcc"
#include "lightspeed/utils/json/jsonfast.tcc"
#include "lightspeed/utils/json/jsonbuilder.h"
#include "../lightcouch/couchDB.h"
#include "../lightcouch/exception.h"

#include "test_common.h"
namespace LightCouch {
using namespace LightSpeed;
using namespace BredyHttpClient;

#define DATABASENAME "lightcouch_unittest_raw"

static void rawConnect(PrintTextA &print) {

	CouchDB db(getTestCouch());

	CouchDB::HttpReq response(db,CouchDB::GET,"/");
	SeqFileInput in(response.getBody());
	JSON::Value v = JSON::fromStream(in);
	print("%1") << v["couchdb"]->getStringUtf8();
}


static void rawCreateDB(PrintTextA &print) {
	CouchDB db(getTestCouch());
	CouchDB::HttpReq response(db,CouchDB::PUT,"/" DATABASENAME);
	if (response.getStatus() != 201) throw RequestError(THISLOCATION,response.getStatus(), response.getStatusMessage(),null);
	SeqFileInput in1(response.getBody());
	JSON::Value v1 = JSON::fromStream(in1);
	print("%1") << (v1["ok"]->getBool()?"true":"false");
}

static void rawDeleteDB(PrintTextA &print) {
	CouchDB db(getTestCouch());
	CouchDB::HttpReq response(db,CouchDB::DELETE,"/" DATABASENAME);
	SeqFileInput in1(response.getBody());
	if (response.getStatus() != 200)  throw RequestError(THISLOCATION,response.getStatus(), response.getStatusMessage(),null);
	JSON::Value v1 = JSON::fromStream(in1);
	print("%1") << (v1["ok"]->getBool()?"true":"false");
}

static void rawCreateDoc(PrintTextA &print) {
	NetworkStreamSource src(NetworkAddress("localhost",5984),naturalNull, 10000,10000);
	CouchDB db(getTestCouch());
	JSON::Builder json;
	JSON::Value doc = json("aaa",1)("bbb","Ahoj");
	CouchDB::HttpReq response(db,CouchDB::PUT,"/" DATABASENAME "/test",JSON::toString(doc,true));
	SeqFileInput in1(response.getBody());
	if (response.getStatus() != 201)  throw RequestError(THISLOCATION,response.getStatus(), response.getStatusMessage(),null);
	JSON::Value v1 = JSON::fromStream(in1);
	print("%1") << (v1["ok"]->getBool()?"true":"false");

}




defineTest test_rawconnect("couchdb.rawconnect","Welcome",&rawConnect);
defineTest test_rawCreateDB("couchdb.rawCreateDb","true",&rawCreateDB);
defineTest test_rawCreateDoc("couchdb.rawCreateDoc","true",&rawCreateDoc);
defineTest test_rawDeleteDB("couchdb.rawDeleteDB","true",&rawDeleteDB);


}



