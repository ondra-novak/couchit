#include <imtjson/json.h>
#include <lightspeed/base/text/textstream.tcc>
#include "lightspeed/base/framework/testapp.h"
#include <algorithm>

#include "../lightcouch/minihttp/hdrrd.h"
#include "../lightcouch/minihttp/hdrwr.h"
#include "../lightcouch/minihttp/chunked.h"
#include "../lightcouch/minihttp/httpclient.h"


namespace LightCouch {
using namespace LightSpeed;


defineTest test_parseHeaders("couchdb.minihttp.parseHeaders",
		"{\"Cache-Control\":\"no-cache, no-store, must-revalidate\",\"Connection\":\"close\",\"Content-Length\":\"167363\",\"Content-Type\":\"text\\/html; charset=UTF-8\",\"Date\":\"Thu, 10 Nov 2016 14:37:29 GMT\",\"Pragma\":\"no-cache\",\"Server\":\"nginx\",\"Vary\":\"Accept-Encoding\",\"_message\":\"OK\",\"_status\":200,\"_version\":\"HTTP\\/1.1\"}"
		,[](PrintTextA &print){

	const char *data = "HTTP/1.1 200 OK\r\n"
			"Server: nginx\r\n"
			"Date: Thu, 10 Nov 2016 14:37:29 GMT\r\n"
			"Content-Type: text/html; charset=UTF-8\r\n"
			"Content-Length: 167363\r\n"
			"Connection: close\r\n"
			"Vary: Accept-Encoding\r\n"
			"Vary: Accept-Encoding\r\n"
			"Cache-Control: no-cache, no-store, must-revalidate\r\n"
			"Pragma: no-cache\r\n"
			"\r\n"
			"FakeHeader:blaaaa-bla-blaaaa\r\n"
			"\r\n";


	auto fn = [&]()->int {return *data?*data++:-1;};
	HeaderRead<decltype(fn)> reader(fn);
	json::Value v = reader.parseHeaders();
	json::String res = v.stringify();
	print("%1") << res.c_str();


});

defineTest test_serializeHeaders("couchdb.minihttp.serializeHeaders",
		"POST /example/path/site.html HTTP/1.1\r\n"
		"Accept: */*\r\n"
		"Connection: close\r\n"
		"Content-Length: 123412\r\n"
		"Host: www.example.com\r\n"
		"User-Agent: LightCouch MiniHttp\r\n"
		"\r\n",
		[](PrintTextA &print) {

	Object hdr;
	hdr("_method","POST")
	  ("_uri","/example/path/site.html")
	  ("_version","HTTP/1.1")
	  ("User-Agent","LightCouch MiniHttp")
	  ("Accept","*/*")
	  ("Host","www.example.com")
	  ("Connection","close")
	  ("Content-Length",123412);



	std::string out;
	out.reserve(2000);
	auto fn = [&](char c){out.push_back(c);};
	HeaderWrite<decltype(fn)> writer(fn);
	writer.serialize(hdr);
	print("%1") << StrView(out);
});

defineTest test_httpcReadChunked("couchdb.minihttp.readChunked", "Wikipedia in\r\n\r\nchunks.-Wikipedia in\r\n\r\nchunks.", [](PrintTextA &out) {

	StrView data =
		"4\r\n"
		"Wiki\r\n"
		"5\r\n"
		"pedia\r\n"
		"E\r\n"
		" in\r\n"
		"\r\n"
		"chunks.\r\n"
		"0\r\n"
		"\r\n"
		"Blableblaebalbqeq";
	std::size_t pos = 0;
	auto infn = [&data,&pos](std::size_t processed, std::size_t *ready) -> const unsigned char * {
		pos+=processed;
		std::size_t l = data.length();
		if (ready) *ready = std::min(l - pos,std::size_t(10));
		return reinterpret_cast<const unsigned char *>(pos >= l?nullptr:data.data()+pos);
	};
	std::string res;
	res.reserve(2000);
	{
	ChunkedRead<decltype(infn)> chunks(infn);
	BufferedRead<ChunkedRead<decltype(infn)> > buffered(chunks);
	do {
		int i = buffered();
		if (i == -1) break;
		res.push_back((char)i);
	} while (true);
	}

	std::string res2;
	res2.reserve(2000);
	 {
		pos = 0;
		ChunkedRead<decltype(infn)> chunks(infn);
		std::size_t rd;
		const unsigned char *c = chunks(0,&rd);
		while (c != 0 && rd != 0) {
			if (rd > 4) rd = 4;
			res2.append(reinterpret_cast<const char *>(c), rd);
			c = chunks(rd,&rd);
		}
	}

	out("%1-%2") << StrView(res) << StrView(res2);

});

defineTest test_writeChunked("couchdb.minihttp.writeChunked",
		"14\r\nThis is long string \r\n14\r\nwritten in chunks...\r\n2\r\n..\r\n0\r\n\r\n",
		[](PrintTextA &out) {

	StrView source = "This is long string written in chunks.....";
	std::string res;
	res.reserve(1000);

	auto outfn = [&](const unsigned char *data, std::size_t length, std::size_t *written) {
		res.append(reinterpret_cast<const char *>(data),length);
		if (written) *written = length;
		return true;
	};
	ChunkedWrite<decltype(outfn),20> chunks(outfn);

	for (auto &&c : source) chunks(c);
	chunks(-1);

	out("%1") << StrView(res);

});
defineTest test_writeChunked2("couchdb.minihttp.writeChunked2",
		"5\r\nThis \r\n13\r\nis long string writ\r\n47\r\nten in chunks..... And because it should be long enough, make it longer\r\n0\r\n\r\n",
		[](PrintTextA &out) {

	StrView source = "This is long string written in chunks..... And because it should be long enough, make it longer";
	std::string res;
	res.reserve(1000);

	auto outfn = [&](const unsigned char *data, std::size_t length, std::size_t *written) {
		res.append(reinterpret_cast<const char *>(data),length);
		if (written) *written = length;
		return true;
	};
	ChunkedWrite<decltype(outfn),20> chunks(outfn);

	const unsigned char *data = reinterpret_cast<const unsigned char *>(source.data());
	chunks(data,1,0);
	chunks(data+1,4,0);
	chunks(data+5,19,0);
	chunks(data+24,source.length()-24,0);
	chunks(0,0,0);

	out("%1") << StrView(res);

});
defineTest test_simpleGetRequest("couchdb.minihttp.getRequest","TestClient",
		[](PrintTextA &out) {

	HttpClient client("TestClient");
	client.open("http://httpbin.org/get","GET",false);
	int status = client.send();
	if (status == 200) {
		json::Value v = json::Value::parse(BufferedRead<InputStream>(client.getResponse()));
		out("%1") << v["headers"]["User-Agent"].getString();
	} else {
		out("Status: %1") << status;
	}


});
defineTest test_simplePostRequest("couchdb.minihttp.postRequest",
		"\"[10,20,30,\\\"ahoj\\\",\\\"nazdar\\\",\\\"cau\\\",[1,2,3]]\"",
		[](PrintTextA &out) {

	HttpClient client("TestClient");
	client.open("http://httpbin.org/post","POST",false);
	json::Value req({10,20,30,"ahoj","nazdar","cau",{1,2,3}});
	req.serialize(BufferedWrite<OutputStream>(client.beginBody()));
	int status = client.send();
	if (status == 200) {
		json::Value v = json::Value::parse(BufferedRead<InputStream>(client.getResponse()));
		out("%1") << StrView(v["data"].stringify());
	} else {
		out("Status: %1") << status;
	}


});

defineTest test_postRequestFromString("couchdb.minihttp.postRequestFromString",
		"\"[10,20,30,\\\"ahoj\\\",\\\"nazdar\\\",\\\"cau\\\",[1,2,3]]\"",
		[](PrintTextA &out) {

	HttpClient client("TestClient");
	client.open("http://httpbin.org/post","POST",false);
	json::Value req({10,20,30,"ahoj","nazdar","cau",{1,2,3}});
	json::String strReq = req.stringify();
	int status = client.send(strReq);
	if (status == 200) {
		json::Value v = json::Value::parse(BufferedRead<InputStream>(client.getResponse()));
		out("%1") << StrView(v["data"].stringify());
	} else {
		out("Status: %1") << status;
	}


});
defineTest test_getAuth("couchdb.minihttp.getAuth","user",
		[](PrintTextA &out) {

	HttpClient client("TestClient");
	client.open("http://user:passwd@httpbin.org/basic-auth/user/passwd","GET",false);
	int status = client.send();
	if (status == 200) {
		json::Value v = json::Value::parse(BufferedRead<InputStream>(client.getResponse()));
		out("%1") << v["user"].getString();
	} else {
		out("Status: %1") << status;
	}


});



}
