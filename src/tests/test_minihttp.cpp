#include <imtjson/json.h>
#include <algorithm>

#include "../couchit/minihttp/hdrrd.h"
#include "../couchit/minihttp/hdrwr.h"
#include "../couchit/minihttp/chunked.h"
#include "../couchit/minihttp/httpclient.h"
#include "testClass.h"


namespace couchit {


void runMiniHttpTests(TestSimple &tst) {

tst.test("couchdb.minihttp.parseHeaders",
		"{\"Cache-Control\":\"no-cache, no-store, must-revalidate\",\"Connection\":\"close\",\"Content-Length\":\"167363\",\"Content-Type\":\"text\\/html; charset=UTF-8\",\"Date\":\"Thu, 10 Nov 2016 14:37:29 GMT\",\"Pragma\":\"no-cache\",\"Server\":\"nginx\",\"Vary\":\"Accept-Encoding\",\"_message\":\"OK\",\"_status\":200,\"_version\":\"HTTP\\/1.1\"}")
		>> [](std::ostream &print){

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
	print << res.c_str();

};

tst.test("couchdb.minihttp.serializeHeaders",
		"POST /example/path/site.html HTTP/1.1\r\n"
		"Accept: */*\r\n"
		"Connection: close\r\n"
		"Content-Length: 123412\r\n"
		"Host: www.example.com\r\n"
		"User-Agent: couchit MiniHttp\r\n"
		"\r\n") >> [](std::ostream &print){

	Object hdr;
	hdr("_method","POST")
	  ("_uri","/example/path/site.html")
	  ("_version","HTTP/1.1")
	  ("User-Agent","couchit MiniHttp")
	  ("Accept","*/*")
	  ("Host","www.example.com")
	  ("Connection","close")
	  ("Content-Length",123412);



	std::string out;
	out.reserve(2000);
	auto fn = [&](char c){out.push_back(c);};
	HeaderWrite<decltype(fn)> writer(fn);
	writer.serialize(hdr);
	print << StrViewA(out);
};

tst.test("couchdb.minihttp.readChunked", "Wikipedia in\r\n\r\nchunks.-Wikipedia in\r\n\r\nchunks.") >> [](std::ostream &print){

	StrViewA data =
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
	auto infn = [&data,&pos](std::size_t processed) -> BinaryView {
		pos+=processed;
		std::size_t l = data.length;
		return BinaryView(reinterpret_cast<const unsigned char *>(pos >= l ? nullptr : data.data + pos),
			std::min(l - pos, std::size_t(10)));
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
		BinaryView c = chunks(0);
		while (!c.empty()) {
			std::size_t rd = c.length;
			if (rd > 4) rd = 4;
			res2.append(reinterpret_cast<const char *>(c.data), rd);
			c = chunks(rd);
		}
	}

	print << res << "-" << res2;

};

tst.test("couchdb.minihttp.writeChunked",
		"14\r\nThis is long string \r\n14\r\nwritten in chunks...\r\n2\r\n..\r\n0\r\n\r\n") >> [](std::ostream &print){

	StrViewA source = "This is long string written in chunks.....";
	std::string res;
	res.reserve(1000);

	auto outfn = [&](BinaryView data) {
		res.append(reinterpret_cast<const char *>(data.data),data.length);
	};
	ChunkedWrite<decltype(outfn),20> chunks(outfn);

	for (auto &&c : source) chunks(c);
	chunks(-1);

	print << StrViewA(res);

};
tst.test("couchdb.minihttp.writeChunked2",
		"5\r\nThis \r\n13\r\nis long string writ\r\n47\r\nten in chunks..... And because it should be long enough, make it longer\r\n0\r\n\r\n"
		) >> [](std::ostream &print){

	StrViewA source = "This is long string written in chunks..... And because it should be long enough, make it longer";
	std::string res;
	res.reserve(1000);

	auto outfn = [&](BinaryView data) {
		res.append(reinterpret_cast<const char *>(data.data),data.length);
	};
	ChunkedWrite<decltype(outfn),20> chunks(outfn);

	const unsigned char *data = reinterpret_cast<const unsigned char *>(source.data);
	chunks(BinaryView(data,1));
	chunks(BinaryView(data+1,4));
	chunks(BinaryView(data+5,19));
	chunks(BinaryView(data+24,source.length-24));
	chunks(nullptr);

	print << res;

};
tst.test("couchdb.minihttp.getRequest","TestClient"
		) >> [](std::ostream &out){

	HttpClient client("TestClient");
	client.open("http://httpbin.org/get","GET",false);
	int status = client.send();
	if (status == 200) {
		json::Value v = json::Value::parse(BufferedRead<InputStream>(client.getResponse()));
		out << v["headers"]["User-Agent"].getString();
	} else {
		out << "Status: " << status;
	}

};
tst.test("couchdb.minihttp.postRequest",
		"\"[10,20,30,\\\"ahoj\\\",\\\"nazdar\\\",\\\"cau\\\",[1,2,3]]\""
		) >> [](std::ostream &out){

	HttpClient client("TestClient");
	client.open("http://httpbin.org/post","POST",false);
	json::Value req({10,20,30,"ahoj","nazdar","cau",{1,2,3}});
	req.serialize(BufferedWrite<OutputStream>(client.beginBody()));
	int status = client.send();
	if (status == 200) {
		json::Value v = json::Value::parse(BufferedRead<InputStream>(client.getResponse()));
		out << StrViewA(v["data"].stringify());
	} else {
		out<< "Status: " << status;
	}
};

tst.test("couchdb.minihttp.postRequestFromString",
		"\"[10,20,30,\\\"ahoj\\\",\\\"nazdar\\\",\\\"cau\\\",[1,2,3]]\""
		) >> [](std::ostream &out){

	HttpClient client("TestClient");
	client.open("http://httpbin.org/post","POST",false);
	json::Value req({10,20,30,"ahoj","nazdar","cau",{1,2,3}});
	json::String strReq = req.stringify();
	int status = client.send(strReq);
	if (status == 200) {
		json::Value v = json::Value::parse(BufferedRead<InputStream>(client.getResponse()));
		out  << StrViewA(v["data"].stringify());
	} else {
		out << "Status:" << status;
	}


};
tst.test("couchdb.minihttp.getAuth","user"
		) >> [](std::ostream &out){

	HttpClient client("TestClient");
	client.open("http://user:passwd@httpbin.org/basic-auth/user/passwd","GET",false);
	int status = client.send();
	if (status == 200) {
		json::Value v = json::Value::parse(BufferedRead<InputStream>(client.getResponse()));
		out << v["user"].getString();
	} else {
		out << "Status: " << status;
	}


};


}

}
