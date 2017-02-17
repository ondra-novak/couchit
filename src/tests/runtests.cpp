#include <imtjson/stringview.h>
#include <fstream>


#include "testClass.h"


static const char *lang = "couchit_test";


namespace couchit {


	void runMiniHttpTests(TestSimple &tst) ;
	void testUUIDs(TestSimple &tst) ;
	void runTestBasics(TestSimple &tst);
	void runTestLocalview(TestSimple &tst);
	void runTestQueryServer(const json::StrViewA &lang, TestSimple &tst);
	void runQueryServer(const json::StrViewA &lang, const json::StrViewA &chkfile);


}

int main(int argc, char *argv[]) {
	TestSimple tst;

	if (argc == 2 && json::StrViewA(argv[1]) == "qserver") {

		couchit::runQueryServer(lang, argv[0]);
		return 0;


	} else {

		std::ofstream cfg("qserver.ini", std::ios::out|std::ios::trunc);
		cfg << "[query_servers]"<<std::endl;
		cfg << lang << '=' <<argv[0] << " qserver" << std::endl;

		couchit::runTestLocalview(tst);
		couchit::runMiniHttpTests(tst);
		couchit::testUUIDs(tst);
		couchit::runTestBasics(tst);
		couchit::runTestQueryServer(lang, tst);


		return tst.didFail()?1:0;
	}

}


