#include "lightspeed/base/framework/app.h"
#include "lightspeed/base/framework/testapp.h"
#include "lightspeed/base/streams/standardIO.tcc"
namespace LightSpeedTest {

	using namespace LightSpeed;


	class Main : public App {
	public:
		
		virtual integer start(const Args &args);

	};

	LightSpeed::integer Main::start(const Args &args)
	{
		ConsoleA console;
		integer retval = 0;
		TestCollector &collector = Singleton<TestCollector>::getInstance();
		if (args.length() < 2) {
			SeqFileOutBuff<> out(StdOutput().getStream());
			collector.runTests(ConstStrW(),out);
		}
		else for (natural i = 1; i < args.length(); i++) {
			ConstStrW itm = args[i];
			if (itm == L"list") {
				StringA lst = collector.listTests();
				console.print("%1") << lst;
			}
			else {
				SeqFileOutBuff<> out(StdOutput().getStream());
				if (collector.runTests(args[i], out )) retval = 1;
			}
		}
		return retval;
	}


	static Main theApp;
}
