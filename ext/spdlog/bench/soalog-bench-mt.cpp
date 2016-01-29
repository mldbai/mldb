#include <time.h>
#include <thread>
#include <vector>
#include <atomic>
#include <cstdlib>
#include <unistd.h>
#include <fcntl.h>
#include "soa/service/logs.cc"
#include "soa/jml/arch/exception.cc"
#include "soa/jml/utils/exc_check.cc"
#include "soa/jml/arch/format.cc"
#include "soa/jml/utils/abort.cc"
#include <boost/lexical_cast.hpp>

struct Level
{
    static Datacratic::Logging::Category info;
};

Datacratic::Logging::Category Level::info("info");


using namespace std;

int main(int argc, char* argv[])
{
    int thread_count = 10;
    bool deactivate = false;

    if(argc > 2) {
        thread_count = atoi(argv[1]);
        deactivate = boost::lexical_cast<bool>(argv[2]);
    } else {
        std::cerr << "require two arguments [thread] [deactivate]" << endl;
        return -1;
    }

    cout << "running with " << thread_count 
         << " thread(s) and deactivate set to " 
         << boolalpha << deactivate << endl;

    ostringstream logpath;
    logpath << "logs/soa_log_" << thread_count << ".log";

    Level::info.writeTo(make_shared<Datacratic::Logging::FileWriter>(logpath.str()));
    if (deactivate)
        Level::info.deactivate(true);

    int howmany = 1000000;
    std::atomic<int > msg_counter {0};
    vector<thread> threads;
    
    for (int t = 0; t < thread_count; ++t)
    {
        threads.push_back(std::thread([&]()
             {
                 while (true)
                 {
                     int counter = ++msg_counter;
                     if (counter > howmany) break;
                     if (true)
                         LOG(Level::info) << "soa_log message #" << counter << ": This is some text for your pleasure" << std::endl;
                 }
             }));
    }

    for (auto &t:threads)
    {
        t.join();
    };
    return 0;
}
