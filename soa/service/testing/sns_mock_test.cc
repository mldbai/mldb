// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

#define BOOST_TEST_MAIN
#define BOOST_TEST_DYN_LINK

#include <boost/test/unit_test.hpp>
#include "mldb/soa/service/sns.h"
#include "mldb/arch/exception.h"

using namespace std;
using namespace MLDB;

struct MockSnsApiWrapper : SnsApiWrapper {

    private:
        int cacheSize;

    public:
        std::queue<std::string> queue;

        MockSnsApiWrapper(int cacheSize = 0) : cacheSize(cacheSize){
            if (cacheSize < 0) {
                throw MLDB::Exception("Cache size cannot be below 0");
            }
        }

        void init(const std::string & accessKeyId,
                  const std::string & accessKey,
                  const std::string & fdefaultTopicArn) {}

        std::string
        publish(const std::string & message,
                int timeout = 10,
                const std::string & subject = "") {
            if (cacheSize == 0) {
                return "";
            }
            while (queue.size() >= cacheSize) {
                queue.pop();
            }
            queue.push(message);
            return "";
        }

        MockSnsApiWrapper(const std::string & accessKeyId,
                          const std::string & accessKey,
                          const std::string & defaultTopicArn) = delete;

};

BOOST_AUTO_TEST_CASE( test_mock_sns_api )
{
    auto sns = make_shared<MockSnsApiWrapper>(3);
    sns->publish("coco");
    sns->publish("caramba");
    sns->publish("caramel");

    BOOST_CHECK(sns->queue.front() == "coco");
    sns->queue.pop();
    BOOST_CHECK(sns->queue.front() == "caramba");
    sns->queue.pop();
    BOOST_CHECK(sns->queue.front() == "caramel");
    sns->queue.pop();

    sns->publish("coco");
    sns->publish("caramba");
    sns->publish("caramel");
    sns->publish("choucroute");
    sns->publish("chapelure");
    BOOST_CHECK(sns->queue.front() == "caramel");
}
