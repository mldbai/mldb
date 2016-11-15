/** mpu6050_test.cc
    Jeremy Barnes, 7 November 2016
    Copyright (c) 2016 mldb.ai Inc.  All rights reserved.
    
*/

#include "mldb/sensors/i2c.h"
#include "mldb/types/date.h"
#include "mldb/types/basic_value_descriptions.h"
#include "mldb/types/json.h"
#include "mldb/arch/endian.h"
#include <iostream>

#define DEBUG 1

#include "mldb/sensors/ext/i2cdevlib/Arduino/ADS1115/ADS1115.h"

#define BOOST_TEST_MAIN
#define BOOST_TEST_DYN_LINK

#include <boost/test/unit_test.hpp>

using namespace std;

using namespace MLDB;

BOOST_AUTO_TEST_CASE( test_ads1115 )
{
    I2CBus bus("/dev/i2c-1");
    //cerr << jsonEncodeStr(bus.capabilities()) << endl;
    cerr << jsonEncodeStr(bus.scan()) << endl;

    ADS1115 adc(0x48);
    adc.initialize();
    cerr << "connection ok: " << adc.testConnection() << endl;
    adc.setMultiplexer(ADS1115_MUX_P1_NG);
    adc.setRate(ADS1115_RATE_860);
    adc.setMode(ADS1115_MODE_CONTINUOUS);
    cerr << "mode is " << (int)adc.getMode() << endl;

    cerr << "config is " << endl;
    adc.showConfigRegister();

    cerr << "low threshold is " << std::hex << adc.getLowThreshold() << endl;
    cerr << "high threshold is " << adc.getHighThreshold() << endl;

    adc.setLowThreshold(0x1234);
    adc.setHighThreshold(0x5678);

    cerr << "low threshold is " << adc.getLowThreshold() << endl;
    cerr << "high threshold is " << adc.getHighThreshold() << endl;
    cerr << "low threshold is " << adc.getLowThreshold() << endl;
    cerr << "high threshold is " << adc.getHighThreshold() << std::dec << endl;

    Date start = Date::now();

    int niter = 100;

    for (size_t i = 0;  i < niter;  ++i) {

        float mv = adc.getMilliVolts(false /* triggerAndPoll */);

        cerr << format("%.6f ", Date::now().secondsSince(start))
             << " " << mv << "mv" << endl;
    }

    double elapsed = Date::now().secondsSince(start);

    cerr << "read " << niter << " samples in "
         << elapsed << " seconds at " << elapsed / niter * 1000.0 << "ms/call or "
         << niter / elapsed << " samples/second" << endl;
}
