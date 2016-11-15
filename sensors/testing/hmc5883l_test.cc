/** hmc5883l_test.cc
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

#include "mldb/sensors/ext/i2cdevlib/Arduino/HMC5883L/HMC5883L.h"

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

    HMC5883L compass;
    compass.initialize();
    compass.setMode(HMC5883L_MODE_CONTINUOUS);
    compass.setDataRate(HMC5883L_RATE_160);


    cerr << "connection ok: " << compass.testConnection() << endl;
    cerr << compass.getIDA() << compass.getIDB() << compass.getIDC() << endl;

    Date start = Date::now();

    int niter = 100;

    for (size_t i = 0;  i < niter;  ++i) {

        bool ready = compass.getReadyStatus();

        int16_t x, y, z;
        compass.getHeading(&x, &y, &z);

        cerr << format("%.6f ", Date::now().secondsSince(start))
             << ready << " " << x << " " << y << " " << z << endl;
    }
    
    double elapsed = Date::now().secondsSince(start);

    cerr << "read " << niter << " samples in "
         << elapsed << " seconds at " << elapsed / niter * 1000.0 << "ms/call or "
         << niter / elapsed << " samples/second" << endl;
}
