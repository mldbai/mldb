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

#include "mldb/sensors/ext/i2cdevlib/Arduino/MPU6050/MPU6050_6Axis_MotionApps20.h"

#define BOOST_TEST_MAIN
#define BOOST_TEST_DYN_LINK

#include <boost/test/unit_test.hpp>

using namespace std;

using namespace MLDB;

BOOST_AUTO_TEST_CASE( test_mpu6050 )
{
    I2CBus bus("/dev/i2c-1");
    //cerr << jsonEncodeStr(bus.capabilities()) << endl;
    //cerr << jsonEncodeStr(bus.scan()) << endl;

    constexpr int address = 0x68;
    constexpr int power_mgmt_1 = 0x6b;
    I2CDevice dev(&bus, address);
    dev.writeByte(power_mgmt_1, 0);

    MPU6050 mpu;
    mpu.initialize();
    mpu.dmpInitialize();

    cerr << "connection ok: " << mpu.testConnection() << endl;

    Date start = Date::now();

    int niter = 100;

    for (size_t i = 0;  i < niter;  ++i) {

        int16_t accelX, accelY, accelZ, gyroX, gyroY, gyroZ;
        int16_t temp = mpu.getTemperature();

        mpu.getMotion6(&accelX, &accelY, &accelZ, &gyroX, &gyroY, &gyroZ);

        cerr << format("%.6f ", Date::now().secondsSince(start))
             << " " << std::hex << accelX << " " << accelY << " " << accelZ
             << " " << gyroX << " " << gyroY << " " << gyroZ
             << " " << std::dec << (temp / 340.0) + 36.53 << "c "
             << endl;
    }

    double elapsed = Date::now().secondsSince(start);

    cerr << "read " << niter << " samples in "
         << elapsed << " seconds at " << elapsed / niter * 1000.0 << "ms/call or "
         << niter / elapsed << " samples/second" << endl;
}
