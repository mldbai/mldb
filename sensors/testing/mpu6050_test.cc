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
    cerr << "connection ok: " << mpu.testConnection() << endl;
    mpu.dmpInitialize();
    mpu.setDMPEnabled(true);
    mpu.setRate(3);  // 1khz


    Date start = Date::now();

    int niter = 100;

/* ================================================================================================ *
 | Default MotionApps v2.0 42-byte FIFO packet structure:                                           |
 |                                                                                                  |
 | [QUAT W][      ][QUAT X][      ][QUAT Y][      ][QUAT Z][      ][GYRO X][      ][GYRO Y][      ] |
 |   0   1   2   3   4   5   6   7   8   9  10  11  12  13  14  15  16  17  18  19  20  21  22  23  |
 |                                                                                                  |
 | [GYRO Z][      ][ACC X ][      ][ACC Y ][      ][ACC Z ][      ][      ]                         |
 |  24  25  26  27  28  29  30  31  32  33  34  35  36  37  38  39  40  41                          |
 * ================================================================================================ */

    union FIFOData {
        uint8_t bytes[42];
        int16_t words[21];
        
        struct {
            int32_be quatW;
            int32_be quatX;
            int32_be quatY;
            int32_be quatZ;
            int32_be gyroX;
            int32_be gyroY;
            int32_be gyroZ;
            int32_be accelX;
            int32_be accelY;
            int32_be accelZ;
            int32_be other;
        };

    } fifoData[30];


    for (size_t i = 0;  i < niter;  ++i) {

        int16_t accelX, accelY, accelZ, gyroX, gyroY, gyroZ;
        int16_t temp = mpu.getTemperature();

        mpu.getMotion6(&accelX, &accelY, &accelZ, &gyroX, &gyroY, &gyroZ);

        size_t fifoAvail = mpu.getFIFOCount();
        //cerr << fifoAvail << " bytes available from the FIFO" << endl;
        
        mpu.getFIFOBytes(fifoData[0].bytes, fifoAvail);

        //for (size_t i = 0;  i < 21;  ++i) {
        //    cerr << " " << fifoData[0].words[i];
        //}
        //cerr << endl;

        double K = 1.0 / INT_MAX;

        cerr << format("%.6f ", Date::now().secondsSince(start))
             << " " << accelX << " " << accelY << " " << accelZ
             << " " << gyroX << " " << gyroY << " " << gyroZ
             << " " << temp << " " << (temp / 340.0) + 36.53 << "c "
             << " " << fifoAvail << " "
             << K * fifoData[0].quatW << " " << K * fifoData[0].quatX
             << " " << K * fifoData[0].quatY << " " << K * fifoData[0].quatZ
             << endl;


    }

    double elapsed = Date::now().secondsSince(start);

    cerr << "read " << niter << " samples in "
         << elapsed << " seconds at " << elapsed / niter * 1000.0 << "ms/call or "
         << niter / elapsed << " samples/second" << endl;
}
