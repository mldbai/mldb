/** i2c_test.cc
    Jeremy Barnes, 3 November 2016
    Copyright (c) 2016 mldb.ai Inc.  All rights reserved.

*/

#include "mldb/sensors/i2c.h"
#include "mldb/types/date.h"
#include "mldb/types/basic_value_descriptions.h"
#include "mldb/types/json.h"
#include "mldb/arch/endian.h"
#include <iostream>
#include <linux/i2c-dev.h>
#include <linux/i2c.h>
#include <sys/ioctl.h>

#define BOOST_TEST_MAIN
#define BOOST_TEST_DYN_LINK

#include <boost/test/unit_test.hpp>

using namespace std;

using namespace MLDB;

uint16_t bswap(uint16_t val)
{
    return (val << 8) | (val >> 8);
}

BOOST_AUTO_TEST_CASE( test_i2c_enumeration )
{
    I2CBus bus("/dev/i2c-1");
    cerr << jsonEncodeStr(bus.capabilities()) << endl;
    cerr << jsonEncodeStr(bus.scan()) << endl;

    // Power management registers of the 6050 Gyro / Accelerometer
    constexpr int address = 0x68;
    constexpr int power_mgmt_1 = 0x6b;
    //constexpr int power_mgmt_2 = 0x6c;
    constexpr int accel_x = 0x3b;
#if 0
    constexpr int accel_y = 0x3d;
    constexpr int accel_z = 0x3f;
    constexpr int gyro_x = 0x43;
    constexpr int gyro_y = 0x45;
    constexpr int gyro_z = 0x47;
#endif

    I2CDevice dev(&bus, address);
    dev.writeByte(power_mgmt_1, 0);

    Date start = Date::now();

    // These are laid out little-endian
    struct DataBlock {
        int16_be accel[3];  // at 0x3b
        int16_be temp;      // at 0x41
        int16_be gyro[3];  // at 0x43
    };

    size_t niter = 100;

#if 0
    constexpr int adc_address = 0x68;
    I2CDevice adcDev(&bus, adc_address);

    enum {
        // Wakeup
        ADS111X_CONFIG_WAKEUP = 1 << 15,

        // Inputs: differential or standard
        ADS111X_CONFIG_INPUT_0_1 = 0 << 12,
        ADS111X_CONFIG_INPUT_0_3 = 1 << 12,
        ADS111X_CONFIG_INPUT_1_3 = 2 << 12,
        ADS111X_CONFIG_INPUT_2_3 = 3 << 12,
        ADS111X_CONFIG_INPUT_0 = 4 << 12,
        ADS111X_CONFIG_INPUT_1 = 5 << 12,
        ADS111X_CONFIG_INPUT_2 = 6 << 12,
        ADS111X_CONFIG_INPUT_3 = 7 << 12,

        // Gain
        ADS111X_GAIN_6V144 = 0 << 9,
        ADS111X_GAIN_4V096 = 1 << 9,
        ADS111X_GAIN_2V048 = 2 << 9,
        ADS111X_GAIN_1V024 = 3 << 9,
        ADS111X_GAIN_0V512 = 4 << 9,
        ADS111X_GAIN_0V256 = 5 << 9,

        // Operating mode
        ADS111X_MODE_CONTINUOUS = 1 << 8,
        ADS111X_MODE_SINGLE_SHOT = 0 << 8,

        // Data rate
        ADS111X_RATE_8SPS = 0 << 5,
        ADS111X_RATE_16SPS = 1 << 5,
        ADS111X_RATE_32SPS = 2 << 5,
        ADS111X_RATE_64SPS = 3 << 5,
        ADS111X_RATE_128SPS = 4 << 5,
        ADS111X_RATE_250SPS = 5 << 5,
        ADS111X_RATE_475SPS = 6 << 5,
        ADS111X_RATE_860SPS = 7 << 5,

        // Comparator mode
        ADS111X_COMPARATOR_TRADITIONAL = 0 << 4,
        ADS111X_COMPARATOR_WINDOW = 1 << 4,

        // Comparator polarity
        ADS111X_POLARITY_ACTIVE_LOW = 0 << 3,
        ADS111X_POLARITY_ACTIVE_HIGH = 1 << 3,

        // Queuing
        ADS111X_QUEUING_1 = 0 << 0,
        ADS111X_QUEUING_2 = 1 << 0,
        ADS111X_QUEUING_4 = 2 << 0,
        ADS111X_QUEUING_DISABLE = 3 << 0
    };

    enum {
        ADS111X_POINTER_CONVERSION = 0,
        ADS111X_POINTER_CONFIG = 1,
        ADS111X_POINTER_LOW_THRESHOLD = 2,
        ADS111X_POINTER_HIGH_THRESHOLD = 3
    };

    constexpr uint16_be config
        = ADS111X_CONFIG_INPUT_1
        | ADS111X_GAIN_0V512
        | ADS111X_MODE_CONTINUOUS
        | ADS111X_RATE_860SPS
        | ADS111X_QUEUING_DISABLE;

    char bytes[3] = {
        ADS111X_POINTER_LOW_THRESHOLD,
        0x12,
        0x34
    };

    int numWritten = write(adcDev.fd, bytes, 3);
    cerr << "numWritten = " << numWritten << endl;

    //char bytesOut[3];

    {
        i2c_rdwr_ioctl_data data;
        data.msgs = new i2c_msg[2];
        data.nmsgs = 0;

        uint8_t mycmd = ADS111X_POINTER_LOW_THRESHOLD;

        data.msgs[0].addr = adc_address;
        data.msgs[0].flags = 0;
        data.msgs[0].len = 1;
        data.msgs[0].buf = &mycmd;
    
        uint8_t outbuf[2];
        data.msgs[0].addr = adc_address;
        data.msgs[0].flags = I2C_M_RD;
        data.msgs[0].len = 2;
        data.msgs[0].buf = outbuf;

        int res = ioctl(adcDev.fd, I2C_RDWR, &data);
        cerr << "res = " << res << endl;
        cerr << "errno = " << strerror(errno) << endl;
        cerr << "outbuf[0] = " << format("%02x", (int)outbuf[0]) << endl;
        cerr << "outbuf[1] = " << format("%02x", (int)outbuf[1]) << endl;
    }

    //addr = ADS111X_POINTER_LOW_THRESHOLD;
            
    adcDev.writeBlock(ADS111X_POINTER_CONFIG, &config, 2);
    uint16_t configRead;
    adcDev.readBlock(ADS111X_POINTER_CONFIG, &configRead, 2);

    uint16_be threshold = 0x1234;
    adcDev.writeBlock(ADS111X_POINTER_LOW_THRESHOLD, &threshold, 2);
    uint16_be threshold2 = 0xffff;
    adcDev.readBlock(ADS111X_POINTER_LOW_THRESHOLD, &threshold2, 2);
    cerr << "threshold2 = " << format("%04x", (int)threshold2) << endl;

    
    cerr << "config = " << format("%04x", (int)config)
         << " configRead = " << format("%04x", (int)configRead) << endl;

    adcDev.readBlock(ADS111X_POINTER_CONFIG, &configRead, 2);
#endif

    for (size_t i = 0;  i < niter;  ++i) {

        DataBlock data;
        dev.readBlock(accel_x, &data, sizeof(data));

        int16_t accelX = data.accel[0];
        int16_t accelY = data.accel[1];
        int16_t accelZ = data.accel[2];

        int16_t gyroX = data.gyro[0];
        int16_t gyroY = data.gyro[1];
        int16_t gyroZ = data.gyro[2];

        //int16_t adc = adcDev.readWord(ADS111X_POINTER_CONVERSION);

#if 0
        int16_t accelX = bswap(dev.readWord(accel_x));
        int16_t accelY = bswap(dev.readWord(accel_y));
        int16_t accelZ = bswap(dev.readWord(accel_z));

        int16_t gyroX = bswap(dev.readWord(gyro_x));
        int16_t gyroY = bswap(dev.readWord(gyro_y));
        int16_t gyroZ = bswap(dev.readWord(gyro_z));
#endif

        cerr << format("%.6f", Date::now().secondsSince(start))
             << " " << std::hex << accelX << " " << accelY << " " << accelZ
             << " " << gyroX << " " << gyroY << " " << gyroZ
             << " " << std::dec << (data.temp / 340.0) + 36.53 << "c "
            /*<< adc*/ << endl;
    }

    double elapsed = Date::now().secondsSince(start);

    cerr << "read " << niter << " samples in "
         << elapsed << " seconds at " << elapsed / niter * 1000.0 << "ms/call or "
         << niter / elapsed << " samples/second" << endl;
}
