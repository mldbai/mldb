/* I2Cdev.h
   Jeremy Barnes, 7 November 2016
   Copyright (c) 2016 mldb.ai Inc.  All rights reserved.

   I2C device header for MLDB compatible with i2cdevlib.  Allows for
   Arduino code to be imported into MLDB so that we can use devices
   that are compatible with the Arduino without needing to rewrite
   all the low-level code for MLDB.
*/

#pragma once

#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <limits>
#include <cmath>
#include <iostream>

static constexpr int I2CDEV_DEFAULT_READ_TIMEOUT=1000;

namespace MLDB {


/*****************************************************************************/
/* I2C DEV                                                                   */
/*****************************************************************************/

struct I2Cdev {
    I2Cdev();

    static void initialize();
    static void enable(bool isEnabled);

    static int8_t readBit(uint8_t devAddr, uint8_t regAddr, uint8_t bitNum, uint8_t *data);
    static int8_t readBitW(uint8_t devAddr, uint8_t regAddr, uint8_t bitNum, uint16_t *data);
    static int8_t readBits(uint8_t devAddr, uint8_t regAddr, uint8_t bitStart, uint8_t length, uint8_t *data);
    static int8_t readBitsW(uint8_t devAddr, uint8_t regAddr, uint8_t bitStart, uint8_t length, uint16_t *data);
    static int8_t readByte(uint8_t devAddr, uint8_t regAddr, uint8_t *data);
    static int8_t readWord(uint8_t devAddr, uint8_t regAddr, uint16_t *data);
    static int8_t readBytes(uint8_t devAddr, uint8_t regAddr, uint8_t length, uint8_t *data);
    static int8_t readWords(uint8_t devAddr, uint8_t regAddr, uint8_t length, uint16_t *data);

    static bool writeBit(uint8_t devAddr, uint8_t regAddr, uint8_t bitNum, uint8_t data);
    static bool writeBitW(uint8_t devAddr, uint8_t regAddr, uint8_t bitNum, uint16_t data);
    static bool writeBits(uint8_t devAddr, uint8_t regAddr, uint8_t bitStart, uint8_t length, uint8_t data);
    static bool writeBitsW(uint8_t devAddr, uint8_t regAddr, uint8_t bitStart, uint8_t length, uint16_t data);
    static bool writeByte(uint8_t devAddr, uint8_t regAddr, uint8_t data);
    static bool writeWord(uint8_t devAddr, uint8_t regAddr, uint16_t data);
    static bool writeBytes(uint8_t devAddr, uint8_t regAddr, uint8_t length, uint8_t *data);
    static bool writeWords(uint8_t devAddr, uint8_t regAddr, uint8_t length, uint16_t *data);
    
    static constexpr uint16_t readTimeout = I2CDEV_DEFAULT_READ_TIMEOUT;
};

void delay(double seconds);

inline uint8_t pgm_read_byte(const uint8_t * addr) { return *addr; }

uint32_t millis();

extern struct HexTag {} HEX;
extern struct DecTag {} DEC;
extern struct BinTag {} BIN;

// Emulate the Arduino serial device
extern struct ArduinoSerial {
    template<typename T>
    void print(const T & val)
    {
        std::cerr << val;
    }

    void print(char val)
    {
        std::cerr << (int)val;
    }

    void print(unsigned char val)
    {
        std::cerr << (int)val;
    }

    void print(signed char val)
    {
        std::cerr << (int)val;
    }

    template<typename T>
    void print(const T & val, HexTag)
    {
        std::cerr << std::hex;
        print(val);
        std::cerr << std::dec;
    }

    template<typename T>
    void print(T val, BinTag) // TODO: really binary
    {
        std::cerr << std::hex;
        print(val);
        std::cerr << std::dec;
    }

    template<typename T>
    void print(const T & val, DecTag)
    {
        print(val);
    }

    template<typename T>
    void print(const char * val)
    {
        std::cerr << val;
    }

    void println(const char * val)
    {
        std::cerr << val << std::endl;
    }

    template<typename... T>
    void println(T && ... vals)
    {
        print(std::forward<T>(vals)...);
        std::cerr << std::endl;
    }
} Serial;

} // namespace MLDB

using MLDB::I2Cdev;
using MLDB::delay;
using MLDB::pgm_read_byte;
using MLDB::millis;
using MLDB::Serial;
using MLDB::HEX;
using MLDB::DEC;
using MLDB::BIN;
