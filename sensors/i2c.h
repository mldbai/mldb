/** i2c.h                                                          -*- C++ -*-
    Jeremy Barnes, 3 November 2016
    Copyright (c) 2016 mldb.ai Inc.  All rights reserved.

    Wrapper library for Linux I2C support.
*/

#pragma once

#include <string>
#include <vector>
#include <mutex>

namespace MLDB {

struct I2CDevice;
struct I2CTransaction;


/*****************************************************************************/
/* I2C BUS                                                                   */
/*****************************************************************************/

struct I2CBus {

    I2CBus(const std::string & deviceName);
    ~I2CBus();

    /** Enumerate all I2C busses available. */
    static std::vector<std::string> enumerate();

    std::vector<std::string> capabilities() const;

    std::vector<int> scan() const;

    std::string deviceName;
};


/*****************************************************************************/
/* I2C DEVICE                                                                */
/*****************************************************************************/

struct I2CDevice {
    I2CDevice()
    {
    }

    I2CDevice(I2CBus * bus, uint8_t address);

    ~I2CDevice();
    
    void writeByte(int address, uint8_t value);
    void writeWord(int address, uint16_t value);
    void writeBlock(int address, const void * data, size_t numBytes);
    uint8_t readByte(int address);
    uint16_t readWord(int address);
    void readBlock(int address, void * data, size_t numBytes);
    
    I2CTransaction acquireBus();

    I2CBus * bus = nullptr;
    int fd = -1;
};


/*****************************************************************************/
/* I2C TRANSACTION                                                           */
/*****************************************************************************/

struct I2CTransaction {
    uint8_t readByte(uint8_t reg);
    std::vector<uint8_t> readBytes(uint8_t reg, size_t numBytes);

    void writeByte(uint8_t reg, uint8_t byte);
    void writeBytes(uint8_t reg, const uint8_t * bytes, size_t numBytes);

    std::unique_lock<std::mutex> guard;
};


} // namespace MLDB
