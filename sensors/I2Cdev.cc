/* I2Cdev.h
   Jeremy Barnes, 7 November 2016
   Copyright (c) 2016 mldb.ai Inc.  All rights reserved.

   I2C device header for MLDB compatible with i2cdevlib.
*/

#include "I2Cdev.h"
#include "i2c.h"
#include "mldb/http/http_exception.h"
#include <thread>
#include <chrono>
#include <iostream>
#include <mutex>

using namespace std;

namespace MLDB {

namespace {

I2CBus & getI2CBus()
{
    static I2CBus result("/dev/i2c-1");
    return result;
}

I2CDevice getLockedDevice(uint8_t address)
{
    static std::map<uint8_t, I2CDevice> devices;
    static std::mutex mutex;

    std::unique_lock<std::mutex> guard(mutex);
    auto it = devices.find(address);
    if (it == devices.end()) {
        return devices[address] = I2CDevice(&getI2CBus(), address);
    }
    return it->second;
}

} // file scope

/*****************************************************************************/
/* I2C DEV                                                                   */
/*****************************************************************************/

I2Cdev::
I2Cdev()
{
}

void
I2Cdev::
initialize()
{
}

void
I2Cdev::
enable(bool isEnabled)
{
}

int8_t
I2Cdev::
readBit(uint8_t devAddr, uint8_t regAddr, uint8_t bitNum, uint8_t *data)
{
    return readBits(devAddr, regAddr, bitNum, 1, data);
}

int8_t
I2Cdev::
readBitW(uint8_t devAddr, uint8_t regAddr, uint8_t bitNum, uint16_t *data)
{
    return readBitsW(devAddr, regAddr, bitNum, 1, data);
}

int8_t
I2Cdev::
readBits(uint8_t devAddr, uint8_t regAddr, uint8_t bitStart, uint8_t length, uint8_t *data)
{
    I2CDevice dev = getLockedDevice(devAddr);
    uint8_t b = dev.readByte(regAddr);
    uint8_t mask = ((1 << length) - 1) << (bitStart - length + 1);
    b &= mask;
    b >>= (bitStart - length + 1);
    *data = b;
    return 1;
}

int8_t
I2Cdev::
readBitsW(uint8_t devAddr, uint8_t regAddr, uint8_t bitStart, uint8_t length, uint16_t *data)
{
    I2CDevice dev = getLockedDevice(devAddr);
    uint16_t b = dev.readWord(regAddr);
    uint16_t mask = ((1 << length) - 1) << (bitStart - length + 1);
    b &= mask;
    b >>= (bitStart - length + 1);
    *data = b;
    return 1;
}

int8_t
I2Cdev::
readByte(uint8_t devAddr, uint8_t regAddr, uint8_t *data)
{
    I2CDevice dev = getLockedDevice(devAddr);
    *data = dev.readByte(regAddr);
    return 1;
}

int8_t
I2Cdev::
readWord(uint8_t devAddr, uint8_t regAddr, uint16_t *data)
{
    I2CDevice dev = getLockedDevice(devAddr);
    *data = dev.readWord(regAddr);
    return 1;
}

int8_t
I2Cdev::
readBytes(uint8_t devAddr, uint8_t regAddr, uint8_t length, uint8_t *data)
{
    I2CDevice dev = getLockedDevice(devAddr);
    while (length > 0) {
        uint8_t n = std::min<uint8_t>(length, 32 /* I2C_SMBUS_BLOCK_MAX */);
        dev.readBlock(regAddr, data, n);
        data += n;
        length -= n;
    }
    return 1;
}

int8_t
I2Cdev::
readWords(uint8_t devAddr, uint8_t regAddr, uint8_t length, uint16_t *data)
{
    throw HttpReturnException(600, "Not implemented");
}

bool
I2Cdev::
writeBit(uint8_t devAddr, uint8_t regAddr, uint8_t bitNum, uint8_t data)
{
    return writeBits(devAddr, regAddr, bitNum, 1, data);
}

bool
I2Cdev::
writeBitW(uint8_t devAddr, uint8_t regAddr, uint8_t bitNum, uint16_t data)
{
    return writeBitsW(devAddr, regAddr, bitNum, 1, data);
}

bool
I2Cdev::
writeBits(uint8_t devAddr, uint8_t regAddr, uint8_t bitStart, uint8_t length, uint8_t data)
{
    I2CDevice dev = getLockedDevice(devAddr);
    uint8_t b = dev.readByte(regAddr);
    uint8_t mask = ((1 << length) - 1) << (bitStart - length + 1);
    data <<= (bitStart - length + 1); // shift data into correct position
    data &= mask; // zero all non-important bits in data
    b &= ~(mask); // zero all important bits in existing byte
    b |= data; // combine data with existing byte
    dev.writeByte(regAddr, b);
    return true;
}

bool
I2Cdev::
writeBitsW(uint8_t devAddr, uint8_t regAddr, uint8_t bitStart, uint8_t length, uint16_t data)
{
    I2CDevice dev = getLockedDevice(devAddr);
    uint16_t b = dev.readWord(regAddr);
    uint16_t mask = ((1 << length) - 1) << (bitStart - length + 1);
    data <<= (bitStart - length + 1); // shift data into correct position
    data &= mask; // zero all non-important bits in data
    b &= ~(mask); // zero all important bits in existing byte
    b |= data; // combine data with existing byte
    dev.writeWord(regAddr, b);
    return true;
}

bool
I2Cdev::
writeByte(uint8_t devAddr, uint8_t regAddr, uint8_t data)
{
    I2CDevice dev = getLockedDevice(devAddr);
    dev.writeByte(regAddr, data);
    return true;
}

bool
I2Cdev::
writeWord(uint8_t devAddr, uint8_t regAddr, uint16_t data)
{
    I2CDevice dev = getLockedDevice(devAddr);
    dev.writeWord(regAddr, data);
    return true;
}

bool
I2Cdev::
writeBytes(uint8_t devAddr, uint8_t regAddr, uint8_t length, uint8_t *data)
{
    I2CDevice dev = getLockedDevice(devAddr);
    dev.writeBlock(regAddr, data, length);
    return true;
}

bool
I2Cdev::
writeWords(uint8_t devAddr, uint8_t regAddr, uint8_t length, uint16_t *data)
{
    throw HttpReturnException(600, "Not implemented");
}
    
void delay(double seconds)
{
    std::this_thread::sleep_for(std::chrono::microseconds(uint64_t(seconds * 1000000)));
}

uint32_t millis()
{
    return 0;
}

HexTag HEX;
DecTag DEC;
BinTag BIN;

} // namespace MLDB
