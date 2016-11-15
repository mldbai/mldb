/** i2c.cc
    Jeremy Barnes, 3 November 2016
    Copyright (c) 2016 mldb.ai Inc.  All rights reserved.

*/

#include "i2c.h"
#include "mldb/http/http_exception.h"
#include "mldb/base/scope.h"
#include <fcntl.h>
#include <linux/i2c-dev.h>
#include <linux/i2c.h>
#include <sys/ioctl.h>
#include <string.h>
#include <unistd.h>
#include <iostream>

using namespace std;


namespace MLDB {

/*****************************************************************************/
/* I2C BUS                                                                   */
/*****************************************************************************/

std::vector<std::string>
I2CBus::
enumerate()
{
    std::vector<std::string> result;
    return result;
}

I2CBus::
I2CBus(const std::string & deviceName)
    : deviceName(deviceName)
{
}

I2CBus::
~I2CBus()
{
}

static constexpr int I2C_FIRST_ADDRESS = 0x3;
static constexpr int I2C_LAST_ADDRESS = 0x77;

std::vector<std::string>
I2CBus::
capabilities() const
{
    int fd = open(deviceName.c_str(), O_RDWR, 0 /* flags */);
    Scope_Exit(::close(fd));

    if (fd == -1) {
        throw HttpReturnException(400, "Error opening I2C bus " + deviceName
                                  + ": " + strerror(errno));
    }    

    unsigned long functionality;
    int res = ioctl(fd, I2C_FUNCS, &functionality);
    if (res != 0) {
        throw HttpReturnException(400, "Error querying functionalities for I2C bus "
                                  + deviceName + ": " + strerror(errno));
    }

    std::vector<std::string> funclist;
#define CHECK_FUNCTIONALITY(func, name) if (functionality & func) funclist.emplace_back(name)

    CHECK_FUNCTIONALITY(I2C_FUNC_I2C, "ic2");
    CHECK_FUNCTIONALITY(I2C_FUNC_10BIT_ADDR, "10bit");
    CHECK_FUNCTIONALITY(I2C_FUNC_PROTOCOL_MANGLING, "mangling");
    CHECK_FUNCTIONALITY(I2C_FUNC_SMBUS_PEC, "pec");
    CHECK_FUNCTIONALITY(I2C_FUNC_NOSTART, "nostart");
    CHECK_FUNCTIONALITY(I2C_FUNC_SMBUS_BLOCK_PROC_CALL, "block_proc_call");
    CHECK_FUNCTIONALITY(I2C_FUNC_SMBUS_QUICK, "quick");
    CHECK_FUNCTIONALITY(I2C_FUNC_SMBUS_READ_BYTE, "read_byte");
    CHECK_FUNCTIONALITY(I2C_FUNC_SMBUS_WRITE_BYTE, "write_byte");
    CHECK_FUNCTIONALITY(I2C_FUNC_SMBUS_READ_BYTE_DATA, "read_byte_data");
    CHECK_FUNCTIONALITY(I2C_FUNC_SMBUS_WRITE_BYTE_DATA, "write_byte_data");
    CHECK_FUNCTIONALITY(I2C_FUNC_SMBUS_READ_WORD_DATA, "read_word_data");
    CHECK_FUNCTIONALITY(I2C_FUNC_SMBUS_WRITE_WORD_DATA, "write_word_data");
    CHECK_FUNCTIONALITY(I2C_FUNC_SMBUS_PROC_CALL, "proc_call");
    CHECK_FUNCTIONALITY(I2C_FUNC_SMBUS_READ_BLOCK_DATA, "read_block_data");
    CHECK_FUNCTIONALITY(I2C_FUNC_SMBUS_WRITE_BLOCK_DATA, "write_block_data");
    CHECK_FUNCTIONALITY(I2C_FUNC_SMBUS_READ_I2C_BLOCK, "read_i2c_block");
    CHECK_FUNCTIONALITY( I2C_FUNC_SMBUS_WRITE_I2C_BLOCK, "write_i2c_block");
#undef CHECK_FUNCTIONALITY

    return funclist;
}

std::vector<int>
I2CBus::
scan() const
{
    int fd = open(deviceName.c_str(), O_RDWR, 0 /* flags */);
    Scope_Exit(::close(fd));

    if (fd == -1) {
        throw HttpReturnException(400, "Error opening I2C bus " + deviceName
                                  + ": " + strerror(errno));
    }    

    std::vector<int> result;
    for (int addr = I2C_FIRST_ADDRESS;  addr <= I2C_LAST_ADDRESS;  ++addr) {
        int res = ioctl(fd,I2C_SLAVE,addr);
        if (res < 0) {
            continue;
        }
        // Try to get the device.  We use SMBUS only as that's the simplest
        // way to do it.
        i2c_smbus_ioctl_data command;
        i2c_smbus_data data;
        command.read_write = I2C_SMBUS_READ;
        command.command = 0x0;  // read register 0 of the device
        command.size = 1;
        command.data = &data;
        res = ioctl(fd, I2C_SMBUS, &command);

        // The real test is can we read a byte from it?
        // if it's busy, we assume that it's available but someone else is
        // using it.
        if (res == -1 && errno != EBUSY) {
            continue;
        }
        
        result.push_back(addr);
    }
    
    return result;
}

/*****************************************************************************/
/* I2C DEVICE                                                                */
/*****************************************************************************/

I2CDevice::
I2CDevice(I2CBus * bus,
          uint8_t address)
    : bus(bus)
{
    fd = open(bus->deviceName.c_str(), O_RDWR, 0 /* flags */);
    if (fd == -1) {
        throw HttpReturnException(400, "Error opening I2C bus " + bus->deviceName
                                  + ": " + strerror(errno));
    }

    int res = ioctl(fd, I2C_SLAVE, address);

    if (res != 0) {
        throw HttpReturnException(400, "Error obtaining slave access on I2C bus "
                                  + bus->deviceName + " address "
                                  + std::to_string((int)address));
    }
}

I2CDevice::
~I2CDevice()
{
    //::close(fd);
}

void
I2CDevice::
writeByte(int address, uint8_t value)
{
    i2c_smbus_ioctl_data command;
    i2c_smbus_data data;
    data.byte = value;
    command.read_write = I2C_SMBUS_WRITE;
    command.size = I2C_SMBUS_BYTE_DATA;
    command.command = address;
    command.data = &data;
    int res = ioctl(fd, I2C_SMBUS, &command);
    if (res == -1) {
        throw HttpReturnException(400, "Error writing to I2C device: "
                                  + string(strerror(errno)));
    }
}

void
I2CDevice::
writeWord(int address, uint16_t value)
{
    uint8_t c[2];
    c[0] = value >> 8;
    c[1] = value >> 0;
    writeBlock(address, c, 2);
}

void
I2CDevice::
writeBlock(int address, const void * dataIn, size_t numBytes)
{
    if (numBytes > I2C_SMBUS_BLOCK_MAX) {
        throw HttpReturnException(400, "Attempt to write too large an SMbus block");
    }

    i2c_smbus_ioctl_data command;
    i2c_smbus_data data;
    data.block[0] = numBytes;
    memcpy(data.block + 1, dataIn, numBytes);
    command.read_write = I2C_SMBUS_WRITE;
    command.size = I2C_SMBUS_I2C_BLOCK_DATA;
    command.command = address;
    command.data = &data;
    int res = ioctl(fd, I2C_SMBUS, &command);
    if (res == -1) {
        throw HttpReturnException(400, "Error writing to I2C device: "
                                  + string(strerror(errno)));
    }
}

uint8_t
I2CDevice::
readByte(int address)
{
    i2c_smbus_ioctl_data command;
    i2c_smbus_data data;
    command.read_write = I2C_SMBUS_READ;
    command.size = I2C_SMBUS_BYTE_DATA;
    command.command = address;
    command.data = &data;
    int res = ioctl(fd, I2C_SMBUS, &command);
    if (res == -1) {
        throw HttpReturnException(400, "Error writing to I2C device: "
                                  + string(strerror(errno)));
    }

    return data.byte;
}

uint16_t
I2CDevice::
readWord(int address)
{
    uint8_t data[2];
    readBlock(address, &data, 2);
    return data[0] << 8 | data[1];
}

void
I2CDevice::
readBlock(int address, void * dataOut, size_t numBytes)
{
    if (numBytes <= I2C_SMBUS_BLOCK_MAX) {
        i2c_smbus_ioctl_data command;
        i2c_smbus_data data;
        data.block[0] = numBytes;
        command.read_write = I2C_SMBUS_READ;
        command.size = I2C_SMBUS_I2C_BLOCK_DATA;
        command.command = address;
        command.data = &data;
        int res = ioctl(fd, I2C_SMBUS, &command);
        if (res == -1) {
            throw HttpReturnException(400, "Error writing to I2C device: "
                                      + string(strerror(errno)));
        }

        if (data.block[0] != numBytes) {
        }
        
        //cerr << "trying to read " << numBytes << " bytes" << endl;
        //cerr << "got " << (int)data.block[0] << " bytes in block" << endl;
        memcpy(dataOut, data.block + 1, numBytes);
    }
    else {
        // We need to use an I2C command to get the data
        i2c_msg msg[2];

        msg[0].addr = address;
        msg[0].flags = 0;
        msg[0].len = 0;
        msg[0].buf = (__u8 *)dataOut;

        msg[1].addr = address;
        msg[1].flags = I2C_M_RD;
        msg[1].len = numBytes;
        msg[1].buf = (__u8 *)dataOut;

        i2c_rdwr_ioctl_data data;
        data.nmsgs = 2;
        data.msgs = msg;

        cerr << "about to do ioctl I2C_RDWR" << endl;
        int res = ioctl(fd, I2C_RDWR, &data);
        cerr << "done ioctl I2C_RDWR" << endl;
        
        if (res == -1) {
            throw HttpReturnException(400, "Error writing to I2C device: "
                                      + string(strerror(errno)));
        }

        throw HttpReturnException(600, "need to finish");
    }
}

I2CTransaction
I2CDevice::
acquireBus()
{
    return I2CTransaction();
}


} // namespace MLDB
