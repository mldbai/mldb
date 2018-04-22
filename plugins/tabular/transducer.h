/** transducer.h                                                   -*- C++ -*-
    Jeremy Barnes, 16 April 2018
    Copyright (c) 2018 Element AI Inc.  All rights reserved.
    This file is part of MLDB. Copyright 2018 mldb.ai inc. All rights reserved.

    Definitions for transducer classes, used to predictively produce
    dataset elements.
*/

#pragma once


#include <memory>
#include <vector>
#include <string>
#include "mldb/utils/lightweight_hash.h"
#include "mldb/compiler/string_view.h"


namespace MLDB {


struct MappedSerializer;
struct StructuredSerializer;
struct StructuredReconstituter;


/*****************************************************************************/
/* TRANSDUCER                                                                */
/*****************************************************************************/

/** A transducer encapsulates something that scans a sequence of input
    tokens, updating a state and producing a set of output tokens.

    It encapsulates many things, including compression and decompression,
    pseudo-random number generation, predictive coding, etc. 

    This common interface takes care of defining the basic functionality
    of a transducer that enables it to be constructed, to generate its
    initial state, to be saved and reconstituted across devices, etc.
*/

struct Transducer {
    virtual ~Transducer()
    {
    }
};


/*****************************************************************************/
/* INTERFACE DESCRIPTION                                                     */
/*****************************************************************************/

struct InterfaceDescription {
    struct Call {
        //StructureDescription parameters;
    };
};

/*****************************************************************************/
/* CALLABLE                                                                  */
/*****************************************************************************/

/** Basic interface for something that exposes a calling contract and is
    this callable.  These can't be called directly, but can be instantiated
    into directly callable objects (for a particular device, for example)
    as the compilation and optimization is device-specific and may take
    a considerable amount of time.
*/

struct Callable {
};


struct CharacterTransducer: public Transducer {
    virtual ~CharacterTransducer()
    {
    }

    virtual char decode(uint32_t input) const = 0;

    virtual uint32_t encode(unsigned char input) const = 0;

    virtual size_t memusage() const = 0;
};


/*****************************************************************************/
/* STRING STATS                                                              */
/*****************************************************************************/

struct StringStats {
    uint64_t totalBytes = 0;

    uint32_t shortLengthDistribution[256] = {0};  // for length 0-255
    LightweightHash<uint32_t, uint32_t> longLengthDistribution;
    size_t uniqueShortLengths = 0, uniqueLongLengths = 0;

    /* We keep the distribution of bytes in the string to determine if
       there is an obvious encoding that's much smaller than the current
       one due to a restricted set of values.
    */
    uint32_t byteDistribution[256] = {0};
    size_t uniqueBytes = 0;
    
    void add(std::string_view s);
};


/*****************************************************************************/
/* STRING TRANSDUCER                                                         */
/*****************************************************************************/

struct StringTransducer: public Transducer, public Callable {
    virtual ~StringTransducer()
    {
    }

    virtual std::string_view
    generateAll(std::string_view input,
                char * outputBuffer,
                size_t outputLength) const = 0;
    
    virtual size_t getOutputLength(std::string_view input) const = 0;
    
    virtual size_t getTemporaryBufferSize(std::string_view input,
                                          ssize_t outputLength) const = 0;

    virtual bool needsTemporaryBuffer() const = 0;

    virtual bool canGetOutputLength() const = 0;
    
    virtual std::string type() const = 0;

    virtual void serializeParameters(StructuredSerializer & serializer) const = 0;

    /** Serialize the object including the metadata necessary to know which
        type it is and invoke a factory function.  This calls
        serializeParameters under the hood.
    */
    void serialize(StructuredSerializer & serializer) const;
    
    static std::shared_ptr<const void>
    registerType
        (const std::string & type,
         std::function<std::shared_ptr<StringTransducer>
                 (StructuredReconstituter &)> create);

    template<typename T>
    struct Register {
        Register(const std::string & type)
        {
            auto fn = [] (StructuredReconstituter & reconst)
                {
                    return std::make_shared<T>(reconst);
                };
            handle = registerType(type, std::move(fn));
        }
        
        std::shared_ptr<const void> handle;
    };
    
    static std::shared_ptr<StringTransducer>
    thaw(StructuredReconstituter & serializer);

    virtual size_t memusage() const = 0;
};


/*****************************************************************************/
/* IDENTITY STRING TRANSDUCER                                                */
/*****************************************************************************/

struct IdentityStringTransducer: public StringTransducer {

    IdentityStringTransducer();
    
    IdentityStringTransducer(StructuredSerializer & serializer);
    
    virtual std::string_view
    generateAll(std::string_view input,
                char * outputBuffer,
                size_t outputLength) const;
    
    virtual size_t getOutputLength(std::string_view input) const;
    
    virtual size_t getTemporaryBufferSize(std::string_view input,
                                          ssize_t outputLength) const;

    virtual bool needsTemporaryBuffer() const;

    virtual bool canGetOutputLength() const;

    virtual std::string type() const;

    virtual void serializeParameters(StructuredSerializer & serializer) const;

    virtual size_t memusage() const;
};


/*****************************************************************************/
/* ZSTD STRING TRANSDUCER                                                    */
/*****************************************************************************/

struct ZstdStringTransducer: public StringTransducer {
    
    ZstdStringTransducer(StructuredSerializer & serializer);

    static std::pair<std::shared_ptr<ZstdStringTransducer>,
                     std::shared_ptr<ZstdStringTransducer> >
    train(const std::vector<std::string> & strings,
          MappedSerializer & serializer);
    
    virtual std::string_view
    generateAll(std::string_view input,
                char * outputBuffer,
                size_t outputLength) const;
    
    virtual size_t getOutputLength(std::string_view input) const;
    
    virtual size_t getTemporaryBufferSize(std::string_view input,
                                          ssize_t outputLength) const;

    virtual bool needsTemporaryBuffer() const;

    virtual bool canGetOutputLength() const;

    virtual std::string type() const;

    virtual void serializeParameters(StructuredSerializer & serializer) const;

    virtual size_t memusage() const;
    
    struct Itl;
    std::shared_ptr<Itl> itl;
};


/*****************************************************************************/
/* ID TRANSDUCER                                                             */
/*****************************************************************************/



std::pair<std::shared_ptr<StringTransducer>,
          std::shared_ptr<StringTransducer> >
trainIdTransducer(const std::vector<std::string> & strings,
                  const StringStats & stats,
                  MappedSerializer & serializer);

} // namespace MLDB
    
