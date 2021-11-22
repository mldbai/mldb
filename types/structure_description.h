/** structure_value_descriptions.h                                 -*- C++ -*-
    Jeremy Barnes, 21 August 2015
    Copyright (c) 2015 mldb.ai inc.  All rights reserved.

    This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

    Value desriptions for structures.
*/

#pragma once

#include "value_description.h"
#include "utility_descriptions.h"
#include <cstring>

namespace MLDB {


/*****************************************************************************/
/* STRUCTURE DESCRIPTION BASE                                                */
/*****************************************************************************/

/** Base information for a structure description. */

struct StructureDescriptionBase {

    StructureDescriptionBase(const std::type_info * type,
                             ValueDescription * owner,
                             const std::string & structName,
                             bool nullAccepted);

    StructureDescriptionBase(const StructureDescriptionBase & other) = delete;
    StructureDescriptionBase(StructureDescriptionBase && other) = delete;
    void operator = (const StructureDescriptionBase & other);
    void operator = (StructureDescriptionBase && other);

    const std::type_info * type;
    std::string structName;
    bool nullAccepted;
    ValueDescription * owner;

    int version = -1;   ///< The version number of this structure

    struct OldVersion {
        int version = -1;
        std::shared_ptr<ValueDescription> desc;
        std::function<void (const void *, void *)> convert;
    };

    void setVersion(int versionNumber)
    {
        this->version = versionNumber;
    }

    /// Current set of old versions of the structure
    std::vector<OldVersion> oldVersions;


    typedef ValueDescription::FieldDescription FieldDescription;

    // Comparison object to allow const char * objects to be looked up
    // in the map and so for comparisons to be done with no memory
    // allocations.
    struct StrCompare {
        bool operator () (const char * s1, const char * s2) const;
    };

    typedef std::map<const char *, FieldDescription, StrCompare> Fields;
    Fields fields;

    /* A deleter that works with buffers allocated with malloc */
    struct FreeDeleter {
        void operator () (void * p)
            const
        {
            ::free(p);
        }
    };

    std::vector<std::unique_ptr<char, FreeDeleter> > fieldNames;

    std::vector<Fields::const_iterator> orderedFields;

    struct Exception: public MLDB::Exception {
        Exception(JsonParsingContext & context,
                  const std::string & message);
        virtual ~Exception() throw ();
    };

    virtual void parseJson(void * output, JsonParsingContext & context) const;
    virtual void printJson(const void * input, JsonPrintingContext & context) const;

    virtual bool onEntry(void * output, JsonParsingContext & context) const = 0;
    virtual void onExit(void * output, JsonParsingContext & context) const = 0;

    void addFieldDesc(std::string name,
                      size_t offset,
                      std::string comment,
                      std::shared_ptr<const ValueDescription> description);

    virtual const FieldDescription *
    hasField(const void * val, const std::string & field) const;

    virtual const FieldDescription *
    getFieldDescription(const void * val, const void * field) const;
    
    virtual void forEachField(const void * val,
                              const std::function<void (const FieldDescription &)> & onField) const;

    virtual const FieldDescription & 
    getField(const std::string & field) const;

    virtual const FieldDescription & 
    getFieldByNumber(int fieldNum) const;

    virtual int getVersion() const;

    virtual void fixupAlign(size_t knownWidth, size_t knownAlign) = 0;
};


/*****************************************************************************/
/* GENERIC STRUCTURE DESCRIPTION                                             */
/*****************************************************************************/

struct GenericStructureDescription:
    public ValueDescription,
    public StructureDescriptionBase {

    GenericStructureDescription(bool nullAccepted,
                                const std::string & structName);

    virtual void parseJson(void * val, JsonParsingContext & context) const override;
    virtual void printJson(const void * val, JsonPrintingContext & context) const override;
    virtual bool isDefault(const void * val) const override;
    virtual void setDefault(void * val) const override;
    virtual void copyValue(const void * from, void * to) const override;
    virtual void moveValue(void * from, void * to) const override;
    virtual void swapValues(void * from, void * to) const override;
    virtual void initializeDefault(void * mem) const override;
    virtual void initializeCopy(void * mem, const void * other) const override;
    virtual void initializeMove(void * mem, void * other) const override;
    virtual void destruct(void *) const override;

    virtual bool onEntry(void * output, JsonParsingContext & context) const override;
    virtual void onExit(void * output, JsonParsingContext & context) const override;

    virtual size_t getFieldCount(const void * val) const override
    {
        return fields.size();
    }

    virtual bool hasFixedFieldCount() const override
    {
        return true;
    }

    virtual size_t getFixedFieldCount() const override
    {
        return fields.size();
    }

    virtual const FieldDescription *
    hasField(const void * val, const std::string & field) const override
    {
        return StructureDescriptionBase::hasField(val, field);
    }

    virtual const FieldDescription *
    getFieldDescription(const void * val, const void * field) const override
    {
        return StructureDescriptionBase::getFieldDescription(val, field);
    }
    
    virtual void forEachField(const void * val,
                              const std::function<void (const FieldDescription &)> & onField) const override
    {
        return StructureDescriptionBase::forEachField(val, onField);
    }

    virtual const FieldDescription & 
    getField(const std::string & field) const override
    {
        return StructureDescriptionBase::getField(field);
    }

    virtual const FieldDescription & 
    getFieldByNumber(int fieldNum) const override
    {
        return StructureDescriptionBase::getFieldByNumber(fieldNum);
    }

    virtual int getVersion() const override
    {
        return StructureDescriptionBase::getVersion();
    }

    virtual void fixupAlign(size_t knownWidth, size_t knownAlign) override;
};


/*****************************************************************************/
/* STRUCTURE DESCRIPTION                                                     */
/*****************************************************************************/

/** Class that implements the base of a description of a structure.  Contains
    methods to register all of the member variables of the class.
*/

template<typename Struct>
struct StructureDescription
    : public ValueDescriptionT<Struct>,
      public StructureDescriptionBase {
    StructureDescription(bool nullAccepted = false,
                         const std::string & structName = type_name<Struct>())
        : ValueDescriptionT<Struct>(ValueKind::STRUCTURE),
          StructureDescriptionBase(&typeid(Struct), this, structName,
                                   nullAccepted)
    {
    }

    /// Function to be called before parsing; if it returns false parsing stops
    std::function<bool (Struct *, JsonParsingContext & context)> onEntryHandler;

    /// Function to be called whenever an unknown field is found
    std::function<void (Struct *, JsonParsingContext & context)> onUnknownField;

    /// Function to be called after parsing and validation
    std::function<void (Struct *, JsonParsingContext & context)> onPostValidate;

    virtual bool onEntry(void * output, JsonParsingContext & context) const override
    {
        if (onEntryHandler) {
            if (!onEntryHandler((Struct *)output, context))
                return false;
        }
        
        if (onUnknownField)
            context.onUnknownFieldHandlers.push_back([=,&context] (const ValueDescription *) { this->onUnknownField((Struct *)output, context); });

        return true;
    }
    
    virtual void onExit(void * output, JsonParsingContext & context) const override
    {
        if (onUnknownField)
            context.onUnknownFieldHandlers.pop_back();
        postValidate(output, context);
        StructureDescription * structParent;
        for (auto parent: parents) {
            structParent = static_cast<StructureDescription *>(parent.get());
            structParent->postValidate(output, context);
        }
    }

    virtual void postValidate(void * output, JsonParsingContext & context) const
    {
        if (onPostValidate) {
            Struct * structOutput = static_cast<Struct *>(output);
            onPostValidate(structOutput, context);
        }
    }

    template<typename V, typename Base>
    void addField(std::string name,
                  V Base::* field,
                  std::string comment)
    {
        addFieldDesc(name, field, comment, getDefaultDescriptionSharedT<V>());
    }

    /** Add a field, but override the default value description to use.
        Note that description needs to be convertible to
        std::shared_ptr<const ValueDescriptionT<V> >, but GCC 5.1 is confused
        by it and rejects it.
    */
    template<typename V, typename Base, typename Desc>
    void addFieldDesc(std::string name,
                      V Base::* field,
                      std::string comment,
                      std::shared_ptr<Desc> description)
    {
        Struct * p = nullptr;
        size_t offset = (size_t)&(p->*field);
        StructureDescriptionBase::addFieldDesc(std::move(name), offset, std::move(comment), std::move(description));
    }

    /** Add a description with a default value. */
    template<typename V, typename Base,
             typename Desc = ValueDescriptionWithDefault<V> >
    void addField(std::string name,
                  V Base::* field,
                  std::string comment,
                  const V & defaultValue,
                  std::shared_ptr<const ValueDescriptionT<V> > baseDesc
                      = getDefaultDescriptionSharedT<V>())
    {
        auto desc = std::make_shared<Desc>(defaultValue, baseDesc);
        return addFieldDesc(std::move(name), field, std::move(comment), std::move(desc));
    }

    /** Add a description with an automatic default value derived
        from the default constructor.
    */
    template<typename V, typename Base,
             typename Desc = ValueDescriptionWithDefault<V> >
    void addAuto(std::string name,
                 V Base::* field,
                 std::string comment,
                 std::shared_ptr<const ValueDescriptionT<V> > baseDesc
                     = getDefaultDescriptionSharedT<V>())
    {
        V defValue = Base() .* field;
        addField(std::move(name), field, comment, defValue, baseDesc);
    }

    using ValueDescriptionT<Struct>::parents;

    template<typename V>
    void addParent(ValueDescriptionT<V> * description_
                   = getDefaultDescription((V *)0));

    virtual void parseJson(void * val, JsonParsingContext & context) const override
    {
        return StructureDescriptionBase::parseJson(val, context);
    }

    virtual void printJson(const void * val, JsonPrintingContext & context) const override
    {
        return StructureDescriptionBase::printJson(val, context);
    }

    virtual size_t getFieldCount(const void * val) const override
    {
        return fields.size();
    }

    virtual bool hasFixedFieldCount() const override
    {
        return true;
    }

    virtual size_t getFixedFieldCount() const override
    {
        return fields.size();
    }

    virtual const FieldDescription *
    hasField(const void * val, const std::string & field) const override
    {
        return StructureDescriptionBase::hasField(val, field);
    }

    virtual const FieldDescription *
    getFieldDescription(const void * val, const void * field) const override
    {
        return StructureDescriptionBase::getFieldDescription(val, field);
    }
    
    virtual void forEachField(const void * val,
                              const std::function<void (const FieldDescription &)> & onField) const override
    {
        return StructureDescriptionBase::forEachField(val, onField);
    }

    virtual const FieldDescription & 
    getField(const std::string & field) const override
    {
        return StructureDescriptionBase::getField(field);
    }

    virtual const FieldDescription & 
    getFieldByNumber(int fieldNum) const override
    {
        return StructureDescriptionBase::getFieldByNumber(fieldNum);
    }

    virtual int getVersion() const override
    {
        return StructureDescriptionBase::getVersion();
    }

    void collectUnparseableJson(Json::Value Struct::* member)
    {
        this->onUnknownField = [=] (Struct * obj, JsonParsingContext & context)
            {
                std::function<Json::Value & (int, Json::Value &)> getEntry
                = [&] (int n, Json::Value & curr) -> Json::Value &
                {
                    if (n == context.pathLength())
                        return curr;
                    else if (context.pathEntry(n).index != -1)
                        return getEntry(n + 1, curr[context.pathEntry(n).index]);
                    else return getEntry(n + 1, curr[context.pathEntry(n).fieldName()]);
                };

                getEntry(0, obj->*member) = context.expectJson();
            };
    }

    virtual void fixupAlign(size_t knownWidth, size_t knownAlign) override
    {
        ExcAssertLessEqual(knownWidth, this->width);
        ExcAssertLessEqual(knownAlign, this->align);
    }
};

/** Base class for an implementation of a structure description.  It
    derives from StructureDescription<Struct>, and also registers
    itself.
*/

template<typename Struct, typename Impl>
struct StructureDescriptionImpl
    :  public StructureDescription<Struct> {
    
    StructureDescriptionImpl(bool nullAccepted = false)
        : StructureDescription<Struct>(nullAccepted)
    {
        regme.done = true;
    }
    
    static RegisterValueDescriptionI<Struct, Impl> regme;
};

template<typename Struct, typename Impl>
RegisterValueDescriptionI<Struct, Impl>
StructureDescriptionImpl<Struct, Impl>::
regme;


template<typename Struct>
template<typename V>
void StructureDescription<Struct>::
addParent(ValueDescriptionT<V> * description_)
{
    // Make sure it has its values
    description_->initialize();

    StructureDescription<V> * desc2
        = dynamic_cast<StructureDescription<V> *>(description_);
    if (!desc2) {
        delete description_;
        throw MLDB::Exception("parent description is not a structure");
    }

    std::shared_ptr<StructureDescription<V> > description(desc2);
    parents.push_back(description);

    constexpr size_t BASE = 0x1000;  // can't use a null pointer
    Struct * p = reinterpret_cast<Struct *>(BASE);
    V * p2 = static_cast<V *>(p);

    size_t ofs = reinterpret_cast<size_t>(p2) - BASE;

    //using namespace std;
    //cerr << "parent " << description_->typeName << " of " << this->typeName << " is at offset " << ofs << endl;

    ExcAssert(!desc2->orderedFields.empty());

    for (auto & oit: description->orderedFields) {
        FieldDescription & ofd = const_cast<FieldDescription &>(oit->second);
        const std::string & name = ofd.fieldName;

        //cerr << "  bringing field " << ofd.fieldName << " of type " << ofd.description->typeName
        //     << " at " << ofd.offset << ":" << ofd.width << " -> " << ofd.offset + ofs << ":" << ofd.width
        //     << endl;

        fieldNames.emplace_back(::strdup(name.c_str()));
        const char * fieldName = fieldNames.back().get();

        auto it = fields.insert(Fields::value_type(fieldName, FieldDescription())).first;
        FieldDescription & fd = it->second;
        fd.fieldName = fieldName;
        fd.comment = ofd.comment;
        fd.description = ofd.description;
        
        fd.offset = ofd.offset + ofs;
        fd.width = ofd.width;
        fd.fieldNum = fields.size() - 1;
        orderedFields.push_back(it);
    }
}

} // namespace MLDB
