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
                             const std::string & structName = "",
                             bool nullAccepted = false);

    StructureDescriptionBase(const StructureDescriptionBase & other) = delete;
    StructureDescriptionBase(StructureDescriptionBase && other) = delete;
    void operator = (const StructureDescriptionBase & other);
    void operator = (StructureDescriptionBase && other);

    const std::type_info * type;
    std::string structName;
    bool nullAccepted;
    ValueDescription * owner;

    typedef ValueDescription::FieldDescription FieldDescription;

    // Comparison object to allow const char * objects to be looked up
    // in the map and so for comparisons to be done with no memory
    // allocations.
    struct StrCompare {
        inline bool operator () (const char * s1, const char * s2) const
        {
            char c1 = *s1++, c2 = *s2++;

            if (c1 < c2) return true;
            if (c1 > c2) return false;
            if (c1 == 0) return false;

            c1 = *s1++; c2 = *s2++;
            
            if (c1 < c2) return true;
            if (c1 > c2) return false;
            if (c1 == 0) return false;

            return std::strcmp(s1, s2) < 0;
        }

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
                         const std::string & structName = "")
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

    virtual bool onEntry(void * output, JsonParsingContext & context) const
    {
        if (onEntryHandler) {
            if (!onEntryHandler((Struct *)output, context))
                return false;
        }
        
        if (onUnknownField)
            context.onUnknownFieldHandlers.push_back([=,&context] (const ValueDescription *) { this->onUnknownField((Struct *)output, context); });

        return true;
    }
    
    virtual void onExit(void * output, JsonParsingContext & context) const
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
        ExcAssert(description);

        if (fields.count(name.c_str()))
            throw MLDB::Exception("field '" + name + "' added twice");

        fieldNames.emplace_back(::strdup(name.c_str()));
        const char * fieldName = fieldNames.back().get();
        
        auto it = fields.insert
            (Fields::value_type(fieldName, FieldDescription()))
            .first;
        
        FieldDescription & fd = it->second;
        fd.fieldName = fieldName;
        fd.comment = comment;
        fd.description = description;
        Struct * p = nullptr;
        fd.offset = (size_t)&(p->*field);
        fd.fieldNum = fields.size() - 1;
        orderedFields.push_back(it);
        //using namespace std;
        //cerr << "offset = " << fd.offset << endl;
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
        if (fields.count(name.c_str()))
            throw MLDB::Exception("field '" + name + "' added twice");

        fieldNames.emplace_back(::strdup(name.c_str()));
        const char * fieldName = fieldNames.back().get();
        
        auto it = fields.insert
            (Fields::value_type(fieldName, FieldDescription()))
            .first;
        
        auto desc = std::make_shared<Desc>(defaultValue, baseDesc);
        
        FieldDescription & fd = it->second;
        fd.fieldName = fieldName;
        fd.comment = comment;
        fd.description = std::move(desc);
        Struct * p = nullptr;
        fd.offset = (size_t)&(p->*field);
        fd.fieldNum = fields.size() - 1;
        orderedFields.push_back(it);
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

    virtual size_t getFieldCount(const void * val) const
    {
        return fields.size();
    }

    virtual const FieldDescription *
    hasField(const void * val, const std::string & field) const
    {
        auto it = fields.find(field.c_str());
        if (it != fields.end())
            return &it->second;
        return nullptr;
    }

    virtual void forEachField(const void * val,
                              const std::function<void (const FieldDescription &)> & onField) const
    {
        for (auto f: orderedFields) {
            onField(f->second);
        }
    }

    virtual const FieldDescription & 
    getField(const std::string & field) const
    {
        auto it = fields.find(field.c_str());
        if (it != fields.end())
            return it->second;
        throw MLDB::Exception("structure has no field " + field);
    }

    virtual void parseJson(void * val, JsonParsingContext & context) const
    {
        return StructureDescriptionBase::parseJson(val, context);
    }

    virtual void printJson(const void * val, JsonPrintingContext & context) const
    {
        return StructureDescriptionBase::printJson(val, context);
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

    Struct * p = nullptr;
    V * p2 = static_cast<V *>(p);

    size_t ofs = (size_t)p2;

    ExcAssert(!desc2->orderedFields.empty());

    for (auto & oit: description->orderedFields) {
        FieldDescription & ofd = const_cast<FieldDescription &>(oit->second);
        const std::string & name = ofd.fieldName;

        fieldNames.emplace_back(::strdup(name.c_str()));
        const char * fieldName = fieldNames.back().get();

        auto it = fields.insert(Fields::value_type(fieldName, FieldDescription())).first;
        FieldDescription & fd = it->second;
        fd.fieldName = fieldName;
        fd.comment = ofd.comment;
        fd.description = std::move(ofd.description);
        
        fd.offset = ofd.offset + ofs;
        fd.fieldNum = fields.size() - 1;
        orderedFields.push_back(it);
    }
}

} // namespace MLDB
