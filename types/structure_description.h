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
#include <string_view>

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

    bool hasLess = true, hasEquality = true, hasWeak = true, hasStrong = true, hasPartial = true;

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
        bool operator () (const std::string_view & s1, const std::string_view & s2) const;
    };

    typedef std::map<std::string_view, FieldDescription, StrCompare> Fields;
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

    FieldDescription &
    addFieldDesc(std::string name,
                 size_t offset,
                 std::string comment,
                 std::shared_ptr<const ValueDescription> description);

    /// Add a bit field, specified by the containing field and a bit offset
    void addBitFieldDesc(std::string name,
                         size_t offset,
                         std::string comment,
                         std::shared_ptr<const ValueDescription> containingDescription,
                         uint32_t bitOffset,
                         uint32_t bitWidth);

    /// Add a discriminated field, including the function used to know whether it is active
    /// or not and a string description of that function.
    void addDiscriminatedFieldDesc(std::string name,
                                   size_t offset,
                                   std::string comment,
                                   std::shared_ptr<const ValueDescription> desc,
                                   std::function<bool (const void *)> isActive,
                                   std::string isActiveStr);

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

    // Comparisons
    virtual bool hasEqualityComparison() const;
    virtual bool compareEquality(const void * val1, const void * val2) const;
    virtual bool hasLessThanComparison() const;
    virtual bool compareLessThan(const void * val1, const void * val2) const;
    virtual bool hasStrongOrderingComparison() const;
    virtual std::strong_ordering compareStrong(const void * val1, const void * val2) const;
    virtual bool hasWeakOrderingComparison() const;
    virtual std::weak_ordering compareWeak(const void * val1, const void * val2) const;
    virtual bool hasPartialOrderingComparison() const;
    virtual std::partial_ordering comparePartial(const void * val1, const void * val2) const;
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

    // Comparisons
    virtual bool hasEqualityComparison() const override
    {
        return StructureDescriptionBase::hasEqualityComparison();
    }

    virtual bool compareEquality(const void * val1, const void * val2) const override
    {
        return StructureDescriptionBase::compareEquality(val1, val2);
    }

    virtual bool hasLessThanComparison() const override
    {
        return StructureDescriptionBase::hasLessThanComparison();
    }

    virtual bool compareLessThan(const void * val1, const void * val2) const override
    {
        return StructureDescriptionBase::compareLessThan(val1, val2);
    }

    virtual bool hasStrongOrderingComparison() const override
    {
        return StructureDescriptionBase::hasStrongOrderingComparison();
    }

    virtual std::strong_ordering compareStrong(const void * val1, const void * val2) const override
    {
        return StructureDescriptionBase::compareStrong(val1, val2);
    }

    virtual bool hasWeakOrderingComparison() const override
    {
        return StructureDescriptionBase::hasWeakOrderingComparison();
    }

    virtual std::weak_ordering compareWeak(const void * val1, const void * val2) const override
    {
        return StructureDescriptionBase::compareWeak(val1, val2);
    }

    virtual bool hasPartialOrderingComparison() const override
    {
        return StructureDescriptionBase::hasPartialOrderingComparison();
    }

    virtual std::partial_ordering comparePartial(const void * val1, const void * val2) const override
    {
        return StructureDescriptionBase::comparePartial(val1, val2);
    }
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

    template<typename V, typename Base1, typename Base2>
    void addField(std::string name,
                  Base2 Base1::* field1,
                  V Base2::* field2,
                  std::string comment)
    {
        addFieldDesc(name, field1, field2, comment, getDefaultDescriptionSharedT<V>());
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

    /** Add a field, which is part of a sub-structure.
    */
    template<typename V, typename Base1, typename Base2, typename Desc>
    void addFieldDesc(std::string name,
                      Base2 Base1::* field1,
                      V Base2::* field2,
                      std::string comment,
                      std::shared_ptr<Desc> description)
    {
        Struct * p1 = nullptr;
        Base2 * p2 = nullptr;
        size_t offset1 = (size_t)&(p1->*field1);
        size_t offset2 = (size_t)&(p2->*field2);
        StructureDescriptionBase::addFieldDesc(std::move(name), offset1 + offset2, std::move(comment), std::move(description));
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

    template<typename V, typename Base>
    void addBitField(std::string name,
                     V Base::* containingField,
                     uint32_t bitStart,
                     uint32_t bitWidth,
                     std::function<V (const void *)> extract,
                     std::string comment)
    {
        addBitFieldDesc(std::move(name), containingField, bitStart, bitWidth,
                        std::move(extract), std::move(comment),
                        getDefaultDescriptionSharedT<V>());
    }

    template<typename V, typename Bits, typename Base>
    void addBitFieldCast(std::string name,
                         Bits Base::* containingFieldBits,
                         uint32_t bitStart,
                         uint32_t bitWidth,
                         std::function<V (const void *)> extract,
                         std::string comment)
    {
        static_assert(sizeof(Bits) == sizeof(V));
        typedef V Base::* ContainingField; 
        auto containingField = (ContainingField)containingFieldBits;
        addBitFieldDesc(std::move(name), containingField, bitStart, bitWidth,
                        std::move(extract), std::move(comment),
                        getDefaultDescriptionSharedT<V>());
    }

    template<typename V, typename Base>
    void addBitField(std::string name,
                     V Base::* containingField,
                     uint32_t bitStart,
                     uint32_t bitWidth,
                     std::string comment)
    {
        addBitFieldCast<V>(std::move(name), containingField, bitStart, bitWidth, std::move(comment));
    }

    template<typename Field, typename V, typename Base>
    void addBitFieldCast(std::string name,
                         V Base::* containingFieldBits,
                         uint32_t bitStart,
                         uint32_t bitWidth,
                         std::string comment)
    {
        static_assert(sizeof(Field) == sizeof(V));
        typedef Field Base::* ContainingField; 
        auto containingField = (ContainingField)containingFieldBits;

        std::shared_ptr<const ValueDescription> desc = getDefaultDescriptionSharedT<Field>();

        std::function<Field (const void *)> extract = [bitStart,bitWidth,containingField,desc] (const void * obj) -> Field
        {
            const Field * containing = &(reinterpret_cast<const Struct *>(obj)->*containingField);
            //using namespace std;
            //cerr << "    extract: containing = " << (uint64_t)*containing << endl;
            Field result;
            desc->extractBitField(containing, &result, bitStart, bitWidth);
            //cerr << "    extract: result = " << (uint64_t)result << endl;
            return result;
        };

        addBitFieldDesc(std::move(name), containingField, bitStart, bitWidth,
                        std::move(extract), std::move(comment),
                        desc);
    }

    template<typename V, typename Base, typename Desc>
    void addBitFieldDesc(std::string name,
                         V Base::* containingField,
                         uint32_t bitStart,
                         uint32_t bitWidth,
                         std::function<V (const void *)> extract,
                         std::string comment,
                         std::shared_ptr<Desc> description)
    {
        Struct * p = nullptr;
        size_t offset = (size_t)&(p->*containingField);
        StructureDescriptionBase::addBitFieldDesc(std::move(name), offset, std::move(comment), std::move(description),
                                                  bitStart, bitWidth);
    }

    template<typename V, typename Base>
    void addDiscriminatedField(std::string name,
                               V Base::* field,
                               std::function<bool (const void * obj)> isActive,
                               std::string comment,
                               std::string isActiveComment)
    {
        addDiscriminatedFieldDesc(std::move(name), field, std::move(isActive),
                                  std::move(comment),
                                  std::move(isActiveComment),
                                  getDefaultDescriptionSharedT<V>());
    }

    template<typename V, typename Base, typename Desc>
    void addDiscriminatedFieldDesc(std::string name,
                                   V Base::* field,
                                   std::function<bool (const void *)> isActive,
                                   std::string comment,
                                   std::string isActiveComment,
                                   std::shared_ptr<Desc> description)
    {
        Struct * p = nullptr;
        size_t offset = (size_t)&(p->*field);
        StructureDescriptionBase::
        addDiscriminatedFieldDesc(std::move(name), offset, std::move(comment), std::move(description),
                                  isActive, isActiveComment);
    }


    using ValueDescriptionT<Struct>::parents;

    template<typename V>
    void addParent(ValueDescriptionT<V> * description_
                   = getDefaultDescription((V *)0));

    template<typename V>
    void addParentAsField(const std::string & name, const std::string & comment,
                          std::shared_ptr<const ValueDescriptionT<V>> description_ = getDefaultDescriptionSharedT<V>());

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

    // Comparisons
    virtual bool hasEqualityComparison() const override
    {
        return StructureDescriptionBase::hasEqualityComparison();
    }

    virtual bool compareEquality(const void * val1, const void * val2) const override
    {
        return StructureDescriptionBase::compareEquality(val1, val2);
    }

    virtual bool hasLessThanComparison() const override
    {
        return StructureDescriptionBase::hasLessThanComparison();
    }

    virtual bool compareLessThan(const void * val1, const void * val2) const override
    {
        return StructureDescriptionBase::compareLessThan(val1, val2);
    }

    virtual bool hasStrongOrderingComparison() const override
    {
        return StructureDescriptionBase::hasStrongOrderingComparison();
    }

    virtual std::strong_ordering compareStrong(const void * val1, const void * val2) const override
    {
        return StructureDescriptionBase::compareStrong(val1, val2);
    }

    virtual bool hasWeakOrderingComparison() const override
    {
        return StructureDescriptionBase::hasWeakOrderingComparison();
    }

    virtual std::weak_ordering compareWeak(const void * val1, const void * val2) const override
    {
        return StructureDescriptionBase::compareWeak(val1, val2);
    }

    virtual bool hasPartialOrderingComparison() const override
    {
        return StructureDescriptionBase::hasPartialOrderingComparison();
    }

    virtual std::partial_ordering comparePartial(const void * val1, const void * val2) const override
    {
        return StructureDescriptionBase::comparePartial(val1, val2);
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

template<typename Struct, typename Parent>
/*constexpr*/ size_t getParentOffset()
{
    constexpr size_t BASE = 0x1000;  // can't use a null pointer
    Struct * p = reinterpret_cast<Struct *>(BASE);
    Parent * p2 = static_cast<Parent *>(p);
    size_t ofs = reinterpret_cast<size_t>(p2) - BASE;
    return ofs;
}

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
        auto name = type_name(*description_);
        delete description_;
        throw MLDB::Exception("parent description is not a structure: it is " + name);
    }

    std::shared_ptr<StructureDescription<V> > description(desc2);
    parents.push_back(description);

    constexpr size_t BASE = 0x1000;  // can't use a null pointer
    Struct * p = reinterpret_cast<Struct *>(BASE);
    V * p2 = static_cast<V *>(p);

    size_t ofs = getParentOffset<Struct, V>();

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

        if (ofd.isActive) {
            fd.isActive = ofd.isActive;
            //MLDB_THROW_UNIMPLEMENTED("TODO: copy isActive");
        }
        fd.isActiveStr = ofd.isActiveStr;

        if (ofd.bitField.has_value()) {
            fd.bitField = *ofd.bitField;
            //MLDB_THROW_UNIMPLEMENTED("TODO: copy bitfield");
        }

        orderedFields.push_back(it);
    }
}

template<typename Struct>
template<typename V>
void StructureDescription<Struct>::
addParentAsField(const std::string & name, const std::string & comment,
                 std::shared_ptr<const ValueDescriptionT<V>> description)
{
    StructureDescriptionBase::addFieldDesc(name, getParentOffset<Struct, V>(), comment, description);
}

} // namespace MLDB
