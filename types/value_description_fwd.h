/* value_description_fwd.h                                         -*- C++ -*-
   Jeremy Barnes, 29 March 2013
   Copyright (c) 2013 mldb.ai inc.  All rights reserved.

   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

   Code for description and introspection of values and structures.  Used
   to allow for automated formatters and parsers to be built.
*/

#pragma once

#include <string>
#include <memory>

namespace Json {
class Value;
} // namespace Json

namespace MLDB {

using half = _Float16;

class Utf8String;
class Utf32String;
struct JsonParsingContext;
struct JsonPrintingContext;
struct JsonParsingContext;
struct StreamingJsonParsingContext;
struct StructuredJsonParsingContext;
struct StringJsonParsingContext;
struct Utf8StringJsonParsingContext;
struct JsonPrintingContext;
struct JSConverters;
struct ValueDescription;
template<typename T> struct ValueDescriptionT;
template<typename T> struct ValueDescriptionInit;

struct ValueDescriptionInitBase {
    static void initialize(ValueDescription & desc);  // calls desc.initialise()
};

// Forward declarations of encoding/decoding for templates.  The
// value_description.h file will need to be included before
// instantiation.
template<typename T> Json::Value jsonEncode(const T & obj);
template<typename T> std::string jsonEncodeStr(const T & obj);
template<typename T> Utf8String jsonEncodeUtf8(const T & obj);
template<typename T> std::ostream & jsonEncodeToStream(const T & obj, std::ostream & stream);

template<typename T> T jsonDecode(const Json::Value & json, T * = 0);
template<typename T> T jsonDecodeStr(const std::string & json, T * = 0);
template<typename T> T jsonDecodeStr(const Utf8String & json, T * = 0);
template<typename T> T jsonDecodeStream(std::istream & stream, T * = 0);

/** Tag structure to indicate that we want to only construct a
    description, not initialize it.  Initialization can be
    performed later.
*/

struct ConstructOnly {
};

static constexpr ConstructOnly constructOnly = {};


enum class ValueKind : int32_t {
    // Atomic, ie all or none is replaced
    ATOM,     ///< Generic, atomic type
    INTEGER,
    FLOAT,
    BOOLEAN,
    STRING,
    ENUM,

    // Non-atomic, ie part of them can be mutated
    OPTIONAL,
    LINK,
    ARRAY,
    STRUCTURE,
    TUPLE,
    VARIANT,
    MAP,
    ANY
};

enum class OwnershipModel : int32_t {
    NONE,
    UNIQUE,
    SHARED
};

/// Models the length of an array
enum class LengthModel : int32_t {
    FIXED,
    VARIABLE
};

template<typename T, ValueKind V, class Desc> struct ValueDescriptionI;

std::ostream & operator << (std::ostream & stream, ValueKind kind);

struct ShouldNeverByFoundByLookup;

ValueDescriptionT<ShouldNeverByFoundByLookup>
getDefaultDescription(ShouldNeverByFoundByLookup *);

/** Return the shared copy of the default description for this value.  This
    will look it up in the registry, and if not found, will create (and
    register) it.
*/
template<typename T>
std::shared_ptr<const ValueDescriptionT<T> >
getDefaultDescriptionShared(T *);

template<typename T>
auto getDefaultDescriptionSharedT()
{
    using namespace MLDB;
    return getDefaultDescriptionShared((T *)0);
}

template<typename T>
auto getDefaultDescriptionSharedGenericT()
{
    using namespace MLDB;
    return getDefaultDescriptionSharedGeneric((T *)0);
}

template<typename T>
auto getDefaultDescriptionUninitialized(T *)
{
    using namespace MLDB;
    return getDefaultDescription((T *)0);
}

/// Macro to introduce a class TypeDescription that is a structure
/// description for that type, and a getDefaultDescription()
/// overload for it.  The constructor still needs to be done.
#define DECLARE_STRUCTURE_DESCRIPTION_NAMED(Name, Type)         \
    struct Name;                                                \
                                                                \
    MLDB::ValueDescriptionT<Type> *                             \
    getDefaultDescription(Type *);                              \
                                                                \
    MLDB::ValueDescriptionT<Type> *                             \
    getDefaultDescriptionUninitialized(Type *);                 \
                                                                \
    std::shared_ptr<const MLDB::ValueDescription>               \
    getDefaultDescriptionSharedGeneric(Type *);                 \

#define DEFINE_STRUCTURE_DESCRIPTION_NAMED(Name, Type)          \
                                                                \
    struct Name                                                 \
        :  public MLDB::StructureDescription<Type> {            \
        Name();                                                 \
                                                                \
        Name(const MLDB::ConstructOnly &);                      \
                                                                \
        virtual void initialize()                               \
        {                                                       \
            Name newMe;                                         \
            *this = std::move(newMe);                           \
        }                                                       \
                                                                \
        struct Regme;                                           \
        static Regme regme;                                     \
    };                                                          \
                                                                \
    struct Name::Regme {                                                \
        bool done;                                                      \
        static ValueDescription * creator() { return new Name(); }      \
        Regme()                                                         \
            : done(false)                                               \
        {                                                               \
            MLDB::registerValueDescriptionFunctions                     \
                (typeid(Type), creator, true);                          \
        }                                                               \
    };                                                                  \
                                                                        \
    Name::Name(const MLDB::ConstructOnly &)                             \
    {                                                                   \
        regme.done = true;                                              \
    }                                                                   \
                                                                        \
    MLDB::ValueDescriptionT<Type> *                                     \
    getDefaultDescription(Type *)                                       \
    {                                                                   \
        return new Name();                                              \
    }                                                                   \
                                                                        \
    MLDB::ValueDescriptionT<Type> *                                     \
    getDefaultDescriptionUninitialized(Type *)                          \
    {                                                                   \
        return new Name(::MLDB::constructOnly);                         \
    }                                                                   \
                                                                        \
    std::shared_ptr<const MLDB::ValueDescription>                       \
    getDefaultDescriptionSharedGeneric(Type *)                          \
    {                                                                   \
        return ValueDescription::get(typeid(Type));                     \
    }                                                                   \
    Name::Regme Name::regme;                                            \
    

#define DECLARE_STRUCTURE_DESCRIPTION(Type)                      \
    DECLARE_STRUCTURE_DESCRIPTION_NAMED(Type##Description, Type)

#define DEFINE_STRUCTURE_DESCRIPTION(Type)                      \
    DEFINE_STRUCTURE_DESCRIPTION_NAMED(Type##Description, Type)

#define DEFINE_STRUCTURE_DESCRIPTION_INLINE(type) \
    DEFINE_STRUCTURE_DESCRIPTION(type) \
    type##Description::type##Description()

#define IMPLEMENT_STRUCTURE_DESCRIPTION(type)   \
    DECLARE_STRUCTURE_DESCRIPTION(type) \
    DEFINE_STRUCTURE_DESCRIPTION(type) \
    type##Description::type##Description()

#define IMPLEMENT_STRUCTURE_DESCRIPTION_NAMED(name, type)   \
    DECLARE_STRUCTURE_DESCRIPTION_NAMED(name, type) \
    DEFINE_STRUCTURE_DESCRIPTION_NAMED(name, type) \
    name::name()

#define DECLARE_ENUM_DESCRIPTION_NAMED(Name, Type)                  \
                                                                    \
    MLDB::ValueDescriptionT<Type> *                                 \
    getDefaultDescription(Type *);                                  \
                                                                    \
    MLDB::ValueDescriptionT<Type> *                                 \
    getDefaultDescriptionUninitialized(Type *);                     \
    
#define DEFINE_ENUM_DESCRIPTION_NAMED(Name, Type)                   \
                                                                    \
    struct Name                                                     \
        : public MLDB::EnumDescription<Type> {                      \
        Name();                                                     \
    };                                                              \
                                                                    \
    MLDB::ValueDescriptionT<Type> *                                 \
    getDefaultDescription(Type *)                                   \
    {                                                               \
        return new Name();                                          \
    }                                                               \
                                                                    \
    MLDB::ValueDescriptionT<Type> *                                 \
    getDefaultDescriptionUninitialized(Type *)                      \
    {                                                               \
        return new Name();                                          \
    }                                                               \
    

#define DECLARE_ENUM_DESCRIPTION(Type)                      \
    DECLARE_ENUM_DESCRIPTION_NAMED(Type##Description, Type)

#define DEFINE_ENUM_DESCRIPTION(Type)                           \
    DEFINE_ENUM_DESCRIPTION_NAMED(Type##Description, Type)

#define DEFINE_ENUM_DESCRIPTION_INLINE(type) \
    DEFINE_ENUM_DESCRIPTION(type) \
    type##Description::type##Description()

#define IMPLEMENT_ENUM_DESCRIPTION(type)        \
    DECLARE_ENUM_DESCRIPTION(type) \
    DEFINE_ENUM_DESCRIPTION(type) \
    type##Description::type##Description()

#define PREDECLARE_VALUE_DESCRIPTION(T)                                 \
    MLDB::ValueDescriptionT<T> *                                        \
    getDefaultDescription(T * = 0);                                     \
    MLDB::ValueDescriptionT<T> *                                        \
    getDefaultDescriptionUninitialized(T * = 0);                        \
    std::shared_ptr<MLDB::ValueDescriptionT<T> >                        \
    getDefaultDescriptionShared(T * = 0);                               \
    std::shared_ptr<const MLDB::ValueDescription>                       \
    getDefaultDescriptionSharedGeneric(T * = 0);                        \
    
#define DECLARE_VALUE_DESCRIPTION(T)                                    \
    PREDECLARE_VALUE_DESCRIPTION(T)                                     \
    extern template struct ValueDescriptionT<T>;                        \

#define DEFINE_VALUE_DESCRIPTION_NS(T, Desc)                            \
    MLDB::ValueDescriptionT<T> *                                        \
    getDefaultDescription(T *)                                          \
    {                                                                   \
        return new Desc();                                              \
    }                                                                   \
                                                                        \
    MLDB::ValueDescriptionT<T> *                                        \
    getDefaultDescriptionUninitialized(T *)                             \
    {                                                                   \
        return new Desc();                                              \
    }                                                                   \
                                                                        \
    std::shared_ptr<MLDB::ValueDescriptionT<T> >                        \
    getDefaultDescriptionShared(T *)                                    \
    {                                                                   \
        static std::shared_ptr<Desc> result = std::make_shared<Desc>(); \
        return result;                                                  \
    }                                                                   \
                                                                        \
    std::shared_ptr<const MLDB::ValueDescription>                       \
    getDefaultDescriptionSharedGeneric(T *)                             \
    {                                                                   \
        return getDefaultDescriptionShared((T*)0);                      \
    }                                                                   \

#define DEFINE_VALUE_DESCRIPTION(T, Desc)                               \
    DEFINE_VALUE_DESCRIPTION_NS(T, Desc)                                \
    template struct MLDB::ValueDescriptionT<T>;                         \

#define VD_SINGLE_ARG(...) __VA_ARGS__


#define DECLARE_TEMPLATE_VALUE_DESCRIPTION_N(Impl, Type, ParamList, ArgList, Enable) \
    template<ParamList>                                                 \
    ValueDescriptionT<Type<ArgList> > *                                 \
    getDefaultDescription(Type<ArgList> * = 0, std::enable_if_t<Enable> * = 0)                          \
    {                                                                   \
        return new Impl<ArgList>();                                     \
    }                                                                   \
                                                                        \
    template<ParamList>                                                 \
    struct ValueDescriptionInit<Type<ArgList> >: public ValueDescriptionInitBase { \
        static ValueDescription * create()                              \
        {                                                               \
            return new Impl<ArgList>(MLDB::ConstructOnly());            \
        }                                                               \
    };                                                                  \
                                                                        \
    template<ParamList>                                                 \
    struct has_default_description<Type<ArgList>> : std::integral_constant<bool, Enable> {} \


#define DECLARE_TEMPLATE_VALUE_DESCRIPTION_1(Impl, Type, ArgType1, Arg1, Enable) \
    DECLARE_TEMPLATE_VALUE_DESCRIPTION_N(Impl, Type, VD_SINGLE_ARG(ArgType1 Arg1), VD_SINGLE_ARG(Arg1), Enable)

#define DECLARE_TEMPLATE_VALUE_DESCRIPTION_2(Impl, Type, ArgType1, Arg1, ArgType2, Arg2, Enable) \
    DECLARE_TEMPLATE_VALUE_DESCRIPTION_N(Impl, Type, VD_SINGLE_ARG(ArgType1 Arg1, ArgType2 Arg2), VD_SINGLE_ARG(Arg1, Arg2), Enable)

#define DECLARE_TEMPLATE_VALUE_DESCRIPTION_3(Impl, Type, ArgType1, Arg1, ArgType2, Arg2, ArgType3, Arg3, Enable) \
    DECLARE_TEMPLATE_VALUE_DESCRIPTION_N(Impl, Type, VD_SINGLE_ARG(ArgType1 Arg1, ArgType2 Arg2, ArgType3 Arg3), VD_SINGLE_ARG(Arg1, Arg2, Arg3), Enable)

#define DECLARE_TEMPLATE_VALUE_DESCRIPTION_4(Impl, Type, ArgType1, Arg1, ArgType2, Arg2, ArgType3, Arg3, ArgType4, Arg4, Enable) \
    DECLARE_TEMPLATE_VALUE_DESCRIPTION_N(Impl, Type, VD_SINGLE_ARG(ArgType1 Arg1, ArgType2 Arg2, ArgType3 Arg3, ArgType4 Arg4), VD_SINGLE_ARG(Arg1, Arg2, Arg3, Arg4), Enable)

#define DECLARE_TEMPLATE_VALUE_DESCRIPTION_5(Impl, Type, ArgType1, Arg1, ArgType2, Arg2, ArgType3, Arg3, ArgType4, Arg4, ArgType5, Arg5, Enable) \
    DECLARE_TEMPLATE_VALUE_DESCRIPTION_N(Impl, Type, VD_SINGLE_ARG(ArgType1 Arg1, ArgType2 Arg2, ArgType3 Arg3, ArgType4 Arg4, ArgType5 Arg5), VD_SINGLE_ARG(Arg1, Arg2, Arg3, Arg4, Arg5), Enable)

#define DECLARE_TEMPLATE_VALUE_DESCRIPTION_6(Impl, Type, ArgType1, Arg1, ArgType2, Arg2, ArgType3, Arg3, ArgType4, Arg4, ArgType5, Arg5, ArgType6, Arg6, Enable) \
    DECLARE_TEMPLATE_VALUE_DESCRIPTION_N(Impl, Type, VD_SINGLE_ARG(ArgType1 Arg1, ArgType2 Arg2, ArgType3 Arg3, ArgType4 Arg4, ArgType5 Arg5, ArgType6 Arg6), VD_SINGLE_ARG(Arg1, Arg2, Arg3, Arg4, Arg5, Arg6), Enable)



/*****************************************************************************/
/* DEFAULT DESCRIPTIONS FOR BASIC TYPES                                      */
/*****************************************************************************/

PREDECLARE_VALUE_DESCRIPTION(std::string);
PREDECLARE_VALUE_DESCRIPTION(Utf8String);
PREDECLARE_VALUE_DESCRIPTION(Utf32String);
PREDECLARE_VALUE_DESCRIPTION(signed char);
PREDECLARE_VALUE_DESCRIPTION(unsigned char);
PREDECLARE_VALUE_DESCRIPTION(char);
PREDECLARE_VALUE_DESCRIPTION(signed short int);
PREDECLARE_VALUE_DESCRIPTION(unsigned short int);
PREDECLARE_VALUE_DESCRIPTION(signed int);
PREDECLARE_VALUE_DESCRIPTION(unsigned int);
PREDECLARE_VALUE_DESCRIPTION(signed long);
PREDECLARE_VALUE_DESCRIPTION(unsigned long);
PREDECLARE_VALUE_DESCRIPTION(signed long long);
PREDECLARE_VALUE_DESCRIPTION(unsigned long long);
PREDECLARE_VALUE_DESCRIPTION(half);
PREDECLARE_VALUE_DESCRIPTION(float);
PREDECLARE_VALUE_DESCRIPTION(double);
PREDECLARE_VALUE_DESCRIPTION(Json::Value);
PREDECLARE_VALUE_DESCRIPTION(bool);

template<typename T>
ValueDescriptionT<T *> * getDefaultDescription(T ** ptr);

// Base case where there is no default description defined, return nullptr_t
inline std::nullptr_t getDefaultDescriptionMaybe(...)
{
    return nullptr;
}

// If there is a default description defined, return it
template<typename T>
auto getDefaultDescriptionMaybe(T *) -> decltype(getDefaultDescription((T*)0))
{
    return getDefaultDescription((T*)0);
}

// Base case where there is no default description defined, return nullptr_t
inline std::nullptr_t getDefaultDescriptionSharedMaybe(...)
{
    return nullptr;
}

// If there is a default description defined, return it
template<typename T>
auto getDefaultDescriptionSharedMaybe(T *) -> decltype(getDefaultDescription((T*)0), getDefaultDescriptionShared((T*)0))
{
    return getDefaultDescriptionShared((T*)0);
}

// Base case where there is no default description defined, return nullptr_t
inline std::shared_ptr<const ValueDescription> getDefaultDescriptionSharedGenericMaybe(...)
{
    return nullptr;
}

// If there is a default description defined, return it
template<typename T>
auto getDefaultDescriptionSharedGenericMaybe(T *) -> decltype(getDefaultDescriptionSharedGeneric((T*)0))
{
    return getDefaultDescriptionSharedGeneric((T*)0);
}

// Allow overriding based on whether a type has a default description defined or not
template<typename T, typename Return = decltype(getDefaultDescriptionMaybe((T*)0))>
struct has_default_description_probe : std::true_type {};

template<typename T>
struct has_default_description_probe<T, std::nullptr_t> : std::false_type {};

template<typename T> struct has_default_description : has_default_description_probe<T> {};

template<typename T>
inline constexpr bool has_default_description_v = has_default_description<T>::value;

template<typename T>
using has_default_description_t = typename has_default_description<T>::type;

template<bool v1, bool v2>
struct and_base : std::false_type {};

template<>
struct and_base<true, true> : std::true_type {};

template<typename... Types>
struct all_have_default_descriptions;

template<>
struct all_have_default_descriptions<> : std::true_type {};

template<typename T1, typename... Rest>
struct all_have_default_descriptions<T1, Rest...>
  : and_base<all_have_default_descriptions<Rest...>::value, has_default_description_v<T1>> {};

template<typename... Ts>
inline constexpr bool all_have_default_descriptions_v = all_have_default_descriptions<Ts...>::value;

template<typename... Ts>
using all_have_default_descriptions_t = typename all_have_default_descriptions<Ts...>::type;

} // namespace MLDB
