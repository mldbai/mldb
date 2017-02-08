// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/** value_descriptions.cc
    Jeremy Barnes, 5 January 2015
    Copyright (c) 2015 mldb.ai inc.  All rights reserved.

*/

#include "value_descriptions.h"
#include "mldb/ml/jml/dense_features.h"
#include "mldb/jml/utils/configuration.h"
#include "mldb/types/structure_description.h"
#include "mldb/types/vector_description.h"
#include "mldb/types/enum_description.h"
#include "mldb/types/pair_description.h"
#include "mldb/types/map_description.h"
#include "mldb/types/distribution_description.h"

#include "mldb/ml/jml/committee.h"
#include "mldb/ml/jml/decision_tree.h"
#include "mldb/ml/jml/glz_classifier.h"
#include "mldb/ml/jml/stump.h"
#include "mldb/ml/jml/boosted_stumps.h"


using namespace std;
using namespace MLDB;


namespace ML {

using MLDB::getDefaultDescriptionShared;

DEFINE_ENUM_DESCRIPTION_NAMED(FeatureTypeDescription, ML::Feature_Type);
DEFINE_ENUM_DESCRIPTION_NAMED(LinkFunctionDescription, ML::Link_Function);
DEFINE_ENUM_DESCRIPTION_NAMED(RegularizationDescription, ML::Regularization);

struct ClassifierImplDescription
    : public MLDB::ValueDescriptionT<std::shared_ptr<ML::Classifier_Impl> > {

    ClassifierImplDescription();

    virtual void parseJsonTyped(std::shared_ptr<ML::Classifier_Impl>  * val,
                                MLDB::JsonParsingContext & context) const;

    virtual void printJsonTyped(const std::shared_ptr<ML::Classifier_Impl>  * val,
                                MLDB::JsonPrintingContext & context) const;
};

DEFINE_VALUE_DESCRIPTION_NS(std::shared_ptr<ML::Classifier_Impl>,
                            ClassifierImplDescription);

FeatureTypeDescription::
FeatureTypeDescription()
{
    addValue("UNKNOWN",     ML::UNKNOWN, "We have not yet determined the feature type");
    addValue("PRESENCE",    ML::PRESENCE, "Feature is present or not present; value unimportant");
    addValue("BOOLEAN",     ML::BOOLEAN, "feature is true (1.0) or false (0.0)");
    addValue("CATEGORICAL", ML::CATEGORICAL, "feature is categorical; ordering makes no sense");
    addValue("REAL",        ML::REAL, "Feature is real valued");
    addValue("INUTILE",     ML::INUTILE, "Feature is inutile and should be ignored");
    addValue("STRING",      ML::STRING, "Feature is an open categorical feature");
}

LinkFunctionDescription::
LinkFunctionDescription()
{
    addValue("LOGIT", ML::LOGIT, "Logit, good generic link for probabilistic");
    addValue("PROBIT", ML::PROBIT, "Probit, advanced usage");
    addValue("COMP_LOG_LOG", ML::COMP_LOG_LOG, "Also good for probabilistic");
    addValue("LINEAR", ML::LINEAR, "Linear; makes it solve linear least squares (identity)");
    addValue("LOG", ML::LOG, "Logarithm; good for transforming the output of boosting");
}

RegularizationDescription::
RegularizationDescription()
{
    addValue("NONE", ML::Regularization_none, "No regularization.");
    addValue("L1", ML::Regularization_l1, "L1 regularization using LASSO.");
    addValue("L2", ML::Regularization_l2, "L2 regularization using ridge regression.");  
}

struct DenseFeatureSpaceDescription
    : public ValueDescriptionI<ML::Dense_Feature_Space, ValueKind::ATOM,
                               DenseFeatureSpaceDescription > {

    DenseFeatureSpaceDescription()
    {
    }

    virtual void parseJson(void * val, JsonParsingContext & context) const
    {
        auto * val2 = reinterpret_cast<ML::Dense_Feature_Space *>(val);
        return parseJsonTyped(val2, context);
    }

    virtual void parseJsonTyped(ML::Dense_Feature_Space * val, JsonParsingContext & context) const
    {
        throw MLDB::Exception("Can't round-trip a dense feature space through JSON");
    }

    virtual void printJson(const void * val, JsonPrintingContext & context) const
    {
        auto val2 = reinterpret_cast<ML::Dense_Feature_Space const *>(val);
        return printJsonTyped(val2, context);
    }

    virtual void printJsonTyped(ML::Dense_Feature_Space const * val,
                                JsonPrintingContext & context) const
    {
        std::vector<std::string> featureNames = val->feature_names();
        std::vector<std::pair<std::string, ML::Feature_Info> > featureInfo(featureNames.size());

        for (unsigned i = 0;  i < featureNames.size();  ++i) {
            featureInfo[i].first = featureNames[i];
            featureInfo[i].second = val->info(ML::Feature(i));
        }

        static auto desc = getDefaultDescriptionShared(&featureInfo);

        desc->printJsonTyped(&featureInfo, context);
    }
};

DEFINE_VALUE_DESCRIPTION_NS(ML::Dense_Feature_Space,
                            DenseFeatureSpaceDescription);

struct FeatureInfoRepr {
    FeatureInfoRepr()
        : optional(false), biased(false), grouping(false)
    {
    }

    FeatureInfoRepr(const ML::Feature_Info & info)
        : type(info.type()), optional(info.optional()), biased(info.biased()),
          grouping(info.grouping())
    {
    }

    ML::Feature_Type type;
    bool optional;
    bool biased;
    bool grouping;
};

DECLARE_STRUCTURE_DESCRIPTION(FeatureInfoRepr);

DEFINE_STRUCTURE_DESCRIPTION(FeatureInfoRepr);

FeatureInfoReprDescription::
FeatureInfoReprDescription()
{
    addField("type", &FeatureInfoRepr::type, "Type of feature");
    addField("optional", &FeatureInfoRepr::optional, "Feature is optional (don't learn from its absence)", false);
    addField("biased", &FeatureInfoRepr::optional, "Feature is biased (don't learn from it)", false);
    addField("grouping", &FeatureInfoRepr::optional, "Feature is used for grouping (don't learn from its value)", false);
}


struct FeatureInfoDescription
    : public ValueDescriptionI<ML::Feature_Info, ValueKind::ATOM,
                               FeatureInfoDescription > {

    FeatureInfoDescription()
    {
    }

    virtual void parseJson(void * val, JsonParsingContext & context) const
    {
        auto * val2 = reinterpret_cast<ML::Feature_Info *>(val);
        return parseJsonTyped(val2, context);
    }

    virtual void parseJsonTyped(ML::Feature_Info * val, JsonParsingContext & context) const
    {
        throw MLDB::Exception("Can't round-trip a feature info through JSON");
    }

    virtual void printJson(const void * val, JsonPrintingContext & context) const
    {
        auto * val2 = reinterpret_cast<ML::Feature_Info const *>(val);
        return printJsonTyped(val2, context);
    }

    virtual void printJsonTyped(ML::Feature_Info const * val,
                                JsonPrintingContext & context) const
    {
        FeatureInfoRepr rep(*val);

        static auto desc = getDefaultDescriptionShared((FeatureInfoRepr *)0);

        desc->printJsonTyped(&rep, context);
    }
};

DEFINE_VALUE_DESCRIPTION_NS(ML::Feature_Info,
                            FeatureInfoDescription);

struct JmlConfigurationDescription
    : public ValueDescriptionI<ML::Configuration, ValueKind::MAP,
                               JmlConfigurationDescription > {

    JmlConfigurationDescription()
    {
    }

    static std::string toUnadornedString(const Json::Value & val)
    {
        if (val.isString())
            return val.asString();
        else if (val.isNull())
            return "";
        else return val.toStringNoNewLine();
    }

    static void insertInto(ML::Configuration & result,
                           const std::string & prefix,
                           const Json::Value & vals)
    {
        if (vals.isObject()) {
            string prefixPlus = prefix.empty() ? "" : prefix + ".";
            for (auto it = vals.begin(), end = vals.end();
                 it != end;  ++it) {
                insertInto(result, prefixPlus + it.memberName(), *it);
            }
        }
        else {
            result[prefix] = toUnadornedString(vals);
        }
    }

    virtual void parseJsonTyped(ML::Configuration * val, JsonParsingContext & context) const
    {
        ML::Configuration result;

        Json::Value json = context.expectJson();

        if (!json.isObject())
            throw MLDB::Exception("Expected JSON object for configuration");

        insertInto(result, "", json);

        *val = std::move(result);
    }

    static Json::Value & getPath(const std::string & key, Json::Value & val)
    {
        if (key.empty())
            throw MLDB::Exception("can't lookup empty key");

        auto pos = key.find('.');
        if (pos == string::npos)
            return val[key];

        string before(key, 0, pos);
        string after(key, pos + 1);

        return getPath(after, val[before]);
    }

    virtual void printJsonTyped(ML::Configuration const * val,
                                JsonPrintingContext & context) const
    {
        Json::Value result;

        for (const std::string & key: val->allKeys()) {
            std::string value = val->operator [] (key);
            getPath(key, result) = value;
        }
        
        context.writeJson(result);
    }
};

DEFINE_VALUE_DESCRIPTION_NS(ML::Configuration,
                            JmlConfigurationDescription);

static thread_local std::vector<const ML::Feature_Space *> featureSpaceContextStack;

void FeatureSpaceContext::push(const ML::Feature_Space * fs)
{
    ExcAssert(fs);
    featureSpaceContextStack.push_back(fs);
}

void FeatureSpaceContext::pop(const ML::Feature_Space * fs)
{
    ExcAssert(fs);
    ExcAssertGreater(featureSpaceContextStack.size(), 0);
    ExcAssertEqual(featureSpaceContextStack.back(), fs);
    featureSpaceContextStack.pop_back();
}

const ML::Feature_Space * FeatureSpaceContext::current()
{
    ExcAssertGreater(featureSpaceContextStack.size(), 0);
    return featureSpaceContextStack.back();
}


/*****************************************************************************/
/* CLASSIFIERS                                                               */
/*****************************************************************************/

ClassifierImplDescription::
ClassifierImplDescription()
{
}

void
ClassifierImplDescription::
parseJsonTyped(std::shared_ptr<ML::Classifier_Impl>  * val,
               MLDB::JsonParsingContext & context) const
{
    throw MLDB::Exception("Can't parse classifiers");
}

template<typename T>
bool tryType(const std::shared_ptr<Classifier_Impl>  * val,
             const std::string & type,
             MLDB::JsonPrintingContext & context)
{
    auto c = dynamic_cast<T *>(val->get());
    if (c) {
        FeatureSpaceContext fsContext(c->feature_space().get());

        Json::Value result;
        result["type"] = type;
        result["params"] = MLDB::jsonEncode(*c);
        context.writeJson(result);
        return true;
    }
    return false;
}

void
ClassifierImplDescription::
printJsonTyped(const std::shared_ptr<Classifier_Impl>  * val,
               MLDB::JsonPrintingContext & context) const
{
    if (tryType<ML::Committee>(val, "Committee", context)) return;
    if (tryType<ML::GLZ_Classifier>(val, "GLZ", context)) return;
    if (tryType<ML::Decision_Tree>(val, "DecisionTree", context)) return;
    if (tryType<ML::Stump>(val, "Stump", context)) return;
    if (tryType<ML::Boosted_Stumps>(val, "BoostedStumps", context)) return;

    Json::Value result;
    result["type"] = MLDB::type_name(**val);
    result["model"] = (*val)->print();
    context.writeJson(result);
}

DEFINE_ENUM_DESCRIPTION_NAMED(GlzClassifierFeatureTypeDescription,
                              GLZ_Classifier::Feature_Spec::Type);
DEFINE_STRUCTURE_DESCRIPTION_NAMED(GlzClassifierFeatureDescription,
                                   GLZ_Classifier::Feature_Spec);
DEFINE_STRUCTURE_DESCRIPTION_NAMED(DecisionTreeDescription, Decision_Tree);
DEFINE_STRUCTURE_DESCRIPTION_NAMED(TreeBaseDescription, Tree::Base);
DEFINE_STRUCTURE_DESCRIPTION_NAMED(TreeNodeDescription, Tree::Node);
DEFINE_STRUCTURE_DESCRIPTION_NAMED(TreeLeafDescription, Tree::Leaf);
DEFINE_STRUCTURE_DESCRIPTION_NAMED(TreeDescription, Tree);
DEFINE_STRUCTURE_DESCRIPTION_NAMED(GlzClassifierDescription, GLZ_Classifier);
DEFINE_STRUCTURE_DESCRIPTION_NAMED(CommitteeDescription, Committee);
DEFINE_STRUCTURE_DESCRIPTION_NAMED(StumpActionDescription, Action);
DEFINE_STRUCTURE_DESCRIPTION_NAMED(StumpDescription, Stump);
DEFINE_STRUCTURE_DESCRIPTION_NAMED(BoostedStumpsDescription, Boosted_Stumps);
DEFINE_ENUM_DESCRIPTION(Output_Encoding);

DEFINE_ENUM_DESCRIPTION_NAMED(SplitOpDescription, Split::Op);

struct FeatureDescription
    : public MLDB::ValueDescriptionT<ML::Feature> {

    virtual void parseJsonTyped(ML::Feature * val,
                                MLDB::JsonParsingContext & context) const
    {
        throw MLDB::Exception("Can't parse classifiers");
    }

    virtual void printJsonTyped(const ML::Feature * val,
                                MLDB::JsonPrintingContext & context) const
    {
        const ML::Feature_Space * fs = FeatureSpaceContext::current();
        context.writeString(fs->print(*val));
    }
};

FeatureDescription *
getDefaultDescription(ML::Feature * = 0)
{
    return new FeatureDescription();
}

struct SplitDescription
    : public MLDB::ValueDescriptionT<ML::Split> {

    virtual void parseJsonTyped(ML::Split * val,
                                MLDB::JsonParsingContext & context) const
    {
        throw MLDB::Exception("Can't parse classifiers");
    }

    virtual void printJsonTyped(const ML::Split * val,
                                MLDB::JsonPrintingContext & context) const
    {
        const ML::Feature_Space * fs = FeatureSpaceContext::current();

        Json::Value result;
        result["feature"] = MLDB::jsonEncode(val->feature());
        if (val->op() != Split::NOT_MISSING)
            result["value"] = MLDB::jsonEncode(fs->print(val->feature(), val->split_val()));
        result["op"] = MLDB::jsonEncode(val->op());
        
        context.writeJson(result);
    }
};

SplitDescription *
getDefaultDescription(ML::Split * = 0)
{
    return new SplitDescription();
}

struct TreePtrDescription
    : public MLDB::ValueDescriptionT<ML::Tree::Ptr> {

    virtual void parseJsonTyped(ML::Tree::Ptr * val,
                                MLDB::JsonParsingContext & context) const
    {
        throw MLDB::Exception("Can't parse classifiers");
    }

    virtual void printJsonTyped(const ML::Tree::Ptr * val,
                                MLDB::JsonPrintingContext & context) const
    {
        Json::Value result;
        if (val->node()) {
            result = MLDB::jsonEncode(*val->node());
            result["type"] = "node";
        }
        else if (val->leaf()) {
            result = MLDB::jsonEncode(*val->leaf());
            result["type"] = "leaf";
        }
        context.writeJson(result);
    }
};

TreePtrDescription *
getDefaultDescription(ML::Tree::Ptr * = 0)
{
    return new TreePtrDescription();
}

DecisionTreeDescription::
DecisionTreeDescription()
{
    addField("tree", &Decision_Tree::tree, "Tree parameters");
    addField("encoding", &Decision_Tree::encoding, "Encoding of tree output");
}

TreeDescription::
TreeDescription()
{
    addField("root", &Tree::root, "Root of tree");
}

TreeBaseDescription::
TreeBaseDescription()
{
    addField("pred", &Tree::Base::pred,
             "Predictions at this level in the tree");
    addField("examples", &Tree::Base::examples,
             "Number of examples at this level in the tree");
}

TreeNodeDescription::
TreeNodeDescription()
{
    addParent<Tree::Base>();

    addField("split", &Tree::Node::split,
             "Predicate for split");
    addField("z", &Tree::Node::z, "Z score for this split");
    addField("true", &Tree::Node::child_true, "Subtree for if predicate is true");
    addField("false", &Tree::Node::child_false, "Subtree for if predicate is false");
    addField("missing", &Tree::Node::child_missing, "Subtree for if predicate has value missing");
}

TreeLeafDescription::TreeLeafDescription()
{
    addParent<Tree::Base>();
}

Output_EncodingDescription::
Output_EncodingDescription()
{
    addValue("PROB", OE_PROB, "Output is a probability between 0 and 1");
    addValue("PM_INF", OE_PROB, "Output is number between -INFINITY and INFINITY");
    addValue("PM_ONE", OE_PM_ONE, "Output is number between -1 and 1");
}

CommitteeDescription::
CommitteeDescription()
{
    addField("classifiers", &Committee::classifiers,
             "List of classifiers to output");
    addField("weights", &Committee::weights,
             "List of weights for each committee member");
    addField("bias", &Committee::bias,
             "Bias for each committee member");
    addField("encoding", &Committee::encoding,
             "Output encoding for committee");
}

GlzClassifierDescription::
GlzClassifierDescription()
{
    addField("addBias", &GLZ_Classifier::add_bias,
             "Whether or not to add a bias to the output");
    addField("weights", &GLZ_Classifier::weights,
             "Weights learnt by the classifier.  For each input labe we have "
             "a vector of input parameters");
    addField("importances", &GLZ_Classifier::importances,
             "Importance of each parameter to the classification.  Used for explain "
             "to avoid biasing by the magnitude of the parameters.");
    addField("features", &GLZ_Classifier::features,
             "List of features used by the classifier");
    addField("link", &GLZ_Classifier::link,
             "Link function used by the classifier");
}

GlzClassifierFeatureDescription::
GlzClassifierFeatureDescription()
{
    addField("feature", &GLZ_Classifier::Feature_Spec::feature,
             "Feature to be used for GLZ classifier");
    addField("extract", &GLZ_Classifier::Feature_Spec::type,
             "What to extract from the feature");
    addField("category", &GLZ_Classifier::Feature_Spec::category,
             "Category for VALUE_EQUALS");
}

SplitOpDescription::
SplitOpDescription()
{
    addValue("LESS", Split::LESS, "");
    addValue("EQUAL", Split::EQUAL, "");
    addValue("PRESENT", Split::NOT_MISSING, "");
}

GlzClassifierFeatureTypeDescription::
GlzClassifierFeatureTypeDescription()
{
    addValue("VALUE", GLZ_Classifier::Feature_Spec::VALUE,
             "Value of feature; it must be present");
    addValue("VALUE IF PRESENT", GLZ_Classifier::Feature_Spec::VALUE_IF_PRESENT,
             "Value of feature if it is present, or zero otherwise");
    addValue("PRESENCE", GLZ_Classifier::Feature_Spec::PRESENCE,
             "Presence of feature");
    addValue("VALUE_EQUALS", GLZ_Classifier::Feature_Spec::VALUE_EQUALS,
             "Boolean: is the value of the feature equal to the argument?");
}

StumpActionDescription::
StumpActionDescription()
{
    addField("true", &Action::pred_true, "Prediction if predicate holds");
    addField("false", &Action::pred_false, "Prediction if predicate doesn't hold");
    addField("missing", &Action::pred_missing, "Prediction if predicate feature is missing");
}

StumpDescription::
StumpDescription()
{
    addField("split", &Stump::split,
             "Predicate to split on");
    addField("Z", &Stump::Z,
             "Z score of the rule to split on");
    addField("action", &Stump::action, "Action to perform on split");
    addField("encoding", &Stump::encoding, "How the output of the stump is encoded");
}

std::string keyToString(const Split & split)
{
    const ML::Feature_Space * fs = FeatureSpaceContext::current();

    std::string result = MLDB::jsonEncodeStr(split.feature());
    switch (split.op()) {
    case Split::LESS:
        result += "<" + fs->print(split.feature(), split.split_val());
        break;
    case Split::EQUAL:
        result += "=" + fs->print(split.feature(), split.split_val());
        break;
    case Split::NOT_MISSING:
        result += " PRESENT";
        break;
    }
    
    return result;
}

Split stringToKey(const std::string & str, Split * = 0)
{
    throw MLDB::Exception("stringToKey(): to implement");
}

BoostedStumpsDescription::
BoostedStumpsDescription()
{
    addField("stumps",  &Boosted_Stumps::stumps,      "Stumps to be boosted");
    addField("bias",    &Boosted_Stumps::bias,        "Bias of stumps");
    addField("missing", &Boosted_Stumps::sum_missing, "Sum of all missing values");
}


} // namespace ML
