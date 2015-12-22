// This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

/** nlp.cc
    Francois Maillet, 20 octobre 2015
    Copyright (c) 2015 Datacratic Inc.  All rights reserved.

*/

#include "nlp.h"
#include "mldb/server/mldb_server.h"
#include "mldb/types/basic_value_descriptions.h"
#include "mldb/server/function_contexts.h"
#include "mldb/types/any_impl.h"
#include "mldb/sql/tokenize.h"
#include "mldb/base/parse_context.h"

using namespace std;


namespace Datacratic {
namespace MLDB {



/*****************************************************************************/
/* APPLY STOP WORDS FUNCTION CONFIG                                          */
/*****************************************************************************/

DEFINE_STRUCTURE_DESCRIPTION(ApplyStopWordsFunctionConfig);

ApplyStopWordsFunctionConfigDescription::
ApplyStopWordsFunctionConfigDescription()
{
}

/*****************************************************************************/
/* APPLY STOP WORDS FUNCTION                                                 */
/*****************************************************************************/
                      
ApplyStopWordsFunction::
ApplyStopWordsFunction(MldbServer * owner,
               PolyConfig config,
               const std::function<bool (const Json::Value &)> & onProgress)
    : Function(owner)
{
    //functionConfig = config.params.convert<ApplyStopWordsFunctionConfig>();

    stopwords = {{"english", {"a's","able","about","above","according","accordingly","across","actually","after","afterwards","again","against","ain't","all","allow","allows","almost","alone","along","already","also","although","always","am","among","amongst","an","and","another","any","anybody","anyhow","anyone","anything","anyway","anyways","anywhere","apart","appear","appreciate","appropriate","are","aren't","around","as","aside","ask","asking","associated","at","available","away","awfully","be","became","because","become","becomes","becoming","been","before","beforehand","behind","being","believe","below","beside","besides","best","better","between","beyond","both","brief","but","by","c'mon","c's","came","can","can't","cannot","cant","cause","causes","certain","certainly","changes","clearly","co","com","come","comes","concerning","consequently","consider","considering","contain","containing","contains","corresponding","could","couldn't","course","currently","definitely","described","despite","did","didn't","different","do","does","doesn't","doing","don't","done","down","downwards","during","each","edu","eg","eight","either","else","elsewhere","enough","entirely","especially","et","etc","even","ever","every","everybody","everyone","everything","everywhere","ex","exactly","example","except","far","few","fifth","first","five","followed","following","follows","for","former","formerly","forth","four","from","further","furthermore","get","gets","getting","given","gives","go","goes","going","gone","got","gotten","greetings","had","hadn't","happens","hardly","has","hasn't","have","haven't","having","he","he's","hello","help","hence","her","here","here's","hereafter","hereby","herein","hereupon","hers","herself","hi","him","himself","his","hither","hopefully","how","howbeit","however","i'd","i'll","i'm","i've","ie","if","ignored","immediate","in","inasmuch","inc","indeed","indicate","indicated","indicates","inner","insofar","instead","into","inward","is","isn't","it","it'd","it'll","it's","its","itself","just","keep","keeps","kept","know","known","knows","last","lately","later","latter","latterly","least","less","lest","let","let's","like","liked","likely","little","look","looking","looks","ltd","mainly","many","may","maybe","me","mean","meanwhile","merely","might","more","moreover","most","mostly","much","must","my","myself","name","namely","nd","near","nearly","necessary","need","needs","neither","never","nevertheless","new","next","nine","no","nobody","non","none","noone","nor","normally","not","nothing","novel","now","nowhere","obviously","of","off","often","oh","ok","okay","old","on","once","one","ones","only","onto","or","other","others","otherwise","ought","our","ours","ourselves","out","outside","over","overall","own","particular","particularly","per","perhaps","placed","please","plus","possible","presumably","probably","provides","que","quite","qv","rather","rd","re","really","reasonably","regarding","regardless","regards","relatively","respectively","right","said","same","saw","say","saying","says","second","secondly","see","seeing","seem","seemed","seeming","seems","seen","self","selves","sensible","sent","serious","seriously","seven","several","shall","she","should","shouldn't","since","six","so","some","somebody","somehow","someone","something","sometime","sometimes","somewhat","somewhere","soon","sorry","specified","specify","specifying","still","sub","such","sup","sure","t's","take","taken","tell","tends","th","than","thank","thanks","thanx","that","that's","thats","the","their","theirs","them","themselves","then","thence","there","there's","thereafter","thereby","therefore","therein","theres","thereupon","these","they","they'd","they'll","they're","they've","think","third","this","thorough","thoroughly","those","though","three","through","throughout","thru","thus","to","together","too","took","toward","towards","tried","tries","truly","try","trying","twice","two","un","under","unfortunately","unless","unlikely","until","unto","up","upon","us","use","used","useful","uses","using","usually","value","various","very","via","viz","vs","want","wants","was","wasn't","way","we","we'd","we'll","we're","we've","welcome","well","went","were","weren't","what","what's","whatever","when","whence","whenever","where","where's","whereafter","whereas","whereby","wherein","whereupon","wherever","whether","which","while","whither","who","who's","whoever","whole","whom","whose","why","will","willing","wish","with","within","without","won't","wonder","would","wouldn't","yes","yet","you","you'd","you'll","you're","you've","your","yours","yourself","yourselves","zero"}}};

    auto it = stopwords.find(functionConfig.language);
    if(it == stopwords.end())
        throw ML::Exception("Unsupported language: " + functionConfig.language);

    selected_stopwords = &(it->second);
}

Any
ApplyStopWordsFunction::
getStatus() const
{
    return Any();
}


FunctionOutput
ApplyStopWordsFunction::
apply(const FunctionApplier & applier,
      const FunctionContext & context) const
{
    RowValue rtnRow;
    auto onAtom = [&] (const Id & columnName,
                       const Id & prefix,
                       const CellValue & val,
                       Date ts)
        {
            const string colStr = columnName.toString();
            if(selected_stopwords->find(colStr) == selected_stopwords->end()) {
                rtnRow.push_back(make_tuple(columnName, val, ts));
            }

            return true;
        };

    ExpressionValue args = context.get<ExpressionValue>("words");
    args.forEachAtom(onAtom);


    FunctionOutput foResult;
    foResult.set("words", rtnRow);
    return foResult;
}

FunctionInfo
ApplyStopWordsFunction::
getFunctionInfo() const
{

    FunctionInfo result;
    result.input.addRowValue("words");
    result.output.addRowValue("words");

    return result;
}

static RegisterFunctionType<ApplyStopWordsFunction, ApplyStopWordsFunctionConfig>
regAppylStopWordsFunction(builtinPackage(),
                          "filter_stopwords",
                          "Apply a list of stop words to column names",
                          "functions/FilterStopWords.md.html",
                          nullptr,
                          { MldbEntity::INTERNAL_ENTITY });



/*****************************************************************************/
/* STEMMER FUNCTION CONFIG                                                   */
/*****************************************************************************/

DEFINE_STRUCTURE_DESCRIPTION(StemmerFunctionConfig);

StemmerFunctionConfigDescription::
StemmerFunctionConfigDescription()
{
    addField("language", &StemmerFunctionConfig::language,
            "Stemming algorithm to use", string("english"));
}

/*****************************************************************************/
/* STEMMER FUNCTION                                                          */
/*****************************************************************************/

StemmerFunction::
StemmerFunction(MldbServer * owner,
               PolyConfig config,
               const std::function<bool (const Json::Value &)> & onProgress)
    : Function(owner)
{
    functionConfig = config.params.convert<StemmerFunctionConfig>();

    stemmer = std::unique_ptr<sb_stemmer>(
            sb_stemmer_new(functionConfig.language.c_str(), "UTF_8"));

    if (!stemmer) {
        throw ML::Exception(ML::format("language `%s' not available for stemming in "
                "encoding `%s'", functionConfig.language, "utf8"));
    }
}

Any
StemmerFunction::
getStatus() const
{
    return Any();
}


FunctionOutput
StemmerFunction::
apply(const FunctionApplier & applier,
      const FunctionContext & context) const
{
    // the stemmer is not thread safe
    unique_lock<mutex> guard(apply_mutex);

    map<Id, pair<int, Date>> accum;

    auto onAtom = [&] (const Id & columnName,
                       const Id & prefix,
                       const CellValue & val,
                       Date ts)
        {
            string str = columnName.toString();
            // cerr << "got: " << str << endl;
            const sb_symbol * stemmed = sb_stemmer_stem(stemmer.get(),
                    (const unsigned char*)str.c_str(), str.size());

            if (stemmed == NULL) {
                throw ML::Exception("Out of memory when stemming");
            }

            Id col(string((const char*)stemmed));
            auto it = accum.find(col);
            if(it == accum.end()) {
                accum.emplace(col, std::move(make_pair(1, ts)));
            }
            else {
                it->second.first ++;
                if(it->second.second > ts)
                    it->second.second = ts;
            }


            return true;
        };


    ExpressionValue args = context.get<ExpressionValue>("words");
    args.forEachAtom(onAtom);

    RowValue rtnRow;
    for(auto it=accum.begin(); it != accum.end(); it++) {
        rtnRow.push_back(make_tuple(it->first, it->second.first, it->second.second));
    }

    FunctionOutput foResult;
    foResult.set("words", rtnRow);
    return foResult;
}

FunctionInfo
StemmerFunction::
getFunctionInfo() const
{
    FunctionInfo result;
    result.input.addRowValue("words");
    result.output.addRowValue("words");

    return result;
}

static RegisterFunctionType<StemmerFunction, StemmerFunctionConfig>
regStemmerFunction(builtinPackage(),
                   "stemmer",
                   "Apply a stemming algorithm column names",
                   "functions/Stemmer.md.html");


/*****************************************************************************/
/* STEMMER ON DOCUMENT FUNCTION                                              */
/*****************************************************************************/

StemmerOnDocumentFunction::
StemmerOnDocumentFunction(MldbServer * owner,
               PolyConfig config,
               const std::function<bool (const Json::Value &)> & onProgress)
    : Function(owner)
{
    functionConfig = config.params.convert<StemmerFunctionConfig>();

    stemmer = std::unique_ptr<sb_stemmer>(
            sb_stemmer_new(functionConfig.language.c_str(), "UTF_8"));

    if (!stemmer) {
        throw ML::Exception(ML::format("language `%s' not available for stemming in "
                "encoding `%s'", functionConfig.language, "utf8"));
    }
}

Any
StemmerOnDocumentFunction::
getStatus() const
{
    return Any();
}


FunctionOutput
StemmerOnDocumentFunction::
apply(const FunctionApplier & applier,
      const FunctionContext & context) const
{
    // the stemmer is not thread safe
    unique_lock<mutex> guard(apply_mutex);

    Utf8String accum;

    auto onGram = [&](Utf8String& word) -> bool
    {
        const string& str = word.rawString(); 

        const sb_symbol * stemmed = sb_stemmer_stem(stemmer.get(),
                    (const unsigned char*)str.c_str(), str.size());

        if (stemmed == NULL) {
            throw ML::Exception("Out of memory when stemming");
        }

        Utf8String out((const char*)stemmed);

        if (accum.empty())
          accum = out;
        else
          accum += " " + out;
       
        return true;
    };

    ExpressionValue args = context.get<ExpressionValue>("document");
    Utf8String text = args.toUtf8String();
    ML::Parse_Context pcontext(text.rawData(), text.rawData(), text.rawLength());

    tokenize_exec(onGram, pcontext, " ", "", 0);

    FunctionOutput foResult;
    foResult.set("document", ExpressionValue(accum, args.getEffectiveTimestamp()));
    return foResult;
}

FunctionInfo
StemmerOnDocumentFunction::
getFunctionInfo() const
{
    FunctionInfo result;
    result.input.addAtomValue("document");
    result.output.addAtomValue("document");

    return result;
}

static RegisterFunctionType<StemmerOnDocumentFunction, StemmerFunctionConfig>
regStemmerOnDocumentFunction(builtinPackage(),
                   "stemmerdoc",
                   "Apply a stemming algorithm on a single document",
                   "functions/Stemmer.md.html");


} // namespace MLDB
} // namespace Datacratic
