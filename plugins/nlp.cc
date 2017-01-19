// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/** nlp.cc
    Francois Maillet, 20 octobre 2015
    Copyright (c) 2015 mldb.ai inc.  All rights reserved.

*/

#include "nlp.h"
#include "mldb/server/mldb_server.h"
#include "mldb/types/basic_value_descriptions.h"
#include "mldb/types/any_impl.h"
#include "mldb/sql/tokenize.h"
#include "mldb/base/parse_context.h"

using namespace std;



namespace MLDB {


DEFINE_STRUCTURE_DESCRIPTION(Words);

WordsDescription::WordsDescription()
{
    addField("words", &Words::words,
             "Row-valued bag of words where keys are words and values are "
             "counts or weights");
}

DEFINE_STRUCTURE_DESCRIPTION(Document);

DocumentDescription::DocumentDescription()
{
    addField("document", &Document::document,
             "String-valued value containing the text of the document");
}


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
    : BaseT(owner, config)
{
    //functionConfig = config.params.convert<ApplyStopWordsFunctionConfig>();

    stopwords = {{"english", {"a's","able","about","above","according","accordingly","across","actually","after","afterwards","again","against","ain't","all","allow","allows","almost","alone","along","already","also","although","always","am","among","amongst","an","and","another","any","anybody","anyhow","anyone","anything","anyway","anyways","anywhere","apart","appear","appreciate","appropriate","are","aren't","around","as","aside","ask","asking","associated","at","available","away","awfully","be","became","because","become","becomes","becoming","been","before","beforehand","behind","being","believe","below","beside","besides","best","better","between","beyond","both","brief","but","by","c'mon","c's","came","can","can't","cannot","cant","cause","causes","certain","certainly","changes","clearly","co","com","come","comes","concerning","consequently","consider","considering","contain","containing","contains","corresponding","could","couldn't","course","currently","definitely","described","despite","did","didn't","different","do","does","doesn't","doing","don't","done","down","downwards","during","each","edu","eg","eight","either","else","elsewhere","enough","entirely","especially","et","etc","even","ever","every","everybody","everyone","everything","everywhere","ex","exactly","example","except","far","few","fifth","first","five","followed","following","follows","for","former","formerly","forth","four","from","further","furthermore","get","gets","getting","given","gives","go","goes","going","gone","got","gotten","greetings","had","hadn't","happens","hardly","has","hasn't","have","haven't","having","he","he's","hello","help","hence","her","here","here's","hereafter","hereby","herein","hereupon","hers","herself","hi","him","himself","his","hither","hopefully","how","howbeit","however","i'd","i'll","i'm","i've","ie","if","ignored","immediate","in","inasmuch","inc","indeed","indicate","indicated","indicates","inner","insofar","instead","into","inward","is","isn't","it","it'd","it'll","it's","its","itself","just","keep","keeps","kept","know","known","knows","last","lately","later","latter","latterly","least","less","lest","let","let's","like","liked","likely","little","look","looking","looks","ltd","mainly","many","may","maybe","me","mean","meanwhile","merely","might","more","moreover","most","mostly","much","must","my","myself","name","namely","nd","near","nearly","necessary","need","needs","neither","never","nevertheless","new","next","nine","no","nobody","non","none","noone","nor","normally","not","nothing","novel","now","nowhere","obviously","of","off","often","oh","ok","okay","old","on","once","one","ones","only","onto","or","other","others","otherwise","ought","our","ours","ourselves","out","outside","over","overall","own","particular","particularly","per","perhaps","placed","please","plus","possible","presumably","probably","provides","que","quite","qv","rather","rd","re","really","reasonably","regarding","regardless","regards","relatively","respectively","right","said","same","saw","say","saying","says","second","secondly","see","seeing","seem","seemed","seeming","seems","seen","self","selves","sensible","sent","serious","seriously","seven","several","shall","she","should","shouldn't","since","six","so","some","somebody","somehow","someone","something","sometime","sometimes","somewhat","somewhere","soon","sorry","specified","specify","specifying","still","sub","such","sup","sure","t's","take","taken","tell","tends","th","than","thank","thanks","thanx","that","that's","thats","the","their","theirs","them","themselves","then","thence","there","there's","thereafter","thereby","therefore","therein","theres","thereupon","these","they","they'd","they'll","they're","they've","think","third","this","thorough","thoroughly","those","though","three","through","throughout","thru","thus","to","together","too","took","toward","towards","tried","tries","truly","try","trying","twice","two","un","under","unfortunately","unless","unlikely","until","unto","up","upon","us","use","used","useful","uses","using","usually","value","various","very","via","viz","vs","want","wants","was","wasn't","way","we","we'd","we'll","we're","we've","welcome","well","went","were","weren't","what","what's","whatever","when","whence","whenever","where","where's","whereafter","whereas","whereby","wherein","whereupon","wherever","whether","which","while","whither","who","who's","whoever","whole","whom","whose","why","will","willing","wish","with","within","without","won't","wonder","would","wouldn't","yes","yet","you","you'd","you'll","you're","you've","your","yours","yourself","yourselves","zero"}}};

    auto it = stopwords.find(functionConfig.language);
    if(it == stopwords.end())
        throw MLDB::Exception("Unsupported language: " + functionConfig.language);

    selected_stopwords = &(it->second);
}

Words
ApplyStopWordsFunction::
call(Words input) const
{
    RowValue rtnRow;
    auto onAtom = [&] (const Path & columnName,
                       const Path & prefix,
                       const CellValue & val,
                       Date ts)
        {
            const string colStr = columnName.toSimpleName().stealRawString();

            if(!selected_stopwords->count(colStr)) {
                rtnRow.push_back(make_tuple(columnName, val, ts));
            }

            return true;
        };

    input.words.forEachAtom(onAtom);
    
    Words result;
    result.words = std::move(rtnRow);
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
    : BaseT(owner, config)
{
    functionConfig = config.params.convert<StemmerFunctionConfig>();

    //this is just to verify the language at creation time
    std::unique_ptr<sb_stemmer> stemmer(sb_stemmer_new(functionConfig.language.c_str(), "UTF_8"));

    if (!stemmer) {
        throw MLDB::Exception(MLDB::format("language `%s' not available for stemming in "
                "encoding `%s'", functionConfig.language, "utf8"));
    }
}

Words
StemmerFunction::
call(Words input) const
{
    // the sb_stemmer object is not thread safe
    // but this allocation is not very expensive as profiled
    // compared to actually doing the stemming
    std::unique_ptr<sb_stemmer> stemmer(sb_stemmer_new(functionConfig.language.c_str(), "UTF_8"));

    map<PathElement, pair<double, Date> > accum;

    auto onAtom = [&] (const Path & columnName,
                       const Path & prefix,
                       const CellValue & val,
                       Date ts)
        {
            string str = columnName.toSimpleName().stealRawString();

            const sb_symbol * stemmed = sb_stemmer_stem(stemmer.get(),
                    (const unsigned char*)str.c_str(), str.size());

            if (stemmed == nullptr) {
                throw MLDB::Exception("Out of memory when stemming");
            }

            // Cast the cell value as a double before we accumulate them
            double val_as_double;
            if (val.isString())
                val_as_double = 1;
            else
                val_as_double = val.toDouble();

            PathElement col(string((const char*)stemmed));

            auto it = accum.find(col);
            if(it == accum.end()) {
                accum.emplace(col, make_pair(val_as_double, ts));
            }
            else {
                it->second.first += val_as_double;
                if(it->second.second > ts)
                    it->second.second = ts;
            }

            return true;
        };

    input.words.forEachAtom(onAtom);

    RowValue rtnRow;
    rtnRow.reserve(accum.size());
    for(auto & r: accum) {
        rtnRow.emplace_back(r.first, std::move(r.second.first),
                            r.second.second);
    }
    
    Words result;
    result.words = std::move(rtnRow);
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
    : BaseT(owner, config)
{
    functionConfig = config.params.convert<StemmerFunctionConfig>();

    //this is just to verify the language at creation time
    std::unique_ptr<sb_stemmer> stemmer(sb_stemmer_new(functionConfig.language.c_str(), "UTF_8"));

    if (!stemmer) {
        throw MLDB::Exception(MLDB::format("language `%s' not available for stemming in "
                "encoding `%s'", functionConfig.language, "utf8"));
    }
}

Document
StemmerOnDocumentFunction::
call(Document doc) const
{
    // the sb_stemmer object is not thread safe
    // but this allocation is not very expensive as profiled
    // compared to actually doing the stemming
    std::unique_ptr<sb_stemmer> stemmer(sb_stemmer_new(functionConfig.language.c_str(), "UTF_8"));

    Utf8String accum;

    auto onGram = [&](Utf8String& word) -> bool
    {
        const string& str = word.rawString(); 

        const sb_symbol * stemmed = sb_stemmer_stem(stemmer.get(),
                    (const unsigned char*)str.c_str(), str.size());

        if (stemmed == NULL) {
            throw MLDB::Exception("Out of memory when stemming");
        }

        Utf8String out((const char*)stemmed);

        if (accum.empty())
          accum = out;
        else
          accum += " " + out;
       
        return true;
    };

    Utf8String text = doc.document.toUtf8String();
    ParseContext pcontext(text.rawData(), text.rawData(), text.rawLength());

    tokenize_exec(onGram, pcontext, " ", "", 0);

    Document result;
    result.document = ExpressionValue(std::move(accum),
                                      doc.document.getEffectiveTimestamp());
    return result;
}

static RegisterFunctionType<StemmerOnDocumentFunction, StemmerFunctionConfig>
regStemmerOnDocumentFunction(builtinPackage(),
                   "stemmerdoc",
                   "Apply a stemming algorithm on a single document",
                   "functions/Stemmer.md.html");


} // namespace MLDB

