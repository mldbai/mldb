/** twitter_importer.cc
    Francois Maillet, 20 fevrier 2016
    This file is part of MLDB. Copyright 2016 Datacratic. All rights reserved.

*/

#include "mldb/core/procedure.h"
#include "mldb/core/dataset.h"
#include "mldb/types/structure_description.h"
#include "mldb/types/any_impl.h"
#include "mldb/ext/libtwitcurl/twitcurl.h"

using namespace std;


namespace Datacratic {
namespace MLDB {


/*****************************************************************************/
/* SENTIWORDNET IMPORTER                                                     */
/*****************************************************************************/

struct TwitterImporterConfig : ProcedureConfig {
    TwitterImporterConfig()
    {
        outputDataset.withType("sparse.mutable");
    }

    std::string consumerKey;
    std::string consumerSecret;

    std::string username;
    std::string password;

    std::string searchQuery;
    std::string resultCount;

    PolyConfigT<Dataset> outputDataset;
};

DECLARE_STRUCTURE_DESCRIPTION(TwitterImporterConfig);

DEFINE_STRUCTURE_DESCRIPTION(TwitterImporterConfig);

TwitterImporterConfigDescription::
TwitterImporterConfigDescription()
{
    addField("consumerKey", &TwitterImporterConfig::consumerKey,
             "Consumer key for OAuth. Obtained by by registering your app at twitter.com");
    addField("consumerSecret", &TwitterImporterConfig::consumerSecret,
             "Consumer key for OAuth. Obtained by by registering your app at twitter.com");
    
    addField("username", &TwitterImporterConfig::username,
             "Twitter username");
    addField("password", &TwitterImporterConfig::password,
             "Twitter password");

    addField("searchQuery", &TwitterImporterConfig::searchQuery,
            "Search query");
    addField("resultCount", &TwitterImporterConfig::resultCount,
            "Result count");
    
    addField("outputDataset", &TwitterImporterConfig::outputDataset,
             "Output dataset for result",
             PolyConfigT<Dataset>().withType("sparse.mutable"));
    addParent<ProcedureConfig>();
}

struct TwitterImporter: public Procedure {

    TwitterImporter(MldbServer * owner,
                 PolyConfig config_,
                 const std::function<bool (const Json::Value &)> & onProgress)
        : Procedure(owner)
    {
        config = config_.params.convert<TwitterImporterConfig>();
    }
    
    TwitterImporterConfig config;

    virtual RunOutput run(const ProcedureRunConfig & run,
                          const std::function<bool (const Json::Value &)> & onProgress) const
    {
        auto runProcConf = applyRunConfOverProcConf(config, run);

        std::shared_ptr<Dataset> outputDataset;
        if (!runProcConf.outputDataset.type.empty() || !runProcConf.outputDataset.id.empty()) {
            outputDataset = createDataset(server, runProcConf.outputDataset, nullptr, true /*overwrite*/);
        }


        twitCurl twitterObj;

        twitterObj.setTwitterUsername( runProcConf.username );
        twitterObj.setTwitterPassword( runProcConf.password );


        /* OAuth flow begins */
        /* Step 0: Set OAuth related params. These are got by registering your app at twitter.com */
        twitterObj.getOAuth().setConsumerKey( runProcConf.consumerKey );
        twitterObj.getOAuth().setConsumerSecret( runProcConf.consumerSecret );

        /* Step 2: Get request token key and secret */
        std::string authUrl;
        twitterObj.oAuthRequestToken( authUrl );

        /* Step 3: Get PIN  */
        /* Else, pass auth url to twitCurl and get it via twitCurl PIN handling */
        twitterObj.oAuthHandlePIN( authUrl );

        /* Step 4: Exchange request token with access token */
        twitterObj.oAuthAccessToken();

        /* Step 5: Now, save this access token key and secret for future use without PIN */
        std::string replyMsg;

        /* Account credentials verification */
        if( !twitterObj.accountVerifyCredGet() ) {
            twitterObj.getLastCurlError( replyMsg );
            ML::Exception("twitter.import::accountVerifyCredGet error:\n%s\n", replyMsg.c_str() );
        }

        if( !twitterObj.search( "mchacks", "recent" )) { // runProcConf.searchQuery, runProcConf.resultCount ) ) {
            twitterObj.getLastCurlError( replyMsg );
            ML::Exception("twitter.import::search error:\n%s\n", replyMsg.c_str() );
        }

        twitterObj.getLastWebResponse( replyMsg );
        Json::Value jsTweets = Json::parse(replyMsg);
        vector<pair<RowName, vector<tuple<ColumnName, CellValue, Date> > > > rows;

        Json::Value status = jsTweets["search_metadata"];
        for(const auto & tweet : jsTweets["statuses"]) {
            if(!tweet.isMember("text"))
                status = tweet;
            
            vector<tuple<ColumnName, CellValue, Date> > cols;

            // Sun Feb 21 02:00:48 +0000 2016
            Date d = Date::parse(tweet["created_at"].asString(), "%a %b %d %H:%M:%S %z %Y");

            if(tweet.isMember("entities") && tweet["entities"].isMember("hashtags")) {
                for(const auto & ht : tweet["entities"]["hashtags"]) {
                    cols.emplace_back(Coord("hashtag."+ht["text"].asStringUtf8()), 1, d);
                }
            }

            cols.emplace_back(Coord("favorite_count"), tweet["favorite_count"].asInt(), d);
            cols.emplace_back(Coord("retweet_count"), tweet["retweet_count"].asInt(), d);
            cols.emplace_back(Coord("lang"), tweet["lang"].asStringUtf8(), d);
            cols.emplace_back(Coord("text"), tweet["text"].asStringUtf8(), d);
            cols.emplace_back(Coord("source"), tweet["source"].asStringUtf8(), d);

            cols.emplace_back(Coord("place.country"), tweet["place"]["country"].asStringUtf8(), d);

            cols.emplace_back(Coord("user.id"), tweet["user"]["id_str"].asString(), d);
            cols.emplace_back(Coord("user.location"), tweet["user"]["location"].asStringUtf8(), d);
            cols.emplace_back(Coord("user.utc_offset"), tweet["user"]["utc_offset"].asInt(), d);
            cols.emplace_back(Coord("user.screen_name"), tweet["user"]["screen_name"].asStringUtf8(), d);

            rows.emplace_back(RowName(tweet["id_str"].asString()), std::move(cols));
        }


        if (outputDataset) {
            outputDataset->recordRows(rows);
            outputDataset->commit();
        }

        RunOutput result(status);
        return result;
    }

    virtual Any getStatus() const
    {
        return Any();
    }

};

RegisterProcedureType<TwitterImporter, TwitterImporterConfig>
regTwitter(builtinPackage(),
                "import.twitter",
                "Import a data using the Twitter APIs",
                "procedures/TwitterImporter.md.html");


} // namespace MLDB
} // namespace Datacratic
