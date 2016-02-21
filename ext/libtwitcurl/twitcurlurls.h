#ifndef _TWITCURLURLS_H_
#define _TWITCURLURLS_H_

#include <string>
#include <cstring>

/* Default values used in twitcurl */
namespace twitCurlDefaults
{
    /* Constants */
    const int TWITCURL_DEFAULT_BUFFSIZE = 1024;
    const std::string TWITCURL_COLON = ":";
    const char TWITCURL_EOS = '\0';
    const unsigned int MAX_TIMELINE_TWEET_COUNT = 200;

    /* Miscellaneous data used to build twitter URLs*/
    const std::string TWITCURL_STATUSSTRING = "status=";
    const std::string TWITCURL_TEXTSTRING = "text=";
    const std::string TWITCURL_QUERYSTRING = "query=";
    const std::string TWITCURL_SEARCHQUERYSTRING = "q=";
    const std::string TWITCURL_SCREENNAME = "screen_name=";
    const std::string TWITCURL_USERID = "user_id=";
    const std::string TWITCURL_EXTENSIONFORMATS[2] = { ".json",
                                                       ".xml"
                                                     };
    const std::string TWITCURL_PROTOCOLS[2] =        { "https://",
                                                       "http://"
                                                     };
    const std::string TWITCURL_TARGETSCREENNAME = "target_screen_name=";
    const std::string TWITCURL_TARGETUSERID = "target_id=";
    const std::string TWITCURL_SINCEID = "since_id=";
    const std::string TWITCURL_TRIMUSER = "trim_user=true";
    const std::string TWITCURL_INCRETWEETS = "include_rts=true";
    const std::string TWITCURL_COUNT = "count=";
    const std::string TWITCURL_NEXT_CURSOR = "cursor=";
    const std::string TWITCURL_SKIP_STATUS = "skip_status=";
    const std::string TWITCURL_INCLUDE_ENTITIES = "include_entities=";
    const std::string TWITCURL_STRINGIFY_IDS = "stringify_ids=";
    const std::string TWITCURL_INREPLYTOSTATUSID = "in_reply_to_status_id=";

    /* URL separators */
    const std::string TWITCURL_URL_SEP_AMP = "&";
    const std::string TWITCURL_URL_SEP_QUES = "?";
};

/* Default twitter URLs */
namespace twitterDefaults
{
    /* Base URL */
    const std::string TWITCURL_BASE_URL = "api.twitter.com/1.1/";

    /* Search URLs */
    const std::string TWITCURL_SEARCH_URL = TWITCURL_BASE_URL + "search/tweets";

    /* Status URLs */
    const std::string TWITCURL_STATUSUPDATE_URL = TWITCURL_BASE_URL + "statuses/update";
    const std::string TWITCURL_STATUSSHOW_URL = TWITCURL_BASE_URL + "statuses/show/";
    const std::string TWITCURL_STATUDESTROY_URL = TWITCURL_BASE_URL + "statuses/destroy/";
    const std::string TWITCURL_RETWEET_URL = TWITCURL_BASE_URL + "statuses/retweet/";

    /* Timeline URLs */
    const std::string TWITCURL_HOME_TIMELINE_URL = TWITCURL_BASE_URL + "statuses/home_timeline";
    const std::string TWITCURL_PUBLIC_TIMELINE_URL = TWITCURL_BASE_URL + "statuses/public_timeline";
    const std::string TWITCURL_FEATURED_USERS_URL = TWITCURL_BASE_URL + "statuses/featured";
    const std::string TWITCURL_FRIENDS_TIMELINE_URL = TWITCURL_BASE_URL + "statuses/friends_timeline";
    const std::string TWITCURL_MENTIONS_URL = TWITCURL_BASE_URL + "statuses/mentions_timeline";
    const std::string TWITCURL_USERTIMELINE_URL = TWITCURL_BASE_URL + "statuses/user_timeline";

    /* Users URLs */
    const std::string TWITCURL_LOOKUPUSERS_URL = TWITCURL_BASE_URL + "users/lookup";
    const std::string TWITCURL_SHOWUSERS_URL = TWITCURL_BASE_URL + "users/show";
    const std::string TWITCURL_SHOWFRIENDS_URL = TWITCURL_BASE_URL + "statuses/friends";
    const std::string TWITCURL_SHOWFOLLOWERS_URL = TWITCURL_BASE_URL + "statuses/followers";

    /* Direct messages URLs */
    const std::string TWITCURL_DIRECTMESSAGES_URL = TWITCURL_BASE_URL + "direct_messages";
    const std::string TWITCURL_DIRECTMESSAGENEW_URL = TWITCURL_BASE_URL + "direct_messages/new";
    const std::string TWITCURL_DIRECTMESSAGESSENT_URL = TWITCURL_BASE_URL + "direct_messages/sent";
    const std::string TWITCURL_DIRECTMESSAGEDESTROY_URL = TWITCURL_BASE_URL + "direct_messages/destroy/";

    /* Friendships URLs */
    const std::string TWITCURL_FRIENDSHIPSCREATE_URL = TWITCURL_BASE_URL + "friendships/create";
    const std::string TWITCURL_FRIENDSHIPSDESTROY_URL = TWITCURL_BASE_URL + "friendships/destroy";
    const std::string TWITCURL_FRIENDSHIPSSHOW_URL = TWITCURL_BASE_URL + "friendships/show";

    /* Social graphs URLs */
    const std::string TWITCURL_FRIENDSIDS_URL = TWITCURL_BASE_URL + "friends/ids";
    const std::string TWITCURL_FOLLOWERSIDS_URL = TWITCURL_BASE_URL + "followers/ids";

    /* Account URLs */
    const std::string TWITCURL_ACCOUNTRATELIMIT_URL = TWITCURL_BASE_URL + "account/rate_limit_status";
    const std::string TWITCURL_ACCOUNTVERIFYCRED_URL = TWITCURL_BASE_URL + "account/verify_credentials";

    /* Favorites URLs */
    const std::string TWITCURL_FAVORITESGET_URL = TWITCURL_BASE_URL + "favorites";
    const std::string TWITCURL_FAVORITECREATE_URL = TWITCURL_BASE_URL + "favorites/create/";
    const std::string TWITCURL_FAVORITEDESTROY_URL = TWITCURL_BASE_URL + "favorites/destroy/";

    /* Block URLs */
    const std::string TWITCURL_BLOCKSCREATE_URL = TWITCURL_BASE_URL + "blocks/create/";
    const std::string TWITCURL_BLOCKSDESTROY_URL = TWITCURL_BASE_URL + "blocks/destroy/";
    const std::string TWITCURL_BLOCKSLIST_URL = TWITCURL_BASE_URL + "blocks/list";
    const std::string TWITCURL_BLOCKSIDS_URL = TWITCURL_BASE_URL + "blocks/ids";

    /* Saved Search URLs */
    const std::string TWITCURL_SAVEDSEARCHGET_URL = TWITCURL_BASE_URL + "saved_searches";
    const std::string TWITCURL_SAVEDSEARCHSHOW_URL = TWITCURL_BASE_URL + "saved_searches/show/";
    const std::string TWITCURL_SAVEDSEARCHCREATE_URL = TWITCURL_BASE_URL + "saved_searches/create";
    const std::string TWITCURL_SAVEDSEARCHDESTROY_URL = TWITCURL_BASE_URL + "saved_searches/destroy/";

    /* Trends URLs */
    const std::string TWITCURL_TRENDS_URL = TWITCURL_BASE_URL + "trends";
    const std::string TWITCURL_TRENDSDAILY_URL = TWITCURL_BASE_URL + "trends/daily";
    const std::string TWITCURL_TRENDSCURRENT_URL = TWITCURL_BASE_URL + "trends/current";
    const std::string TWITCURL_TRENDSWEEKLY_URL = TWITCURL_BASE_URL + "trends/weekly";
    const std::string TWITCURL_TRENDSAVAILABLE_URL = TWITCURL_BASE_URL + "trends/available";

};

namespace oAuthLibDefaults
{
    /* Constants */
    const int OAUTHLIB_BUFFSIZE = 1024;
    const int OAUTHLIB_BUFFSIZE_LARGE = 1024;
    const std::string OAUTHLIB_CONSUMERKEY_KEY = "oauth_consumer_key";
    const std::string OAUTHLIB_CALLBACK_KEY = "oauth_callback";
    const std::string OAUTHLIB_VERSION_KEY = "oauth_version";
    const std::string OAUTHLIB_SIGNATUREMETHOD_KEY = "oauth_signature_method";
    const std::string OAUTHLIB_SIGNATURE_KEY = "oauth_signature";
    const std::string OAUTHLIB_TIMESTAMP_KEY = "oauth_timestamp";
    const std::string OAUTHLIB_NONCE_KEY = "oauth_nonce";
    const std::string OAUTHLIB_TOKEN_KEY = "oauth_token";
    const std::string OAUTHLIB_TOKENSECRET_KEY = "oauth_token_secret";
    const std::string OAUTHLIB_VERIFIER_KEY = "oauth_verifier";
    const std::string OAUTHLIB_SCREENNAME_KEY = "screen_name";
    const std::string OAUTHLIB_AUTHENTICITY_TOKEN_KEY = "authenticity_token";
    const std::string OAUTHLIB_SESSIONUSERNAME_KEY = "session[username_or_email]";
    const std::string OAUTHLIB_SESSIONPASSWORD_KEY = "session[password]";
    const std::string OAUTHLIB_AUTHENTICITY_TOKEN_TWITTER_RESP_KEY = "authenticity_token\" type=\"hidden\" value=\"";
    const std::string OAUTHLIB_TOKEN_TWITTER_RESP_KEY = "oauth_token\" type=\"hidden\" value=\"";
    const std::string OAUTHLIB_PIN_TWITTER_RESP_KEY = "code-desc\"><code>";
    const std::string OAUTHLIB_TOKEN_END_TAG_TWITTER_RESP = "\">";
    const std::string OAUTHLIB_PIN_END_TAG_TWITTER_RESP = "</code>";

    const std::string OAUTHLIB_AUTHHEADER_STRING = "Authorization: OAuth ";
};

namespace oAuthTwitterApiUrls
{
    /* Twitter OAuth API URLs */
    const std::string OAUTHLIB_TWITTER_REQUEST_TOKEN_URL = "api.twitter.com/oauth/request_token";
    const std::string OAUTHLIB_TWITTER_AUTHORIZE_URL = "api.twitter.com/oauth/authorize?oauth_token=";
    const std::string OAUTHLIB_TWITTER_ACCESS_TOKEN_URL = "api.twitter.com/oauth/access_token";
};

#endif // _TWITCURLURLS_H_
