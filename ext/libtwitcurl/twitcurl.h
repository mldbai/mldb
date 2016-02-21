#ifndef _TWITCURL_H_
#define _TWITCURL_H_

#include <string>
#include <sstream>
#include <cstring>
#include <vector>
#include "oauthlib.h"
#include "include/curl/curl.h"

/* Few common types used by twitCurl */
namespace twitCurlTypes
{
    typedef enum _eTwitCurlApiFormatType
    {
        eTwitCurlApiFormatJson = 0,
        eTwitCurlApiFormatXml,
        eTwitCurlApiFormatMax
    } eTwitCurlApiFormatType;

    typedef enum _eTwitCurlProtocolType
    {
        eTwitCurlProtocolHttps = 0,
        eTwitCurlProtocolHttp,
        eTwitCurlProtocolMax
    } eTwitCurlProtocolType;
};

/* twitCurl class */
class twitCurl
{
public:
    twitCurl();
    ~twitCurl();

    /* Twitter OAuth authorization methods */
    oAuth& getOAuth();
    bool oAuthRequestToken( std::string& authorizeUrl /* out */ );
    bool oAuthAccessToken();
    bool oAuthHandlePIN( const std::string& authorizeUrl /* in */ );

    /* Twitter login APIs, set once and forget */
    std::string& getTwitterUsername();
    std::string& getTwitterPassword();
    void setTwitterUsername( const std::string& userName /* in */ );
    void setTwitterPassword( const std::string& passWord /* in */ );

    /* Twitter search APIs */
    bool search( const std::string& searchQuery /* in */, const std::string resultCount = "" /* in */ );

    /* Twitter status APIs */
    bool statusUpdate( const std::string& newStatus /* in */, const std::string inReplyToStatusId = "" /* in */ );
    bool statusShowById( const std::string& statusId /* in */ );
    bool statusDestroyById( const std::string& statusId /* in */ );
    bool retweetById( const std::string& statusId /* in */ );

    /* Twitter timeline APIs */
    bool timelineHomeGet( const std::string sinceId = ""  /* in */ );
    bool timelinePublicGet();
    bool timelineFriendsGet();
    bool timelineUserGet( const bool trimUser /* in */,
	                      const bool includeRetweets /* in */,
                          const unsigned int tweetCount /* in */,
                          const std::string userInfo = "" /* in */,
                          const bool isUserId = false /* in */ );
    bool featuredUsersGet();
    bool mentionsGet( const std::string sinceId = "" /* in */ );

    /* Twitter user APIs */
    bool userLookup( const std::vector<std::string> &userInfo /* in */,  const bool isUserId = false /* in */ );
    bool userGet( const std::string& userInfo /* in */, const bool isUserId = false /* in */ );
    bool friendsGet( const std::string userInfo = "" /* in */, const bool isUserId = false /* in */ );
    bool followersGet( const std::string userInfo = "" /* in */, const bool isUserId = false /* in */ );

    /* Twitter direct message APIs */
    bool directMessageGet( const std::string sinceId = "" /* in */ );
    bool directMessageSend( const std::string& userInfo /* in */, const std::string& dMsg /* in */, const bool isUserId = false /* in */ );
    bool directMessageGetSent();
    bool directMessageDestroyById( const std::string& dMsgId /* in */ );

    /* Twitter friendships APIs */
    bool friendshipCreate( const std::string& userInfo /* in */, const bool isUserId = false /* in */ );
    bool friendshipDestroy( const std::string& userInfo /* in */, const bool isUserId = false /* in */ );
    bool friendshipShow( const std::string& userInfo /* in */, const bool isUserId = false /* in */ );

    /* Twitter social graphs APIs */
    bool friendsIdsGet( const std::string& nextCursor /* in */,
                        const std::string& userInfo /* in */,
	                    const bool isUserId = false /* in */ );
    bool followersIdsGet( const std::string& nextCursor /* in */,
                          const std::string& userInfo /* in */,
	                      const bool isUserId = false /* in */ );

    /* Twitter account APIs */
    bool accountRateLimitGet();
    bool accountVerifyCredGet();

    /* Twitter favorites APIs */
    bool favoriteGet();
    bool favoriteCreate( const std::string& statusId /* in */ );
    bool favoriteDestroy( const std::string& statusId /* in */ );

    /* Twitter block APIs */
    bool blockCreate( const std::string& userInfo /* in */ );
    bool blockDestroy( const std::string& userInfo /* in */ );
    bool blockListGet( const std::string& nextCursor /* in */,
                       const bool includeEntities /* in */,
	                   const bool skipStatus /* in */ );
    bool blockIdsGet( const std::string& nextCursor /* in */, const bool stringifyIds /* in */ );

    /* Twitter search APIs */
    bool savedSearchGet();
    bool savedSearchCreate( const std::string& query /* in */ );
    bool savedSearchShow( const std::string& searchId /* in */ );
    bool savedSearchDestroy( const std::string& searchId /* in */ );

    /* Twitter trends APIs (JSON) */
    bool trendsGet();
    bool trendsDailyGet();
    bool trendsWeeklyGet();
    bool trendsCurrentGet();
    bool trendsAvailableGet();

    /* cURL APIs */
    bool isCurlInit();
    void getLastWebResponse( std::string& outWebResp /* out */ );
    void getLastCurlError( std::string& outErrResp /* out */);

    /* Internal cURL related methods */
    int saveLastWebResponse( char*& data, size_t size );

    /* cURL proxy APIs */
    std::string& getProxyServerIp();
    std::string& getProxyServerPort();
    std::string& getProxyUserName();
    std::string& getProxyPassword();
    void setProxyServerIp( const std::string& proxyServerIp /* in */ );
    void setProxyServerPort( const std::string& proxyServerPort /* in */ );
    void setProxyUserName( const std::string& proxyUserName /* in */ );
    void setProxyPassword( const std::string& proxyPassword /* in */ );

    /* cURL Interface APIs */
    std::string& getInterface();
    void setInterface( const std::string& Interface /* in */ );

    
    /* Clones this object */
    twitCurl* clone();

private:
    /* cURL data */
    CURL* m_curlHandle;
    char* m_errorBuffer;
    std::string m_callbackData;

    /* cURL flags */
    bool m_curlProxyParamsSet;
    bool m_curlLoginParamsSet;
    bool m_curlCallbackParamsSet;
    bool m_curlInterfaseParamSet;

    /* cURL proxy data */
    std::string m_proxyServerIp;
    std::string m_proxyServerPort;
    std::string m_proxyUserName;
    std::string m_proxyPassword;
    std::string m_Interface;
    
    /* Twitter data */
    std::string m_twitterUsername;
    std::string m_twitterPassword;

    /* Twitter API type */
    twitCurlTypes::eTwitCurlApiFormatType m_eApiFormatType;
    twitCurlTypes::eTwitCurlProtocolType m_eProtocolType;

    /* OAuth data */
    oAuth m_oAuth;

    /* Private methods */
    void clearCurlCallbackBuffers();
    void prepareCurlProxy();
    void prepareCurlInterface();
    void prepareCurlCallback();
    void prepareCurlUserPass();
    void prepareStandardParams();
    bool performGet( const std::string& getUrl );
    bool performGetInternal( const std::string& getUrl,
                             const std::string& oAuthHttpHeader );
    bool performDelete( const std::string& deleteUrl );
    bool performPost( const std::string& postUrl, std::string dataStr = "" );

    /* Internal cURL related methods */
    static int curlCallback( char* data, size_t size, size_t nmemb, twitCurl* pTwitCurlObj );
};


/* Private functions */
void utilMakeCurlParams( std::string& outStr, const std::string& inParam1, const std::string& inParam2 );
void utilMakeUrlForUser( std::string& outUrl, const std::string& baseUrl, const std::string& userInfo, const bool isUserId );

#endif // _TWITCURL_H_
