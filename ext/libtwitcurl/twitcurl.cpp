#define NOMINMAX
#include <memory.h>
#include "twitcurlurls.h"
#include "twitcurl.h"
#include "urlencode.h"

/*++
* @method: twitCurl::twitCurl
*
* @description: constructor
*
* @input: none
*
* @output: none
*
*--*/
twitCurl::twitCurl():
m_curlHandle( NULL ),
m_curlProxyParamsSet( false ),
m_curlLoginParamsSet( false ),
m_curlCallbackParamsSet( false ),
m_curlInterfaseParamSet ( false ),
m_eApiFormatType( twitCurlTypes::eTwitCurlApiFormatJson ),
m_eProtocolType( twitCurlTypes::eTwitCurlProtocolHttps )
{
    /* Alloc memory for cURL error responses */
    m_errorBuffer = (char*)malloc( twitCurlDefaults::TWITCURL_DEFAULT_BUFFSIZE );

    /* Clear callback buffers */
    clearCurlCallbackBuffers();

    /* Initialize cURL */
    m_curlHandle = curl_easy_init();
    if( NULL == m_curlHandle )
    {
        std::string dummyStr;
        getLastCurlError( dummyStr );
    }
    curl_easy_setopt(m_curlHandle, CURLOPT_SSL_VERIFYPEER, 0);
}

/*++
* @method: twitCurl::~twitCurl
*
* @description: destructor
*
* @input: none
*
* @output: none
*
*--*/
twitCurl::~twitCurl()
{
    /* Cleanup cURL */
    if( m_curlHandle )
    {
        curl_easy_cleanup( m_curlHandle );
        m_curlHandle = NULL;
    }
    if( m_errorBuffer )
    {
        free( m_errorBuffer );
        m_errorBuffer = NULL;
    }
}

/*++
* @method: twitCurl::clone
*
* @description: creates a clone of twitcurl object
*
* @input: none
*
* @output: cloned object
*
*--*/
twitCurl* twitCurl::clone()
{
    twitCurl *cloneObj = new twitCurl();

    /* cURL proxy data */
    cloneObj->setProxyServerIp(m_proxyServerIp);
    cloneObj->setProxyServerPort(m_proxyServerPort);
    cloneObj->setProxyUserName(m_proxyUserName);
    cloneObj->setProxyPassword(m_proxyPassword);

    /* Twitter data */
    cloneObj->setTwitterUsername(m_twitterUsername);
    cloneObj->setTwitterPassword(m_twitterPassword);

    /* OAuth data */
    cloneObj->m_oAuth = m_oAuth.clone();

    return cloneObj;
}

/*++
* @method: twitCurl::isCurlInit
*
* @description: method to check if cURL is initialized properly
*
* @input: none
*
* @output: true if cURL is intialized, otherwise false
*
*--*/
bool twitCurl::isCurlInit()
{
    return ( NULL != m_curlHandle ) ? true : false;
}

/*++
* @method: twitCurl::getTwitterUsername
*
* @description: method to get stored Twitter username
*
* @input: none
*
* @output: twitter username
*
*--*/
std::string& twitCurl::getTwitterUsername()
{
    return m_twitterUsername;
}

/*++
* @method: twitCurl::getTwitterPassword
*
* @description: method to get stored Twitter password
*
* @input: none
*
* @output: twitter password
*
*--*/
std::string& twitCurl::getTwitterPassword()
{
    return m_twitterPassword;
}

/*++
* @method: twitCurl::setTwitterUsername
*
* @description: method to set username
*
* @input: userName
*
* @output: none
*
*--*/
void twitCurl::setTwitterUsername( const std::string& userName )
{
    if( userName.length() )
    {
        m_twitterUsername = userName;
        m_curlLoginParamsSet = false;
    }
}

/*++
* @method: twitCurl::setTwitterPassword
*
* @description: method to set password
*
* @input: passWord
*
* @output: none
*
*--*/
void twitCurl::setTwitterPassword( const std::string& passWord )
{
    if( passWord.length() )
    {
        m_twitterPassword = passWord;
        m_curlLoginParamsSet = false;
    }
}

/*++
* @method: twitCurl::getProxyServerIp
*
* @description: method to get proxy server IP address
*
* @input: none
*
* @output: proxy server IP address
*
*--*/
std::string& twitCurl::getProxyServerIp()
{
    return m_proxyServerIp;
}

/*++
* @method: twitCurl::getProxyServerPort
*
* @description: method to get proxy server port
*
* @input: none
*
* @output: proxy server port
*
*--*/
std::string& twitCurl::getProxyServerPort()
{
    return m_proxyServerPort;
}

/*++
* @method: twitCurl::getProxyUserName
*
* @description: method to get proxy user name
*
* @input: none
*
* @output: proxy server user name
*
*--*/
std::string& twitCurl::getProxyUserName()
{
    return m_proxyUserName;
}

/*++
* @method: twitCurl::getProxyPassword
*
* @description: method to get proxy server password
*
* @input: none
*
* @output: proxy server password
*
*--*/
std::string& twitCurl::getProxyPassword()
{
    return m_proxyPassword;
}

/*++
* @method: twitCurl::getInterface
*
* @description: method to get proxy server password
*
* @input: none
*
* @output: proxy server password
*
*--*/
std::string& twitCurl::getInterface()
{
    return m_Interface;
}


/*++
* @method: twitCurl::setProxyServerIp
*
* @description: method to set proxy server IP address
*
* @input: proxyServerIp
*
* @output: none
*
*--*/
void twitCurl::setProxyServerIp( const std::string& proxyServerIp )
{
    if( proxyServerIp.length() )
    {
        m_proxyServerIp = proxyServerIp;
        /*
         * Reset the flag so that next cURL http request
         * would set proxy details again into cURL.
         */
        m_curlProxyParamsSet = false;
    }
}

/*++
* @method: twitCurl::setProxyServerPort
*
* @description: method to set proxy server port
*
* @input: proxyServerPort
*
* @output: none
*
*--*/
void twitCurl::setProxyServerPort( const std::string& proxyServerPort )
{
    if( proxyServerPort.length() )
    {
        m_proxyServerPort = proxyServerPort;
        /*
         * Reset the flag so that next cURL http request
         * would set proxy details again into cURL.
         */
        m_curlProxyParamsSet = false;
    }
}

/*++
* @method: twitCurl::setProxyUserName
*
* @description: method to set proxy server username
*
* @input: proxyUserName
*
* @output: none
*
*--*/
void twitCurl::setProxyUserName( const std::string& proxyUserName )
{
    if( proxyUserName.length() )
    {
        m_proxyUserName = proxyUserName;
        /*
         * Reset the flag so that next cURL http request
         * would set proxy details again into cURL.
         */
        m_curlProxyParamsSet = false;
    }
}

/*++
* @method: twitCurl::setProxyPassword
*
* @description: method to set proxy server password
*
* @input: proxyPassword
*
* @output: none
*
*--*/
void twitCurl::setProxyPassword( const std::string& proxyPassword )
{
    if( proxyPassword.length() )
    {
        m_proxyPassword = proxyPassword;
        /*
         * Reset the flag so that next cURL http request
         * would set proxy details again into cURL.
         */
        m_curlProxyParamsSet = false;
    }
}

/*++
* @method: twitCurl::setInterface
*
* @description: method to set proxy server IP address
*
* @input: Interface
*
* @output: none
*
*--*/
void twitCurl::setInterface( const std::string& Interface )
{
    if( Interface.length() )
    {
        m_Interface = Interface;
        /*
         * Reset the flag so that next cURL http request
         * would set proxy details again into cURL.
         */
        m_curlProxyParamsSet = false;
    }
}
/*++
* @method: twitCurl::search
*
* @description: method to return tweets that match a specified query.
*
* @input: searchQuery - search query in string format
*         resultCount - optional search result count
*
* @output: true if GET is success, otherwise false. This does not check http
*          response by twitter. Use getLastWebResponse() for that.
*
* @note: Only ATOM and JSON format supported.
*
*--*/
bool twitCurl::search( const std::string& searchQuery, const std::string resultCount )
{
    /* Prepare URL */
    std::string buildUrl = twitCurlDefaults::TWITCURL_PROTOCOLS[m_eProtocolType] +
                           twitterDefaults::TWITCURL_SEARCH_URL +
                           twitCurlDefaults::TWITCURL_EXTENSIONFORMATS[m_eApiFormatType] +
                           twitCurlDefaults::TWITCURL_URL_SEP_QUES + twitCurlDefaults::TWITCURL_SEARCHQUERYSTRING +
                           searchQuery;

    /* Add number of results count if provided */
    if( resultCount.size() )
    {
        buildUrl += twitCurlDefaults::TWITCURL_URL_SEP_AMP +
                    twitCurlDefaults::TWITCURL_COUNT + urlencode( resultCount );
    }

    /* Perform GET */
    return performGet( buildUrl );
}

/*++
* @method: twitCurl::statusUpdate
*
* @description: method to update new status message in twitter profile
*
* @input: newStatus - status message text
*         inReplyToStatusId - optional status id to we're replying to
*
* @output: true if POST is success, otherwise false. This does not check http
*          response by twitter. Use getLastWebResponse() for that.
*
*--*/
bool twitCurl::statusUpdate( const std::string& newStatus, const std::string inReplyToStatusId )
{
    if( newStatus.empty() )
    {
        return false;
    }

    /* Prepare new status message */
    std::string newStatusMsg = twitCurlDefaults::TWITCURL_STATUSSTRING + urlencode( newStatus );

    /* Append status id to which we're replying to */
    if( inReplyToStatusId.size() )
    {
        newStatusMsg += twitCurlDefaults::TWITCURL_URL_SEP_AMP +
                        twitCurlDefaults::TWITCURL_INREPLYTOSTATUSID +
                        urlencode( inReplyToStatusId );
    }

    /* Perform POST */
    return  performPost( twitCurlDefaults::TWITCURL_PROTOCOLS[m_eProtocolType] +
                         twitterDefaults::TWITCURL_STATUSUPDATE_URL +
                         twitCurlDefaults::TWITCURL_EXTENSIONFORMATS[m_eApiFormatType],
                         newStatusMsg );
}

/*++
* @method: twitCurl::statusShowById
*
* @description: method to get a status message by its id
*
* @input: statusId - a number in std::string format
*
* @output: true if GET is success, otherwise false. This does not check http
*          response by twitter. Use getLastWebResponse() for that.
*
*--*/
bool twitCurl::statusShowById( const std::string& statusId )
{
    if( statusId.empty() )
    {
        return false;
    }

    /* Prepare URL */
    std::string buildUrl = twitCurlDefaults::TWITCURL_PROTOCOLS[m_eProtocolType] +
                           twitterDefaults::TWITCURL_STATUSSHOW_URL + statusId +
                           twitCurlDefaults::TWITCURL_EXTENSIONFORMATS[m_eApiFormatType];

    /* Perform GET */
    return performGet( buildUrl );
}

/*++
* @method: twitCurl::statusDestroyById
*
* @description: method to delete a status message by its id
*
* @input: statusId - a number in std::string format
*
* @output: true if DELETE is success, otherwise false. This does not check http
*          response by twitter. Use getLastWebResponse() for that.
*
*--*/
bool twitCurl::statusDestroyById( const std::string& statusId )
{
    if( statusId.empty() )
    {
        return false;
    }

    /* Prepare URL */
    std::string buildUrl = twitCurlDefaults::TWITCURL_PROTOCOLS[m_eProtocolType] +
                           twitterDefaults::TWITCURL_STATUDESTROY_URL + statusId +
                           twitCurlDefaults::TWITCURL_EXTENSIONFORMATS[m_eApiFormatType];

    /* Perform DELETE */
    return performDelete( buildUrl );
}

/*++
* @method: twitCurl::retweetById
*
* @description: method to RETWEET a status message by its id
*
* @input: statusId - a number in std::string format
*
* @output: true if RETWEET is success, otherwise false. This does not check http
*          response by twitter. Use getLastWebResponse() for that.
*
*--*/
bool twitCurl::retweetById( const std::string& statusId )
{
    if( statusId.empty() )
    {
        return false;
    }

    /* Prepare URL */
    std::string buildUrl = twitCurlDefaults::TWITCURL_PROTOCOLS[m_eProtocolType] +
                           twitterDefaults::TWITCURL_RETWEET_URL + statusId +
                           twitCurlDefaults::TWITCURL_EXTENSIONFORMATS[m_eApiFormatType];

    /* Send some dummy data in POST */
    std::string dummyData = twitCurlDefaults::TWITCURL_TEXTSTRING +
                            urlencode( std::string( "dummy" ) );

    /* Perform Retweet */
    return performPost( buildUrl, dummyData );
}

/*++
* @method: twitCurl::timelineHomeGet
*
* @description: method to get home timeline
*
* @input: none
*
* @output: true if GET is success, otherwise false. This does not check http
*          response by twitter. Use getLastWebResponse() for that.
*
*--*/
bool twitCurl::timelineHomeGet( const std::string sinceId )
{
    std::string buildUrl = twitCurlDefaults::TWITCURL_PROTOCOLS[m_eProtocolType] +
                           twitterDefaults::TWITCURL_HOME_TIMELINE_URL +
                           twitCurlDefaults::TWITCURL_EXTENSIONFORMATS[m_eApiFormatType];
    if( sinceId.length() )
    {
        buildUrl += twitCurlDefaults::TWITCURL_URL_SEP_QUES + twitCurlDefaults::TWITCURL_SINCEID + sinceId;
    }

    /* Perform GET */
    return performGet( buildUrl );
}

/*++
* @method: twitCurl::timelinePublicGet
*
* @description: method to get public timeline
*
* @input: none
*
* @output: true if GET is success, otherwise false. This does not check http
*          response by twitter. Use getLastWebResponse() for that.
*
*--*/
bool twitCurl::timelinePublicGet()
{
    /* Perform GET */
    return performGet( twitCurlDefaults::TWITCURL_PROTOCOLS[m_eProtocolType] +
                       twitterDefaults::TWITCURL_PUBLIC_TIMELINE_URL +
                       twitCurlDefaults::TWITCURL_EXTENSIONFORMATS[m_eApiFormatType] );
}

/*++
* @method: twitCurl::featuredUsersGet
*
* @description: method to get featured users
*
* @input: none
*
* @output: true if GET is success, otherwise false. This does not check http
*          response by twitter. Use getLastWebResponse() for that.
*
*--*/
bool twitCurl::featuredUsersGet()
{
    /* Perform GET */
    return performGet( twitCurlDefaults::TWITCURL_PROTOCOLS[m_eProtocolType] +
                       twitterDefaults::TWITCURL_FEATURED_USERS_URL +
                       twitCurlDefaults::TWITCURL_EXTENSIONFORMATS[m_eApiFormatType] );
}

/*++
* @method: twitCurl::timelineFriendsGet
*
* @description: method to get friends timeline
*
* @input: none
*
* @output: true if GET is success, otherwise false. This does not check http
*          response by twitter. Use getLastWebResponse() for that.
*
*--*/
bool twitCurl::timelineFriendsGet()
{
    /* Perform GET */
    return performGet( twitCurlDefaults::TWITCURL_PROTOCOLS[m_eProtocolType] +
                       twitterDefaults::TWITCURL_FRIENDS_TIMELINE_URL +
                       twitCurlDefaults::TWITCURL_EXTENSIONFORMATS[m_eApiFormatType] );
}

/*++
* @method: twitCurl::mentionsGet
*
* @description: method to get mentions
*
* @input: sinceId - String specifying since id parameter
*
* @output: true if GET is success, otherwise false. This does not check http
*          response by twitter. Use getLastWebResponse() for that.
*
*--*/
bool twitCurl::mentionsGet( const std::string sinceId )
{
    std::string buildUrl = twitCurlDefaults::TWITCURL_PROTOCOLS[m_eProtocolType] +
                           twitterDefaults::TWITCURL_MENTIONS_URL +
                           twitCurlDefaults::TWITCURL_EXTENSIONFORMATS[m_eApiFormatType];
    if( sinceId.length() )
    {
        buildUrl += twitCurlDefaults::TWITCURL_URL_SEP_QUES + twitCurlDefaults::TWITCURL_SINCEID + sinceId;
    }

    /* Perform GET */
    return performGet( buildUrl );
}

/*++
* @method: twitCurl::timelineUserGet
*
* @description: method to get mentions
*
* @input: trimUser - Trim user name if true
*         tweetCount - Number of tweets to get. Max 200.
*         userInfo - screen name or user id in string format,
*         isUserId - true if userInfo contains an id
*
* @output: true if GET is success, otherwise false. This does not check http
*          response by twitter. Use getLastWebResponse() for that.
*
*--*/
bool twitCurl::timelineUserGet( const bool trimUser,
                                const bool includeRetweets,
                                const unsigned int tweetCount,
                                const std::string userInfo,
                                const bool isUserId )
{
    /* Prepare URL */
    std::string buildUrl;

    utilMakeUrlForUser( buildUrl, twitCurlDefaults::TWITCURL_PROTOCOLS[m_eProtocolType] +
                        twitterDefaults::TWITCURL_USERTIMELINE_URL +
                        twitCurlDefaults::TWITCURL_EXTENSIONFORMATS[m_eApiFormatType],
                        userInfo, isUserId );

    if( userInfo.empty() )
    {
        buildUrl += twitCurlDefaults::TWITCURL_URL_SEP_QUES;
    }

    if( tweetCount )
    {
		std::stringstream tmpStrm;
        if( tweetCount < twitCurlDefaults::MAX_TIMELINE_TWEET_COUNT )
        {
            tmpStrm << twitCurlDefaults::TWITCURL_URL_SEP_AMP + twitCurlDefaults::TWITCURL_COUNT << tweetCount;
        }
        else
        {
            tmpStrm << twitCurlDefaults::TWITCURL_URL_SEP_AMP + twitCurlDefaults::TWITCURL_COUNT << twitCurlDefaults::MAX_TIMELINE_TWEET_COUNT;
        }
        buildUrl += tmpStrm.str();
        tmpStrm.str().clear();
    }

    if( includeRetweets )
    {
        buildUrl += twitCurlDefaults::TWITCURL_URL_SEP_AMP + twitCurlDefaults::TWITCURL_INCRETWEETS;
    }

    if( trimUser )
    {
        buildUrl += twitCurlDefaults::TWITCURL_URL_SEP_AMP + twitCurlDefaults::TWITCURL_TRIMUSER;
    }

    /* Perform GET */
    return performGet( buildUrl );
}

/*++
* @method: twitCurl::userLookup
*
* @description: method to get a number of user's profiles
*
* @input: userInfo - vector of screen names or user ids
*         isUserId - true if userInfo contains an id
*
* @output: true if POST is success, otherwise false. This does not check http
*          response by twitter. Use getLastWebResponse() for that.
*
*--*/
bool twitCurl::userLookup( const std::vector<std::string> &userInfo, const bool isUserId )
{
    if( userInfo.empty() )
    {
        return false;
    }

    std::string userIds = "";
    std::string sep = "";
    for( unsigned int i = 0 ; i < std::min((size_t)100, userInfo.size()); i++, sep = "," )
    {
        userIds += sep + userInfo[i];
    }

    userIds = ( isUserId ? twitCurlDefaults::TWITCURL_USERID : twitCurlDefaults::TWITCURL_SCREENNAME ) +
              urlencode( userIds );

    std::string buildUrl = twitCurlDefaults::TWITCURL_PROTOCOLS[m_eProtocolType] + 
                           twitterDefaults::TWITCURL_LOOKUPUSERS_URL + 
                           twitCurlDefaults::TWITCURL_EXTENSIONFORMATS[m_eApiFormatType];

    /* Perform POST */
    return performPost( buildUrl, userIds);
}

/*++
* @method: twitCurl::userGet
*
* @description: method to get a user's profile
*
* @input: userInfo - screen name or user id in string format,
*         isUserId - true if userInfo contains an id
*
* @output: true if GET is success, otherwise false. This does not check http
*          response by twitter. Use getLastWebResponse() for that.
*
*--*/
bool twitCurl::userGet( const std::string& userInfo, const bool isUserId )
{
    if( userInfo.empty() )
    {
        return false;
    }

    /* Set URL */
    std::string buildUrl;
    utilMakeUrlForUser( buildUrl, twitCurlDefaults::TWITCURL_PROTOCOLS[m_eProtocolType] +
                        twitterDefaults::TWITCURL_SHOWUSERS_URL +
                        twitCurlDefaults::TWITCURL_EXTENSIONFORMATS[m_eApiFormatType],
                        userInfo, isUserId );

    /* Perform GET */
    return performGet( buildUrl );
}

/*++
* @method: twitCurl::friendsGet
*
* @description: method to get a user's friends
*
* @input: userInfo - screen name or user id in string format,
*         isUserId - true if userInfo contains an id
*
* @output: true if GET is success, otherwise false. This does not check http
*          response by twitter. Use getLastWebResponse() for that.
*
*--*/
bool twitCurl::friendsGet( const std::string userInfo, const bool isUserId )
{
    /* Set URL */
    std::string buildUrl;
    utilMakeUrlForUser( buildUrl, twitCurlDefaults::TWITCURL_PROTOCOLS[m_eProtocolType] +
                        twitterDefaults::TWITCURL_SHOWFRIENDS_URL +
                        twitCurlDefaults::TWITCURL_EXTENSIONFORMATS[m_eApiFormatType],
                        userInfo, isUserId );

    /* Perform GET */
    return performGet( buildUrl );
}

/*++
* @method: twitCurl::followersGet
*
* @description: method to get a user's followers
*
* @input: userInfo - screen name or user id in string format,
*         isUserId - true if userInfo contains an id
*
* @output: true if GET is success, otherwise false. This does not check http
*          response by twitter. Use getLastWebResponse() for that.
*
*--*/
bool twitCurl::followersGet( const std::string userInfo, const bool isUserId )
{
    /* Prepare URL */
    std::string buildUrl;
    utilMakeUrlForUser( buildUrl, twitCurlDefaults::TWITCURL_PROTOCOLS[m_eProtocolType] +
                        twitterDefaults::TWITCURL_SHOWFOLLOWERS_URL +
                        twitCurlDefaults::TWITCURL_EXTENSIONFORMATS[m_eApiFormatType],
                        userInfo, isUserId );

    /* Perform GET */
    return performGet( buildUrl );
}

/*++
* @method: twitCurl::directMessageGet
*
* @description: method to get direct messages
*
* @input: since id
*
* @output: true if GET is success, otherwise false. This does not check http
*          response by twitter. Use getLastWebResponse() for that.
*
*--*/
bool twitCurl::directMessageGet( const std::string sinceId )
{
    std::string buildUrl = twitCurlDefaults::TWITCURL_PROTOCOLS[m_eProtocolType] +
                           twitterDefaults::TWITCURL_DIRECTMESSAGES_URL +
                           twitCurlDefaults::TWITCURL_EXTENSIONFORMATS[m_eApiFormatType];

    if( sinceId.length() )
    {
        buildUrl += twitCurlDefaults::TWITCURL_URL_SEP_QUES + twitCurlDefaults::TWITCURL_SINCEID + sinceId;
    }

    /* Perform GET */
    return performGet( buildUrl );
}

/*++
* @method: twitCurl::directMessageSend
*
* @description: method to send direct message to a user
*
* @input: userInfo - screen name or user id of a user to whom message needs to be sent,
*         dMsg - message
*         isUserId - true if userInfo contains target user's id
*
* @output: true if POST is success, otherwise false. This does not check http
*          response by twitter. Use getLastWebResponse() for that.
*
*--*/
bool twitCurl::directMessageSend( const std::string& userInfo, const std::string& dMsg, const bool isUserId )
{
    if( userInfo.empty() || dMsg.empty() )
    {
        return false;
    }

    /* Prepare new direct message */
    std::string newDm = twitCurlDefaults::TWITCURL_TEXTSTRING + urlencode( dMsg );

    /* Prepare URL */
    std::string buildUrl;
    utilMakeUrlForUser( buildUrl, twitCurlDefaults::TWITCURL_PROTOCOLS[m_eProtocolType] +
                        twitterDefaults::TWITCURL_DIRECTMESSAGENEW_URL +
                        twitCurlDefaults::TWITCURL_EXTENSIONFORMATS[m_eApiFormatType],
                        userInfo, isUserId );

    /* Perform POST */
    return performPost( buildUrl, newDm );
}

/*++
* @method: twitCurl::directMessageGetSent
*
* @description: method to get sent direct messages
*
* @input: none
*
* @output: true if GET is success, otherwise false. This does not check http
*          response by twitter. Use getLastWebResponse() for that.
*
*--*/
bool twitCurl::directMessageGetSent()
{
    /* Perform GET */
    return performGet( twitCurlDefaults::TWITCURL_PROTOCOLS[m_eProtocolType] +
                       twitterDefaults::TWITCURL_DIRECTMESSAGESSENT_URL +
                       twitCurlDefaults::TWITCURL_EXTENSIONFORMATS[m_eApiFormatType] );
}

/*++
* @method: twitCurl::directMessageDestroyById
*
* @description: method to delete direct messages by its id
*
* @input: dMsgId - id of direct message in string format
*
* @output: true if DELETE is success, otherwise false. This does not check http
*          response by twitter. Use getLastWebResponse() for that.
*
*--*/
bool twitCurl::directMessageDestroyById( const std::string& dMsgId )
{
    if( dMsgId.empty() )
    {
        return false;
    }

    /* Prepare URL */
    std::string buildUrl = twitCurlDefaults::TWITCURL_PROTOCOLS[m_eProtocolType] +
                           twitterDefaults::TWITCURL_DIRECTMESSAGEDESTROY_URL + dMsgId +
                           twitCurlDefaults::TWITCURL_EXTENSIONFORMATS[m_eApiFormatType];

    /* Perform DELETE */
    return performDelete( buildUrl );
}

/*++
* @method: twitCurl::friendshipCreate
*
* @description: method to add a twitter user as friend (follow a user)
*
* @input: userInfo - user id or screen name of a user
*         isUserId - true if userInfo contains a user id instead of screen name
*
* @output: true if POST is success, otherwise false. This does not check http
*          response by twitter. Use getLastWebResponse() for that.
*
*--*/
bool twitCurl::friendshipCreate( const std::string& userInfo, const bool isUserId )
{
    if( userInfo.empty() )
    {
        return false;
    }

    /* Prepare URL */
    std::string buildUrl;
    utilMakeUrlForUser( buildUrl, twitCurlDefaults::TWITCURL_PROTOCOLS[m_eProtocolType] +
                        twitterDefaults::TWITCURL_FRIENDSHIPSCREATE_URL +
                        twitCurlDefaults::TWITCURL_EXTENSIONFORMATS[m_eApiFormatType],
                        userInfo, isUserId );

    /* Send some dummy data in POST */
    std::string dummyData = twitCurlDefaults::TWITCURL_TEXTSTRING +
                            urlencode( std::string( "dummy" ) );

    /* Perform POST */
    return performPost( buildUrl, dummyData );
}

/*++
* @method: twitCurl::friendshipDestroy
*
* @description: method to delete a twitter user from friend list (unfollow a user)
*
* @input: userInfo - user id or screen name of a user
*         isUserId - true if userInfo contains a user id instead of screen name
*
* @output: true if DELETE is success, otherwise false. This does not check http
*          response by twitter. Use getLastWebResponse() for that.
*
*--*/
bool twitCurl::friendshipDestroy( const std::string& userInfo, const bool isUserId )
{
    if( userInfo.empty() )
    {
        return false;
    }

    /* Prepare URL */
    std::string buildUrl;
    utilMakeUrlForUser( buildUrl, twitCurlDefaults::TWITCURL_PROTOCOLS[m_eProtocolType] +
                        twitterDefaults::TWITCURL_FRIENDSHIPSDESTROY_URL +
                        twitCurlDefaults::TWITCURL_EXTENSIONFORMATS[m_eApiFormatType],
                        userInfo, isUserId );

    /* Perform DELETE */
    return performDelete( buildUrl );
}

/*++
* @method: twitCurl::friendshipShow
*
* @description: method to show all friends
*
* @input: userInfo - user id or screen name of a user of whom friends need to be shown
*         isUserId - true if userInfo contains a user id instead of screen name
*
* @output: true if GET is success, otherwise false. This does not check http
*          response by twitter. Use getLastWebResponse() for that.
*
*--*/
bool twitCurl::friendshipShow( const std::string& userInfo, const bool isUserId )
{
    /* Prepare URL */
    std::string buildUrl = twitCurlDefaults::TWITCURL_PROTOCOLS[m_eProtocolType] +
                           twitterDefaults::TWITCURL_FRIENDSHIPSSHOW_URL +
                           twitCurlDefaults::TWITCURL_EXTENSIONFORMATS[m_eApiFormatType];
    if( userInfo.length() )
    {
        /* Append username to the URL */
        buildUrl += twitCurlDefaults::TWITCURL_URL_SEP_QUES;
        if( isUserId )
        {
            buildUrl += twitCurlDefaults::TWITCURL_TARGETUSERID;
        }
        else
        {
            buildUrl += twitCurlDefaults::TWITCURL_TARGETSCREENNAME;
        }
        buildUrl += userInfo;
    }

    /* Perform GET */
    return performGet( buildUrl );
}

/*++
* @method: twitCurl::friendsIdsGet
*
* @description: method to show IDs of all friends of a twitter user
*
* @input: userInfo - user id or screen name of a user
*         isUserId - true if userInfo contains a user id instead of screen name
*         nextCursor - next cursor string returned from a previous call
*                      to this API, otherwise an empty string
*
* @output: true if GET is success, otherwise false. This does not check http
*          response by twitter. Use getLastWebResponse() for that.
*
*--*/
bool twitCurl::friendsIdsGet( const std::string& nextCursor, const std::string& userInfo, const bool isUserId )
{
    /* Prepare URL */
    std::string buildUrl;
    utilMakeUrlForUser( buildUrl, twitCurlDefaults::TWITCURL_PROTOCOLS[m_eProtocolType] +
                        twitterDefaults::TWITCURL_FRIENDSIDS_URL +
                        twitCurlDefaults::TWITCURL_EXTENSIONFORMATS[m_eApiFormatType],
                        userInfo, isUserId );

    if( buildUrl.length() && nextCursor.length() )
    {
        buildUrl += twitCurlDefaults::TWITCURL_URL_SEP_AMP +
                    twitCurlDefaults::TWITCURL_NEXT_CURSOR +
                    nextCursor;
    }

    /* Perform GET */
    return performGet( buildUrl );
}

/*++
* @method: twitCurl::followersIdsGet
*
* @description: method to show IDs of all followers of a twitter user
*
* @input: userInfo - user id or screen name of a user
*         isUserId - true if userInfo contains a user id instead of screen name
*         nextCursor - next cursor string returned from a previous call
*                      to this API, otherwise an empty string
*
* @output: true if GET is success, otherwise false. This does not check http
*          response by twitter. Use getLastWebResponse() for that.
*
*--*/
bool twitCurl::followersIdsGet( const std::string& nextCursor, const std::string& userInfo, const bool isUserId )
{
    /* Prepare URL */
    std::string buildUrl;
    utilMakeUrlForUser( buildUrl, twitCurlDefaults::TWITCURL_PROTOCOLS[m_eProtocolType] +
                        twitterDefaults::TWITCURL_FOLLOWERSIDS_URL +
                        twitCurlDefaults::TWITCURL_EXTENSIONFORMATS[m_eApiFormatType],
                        userInfo, isUserId );

    if( buildUrl.length() && nextCursor.length() )
    {
        buildUrl += twitCurlDefaults::TWITCURL_URL_SEP_AMP +
                    twitCurlDefaults::TWITCURL_NEXT_CURSOR +
                    nextCursor;
    }

    /* Perform GET */
    return performGet( buildUrl );
}

/*++
* @method: twitCurl::accountRateLimitGet
*
* @description: method to get API rate limit of current user
*
* @input: none
*
* @output: true if GET is success, otherwise false. This does not check http
*          response by twitter. Use getLastWebResponse() for that.
*
*--*/
bool twitCurl::accountRateLimitGet()
{
    /* Perform GET */
    return performGet( twitCurlDefaults::TWITCURL_PROTOCOLS[m_eProtocolType] +
                       twitterDefaults::TWITCURL_ACCOUNTRATELIMIT_URL +
                       twitCurlDefaults::TWITCURL_EXTENSIONFORMATS[m_eApiFormatType] );
}

/*++
* @method: twitCurl::accountVerifyCredGet
*
* @description: method to get information on user identified by given credentials
*
* @input: none
*
* @output: true if GET is success, otherwise false. This does not check http
*          response by twitter. Use getLastWebResponse() for that.
*
*--*/
bool twitCurl::accountVerifyCredGet()
{
    /* Perform GET */
    return performGet( twitCurlDefaults::TWITCURL_PROTOCOLS[m_eProtocolType] +
                       twitterDefaults::TWITCURL_ACCOUNTVERIFYCRED_URL +
                       twitCurlDefaults::TWITCURL_EXTENSIONFORMATS[m_eApiFormatType] );
}

/*++
* @method: twitCurl::favoriteGet
*
* @description: method to get favorite users' statuses
*
* @input: none
*
* @output: true if GET is success, otherwise false. This does not check http
*          response by twitter. Use getLastWebResponse() for that.
*
*--*/
bool twitCurl::favoriteGet()
{
    /* Perform GET */
    return performGet( twitCurlDefaults::TWITCURL_PROTOCOLS[m_eProtocolType] +
                       twitterDefaults::TWITCURL_FAVORITESGET_URL +
                       twitCurlDefaults::TWITCURL_EXTENSIONFORMATS[m_eApiFormatType] );
}

/*++
* @method: twitCurl::favoriteCreate
*
* @description: method to favorite a status message
*
* @input: statusId - id in string format of the status to be favorited
*
* @output: true if POST is success, otherwise false. This does not check http
*          response by twitter. Use getLastWebResponse() for that.
*
*--*/
bool twitCurl::favoriteCreate( const std::string& statusId )
{
    /* Prepare URL */
    std::string buildUrl = twitCurlDefaults::TWITCURL_PROTOCOLS[m_eProtocolType] +
                           twitterDefaults::TWITCURL_FAVORITECREATE_URL + statusId +
                           twitCurlDefaults::TWITCURL_EXTENSIONFORMATS[m_eApiFormatType];

    /* Send some dummy data in POST */
    std::string dummyData = twitCurlDefaults::TWITCURL_TEXTSTRING +
                            urlencode( std::string( "dummy" ) );

    /* Perform POST */
    return performPost( buildUrl, dummyData );
}

/*++
* @method: twitCurl::favoriteDestroy
*
* @description: method to delete a favorited the status
*
* @input: statusId - id in string format of the favorite status to be deleted
*
* @output: true if DELETE is success, otherwise false. This does not check http
*          response by twitter. Use getLastWebResponse() for that.
*
*--*/
bool twitCurl::favoriteDestroy( const std::string& statusId )
{
    /* Prepare URL */
    std::string buildUrl = twitCurlDefaults::TWITCURL_PROTOCOLS[m_eProtocolType] +
                           twitterDefaults::TWITCURL_FAVORITEDESTROY_URL + statusId +
                           twitCurlDefaults::TWITCURL_EXTENSIONFORMATS[m_eApiFormatType];

    /* Perform DELETE */
    return performDelete( buildUrl );
}

/*++
* @method: twitCurl::blockCreate
*
* @description: method to block a user
*
* @input: userInfo - user id or screen name who needs to be blocked
*
* @output: true if POST is success, otherwise false. This does not check http
*          response by twitter. Use getLastWebResponse() for that.
*
*--*/
bool twitCurl::blockCreate( const std::string& userInfo )
{
        /* Prepare URL */
        std::string buildUrl = twitCurlDefaults::TWITCURL_PROTOCOLS[m_eProtocolType] +
                               twitterDefaults::TWITCURL_BLOCKSCREATE_URL + userInfo +
                               twitCurlDefaults::TWITCURL_EXTENSIONFORMATS[m_eApiFormatType];

        /* Send some dummy data in POST */
        std::string dummyData = twitCurlDefaults::TWITCURL_TEXTSTRING +
                                urlencode( std::string( "dummy" ) );

        /* Perform POST */
        return performPost( buildUrl, dummyData );
}

/*++
* @method: twitCurl::blockDestroy
*
* @description: method to unblock a user
*
* @input: userInfo - user id or screen name who need to unblocked
*
* @output: true if DELETE is success, otherwise false. This does not check http
*          response by twitter. Use getLastWebResponse() for that.
*
*--*/
bool twitCurl::blockDestroy( const std::string& userInfo )
{
    /* Prepare URL */
    std::string buildUrl = twitCurlDefaults::TWITCURL_PROTOCOLS[m_eProtocolType] +
                           twitterDefaults::TWITCURL_BLOCKSDESTROY_URL + userInfo +
                           twitCurlDefaults::TWITCURL_EXTENSIONFORMATS[m_eApiFormatType];

    /* Perform DELETE */
    return performDelete( buildUrl );
}

/*++
* @method: twitCurl::blockListGet
*
* @description: method to get list of users blocked by authenticated user
*
* @input: includeEntities - indicates whether or not to include 'entities' node
*         skipStatus - indicates whether or not to include status for returned users
*         nextCursor - next cursor string returned from a previous call
*                      to this API, otherwise an empty string
*
* @output: true if GET is success, otherwise false. This does not check http
*          response by twitter. Use getLastWebResponse() for that.
*
*--*/
bool twitCurl::blockListGet( const std::string& nextCursor, const bool includeEntities, const bool skipStatus )
{
    /* Prepare URL */
    std::string buildUrl, urlParams;

    buildUrl = twitCurlDefaults::TWITCURL_PROTOCOLS[m_eProtocolType] +
               twitterDefaults::TWITCURL_BLOCKSLIST_URL +
               twitCurlDefaults::TWITCURL_EXTENSIONFORMATS[m_eApiFormatType];
    if( includeEntities )
    {
        urlParams += twitCurlDefaults::TWITCURL_INCLUDE_ENTITIES + std::string("true");
    }
    if( skipStatus )
    {
        if( urlParams.length() )
        {
            urlParams += twitCurlDefaults::TWITCURL_URL_SEP_AMP;
        }
        urlParams += twitCurlDefaults::TWITCURL_SKIP_STATUS + std::string("true");
    }
    if( nextCursor.length() )
    {
        if( urlParams.length() )
        {
            urlParams += twitCurlDefaults::TWITCURL_URL_SEP_AMP;
        }
        urlParams += twitCurlDefaults::TWITCURL_NEXT_CURSOR + nextCursor;
    }
    if( urlParams.length() )
    {
        buildUrl += twitCurlDefaults::TWITCURL_URL_SEP_QUES + urlParams;
    }

    /* Perform GET */
    return performGet( buildUrl );
}

/*++
* @method: twitCurl::blockIdsGet
*
* @description: method to get list of IDs blocked by authenticated user
*
* @input: stringifyIds - indicates whether or not returned ids should
*                        be in string format
*         nextCursor - next cursor string returned from a previous call
*                      to this API, otherwise an empty string
*
* @output: true if GET is success, otherwise false. This does not check http
*          response by twitter. Use getLastWebResponse() for that.
*
*--*/
bool twitCurl::blockIdsGet( const std::string& nextCursor, const bool stringifyIds )
{
    /* Prepare URL */
    std::string buildUrl, urlParams;

    buildUrl = twitCurlDefaults::TWITCURL_PROTOCOLS[m_eProtocolType] +
               twitterDefaults::TWITCURL_BLOCKSIDS_URL +
               twitCurlDefaults::TWITCURL_EXTENSIONFORMATS[m_eApiFormatType];
    if( stringifyIds )
    {
        urlParams += twitCurlDefaults::TWITCURL_STRINGIFY_IDS + std::string("true");
    }
    if( nextCursor.length() )
    {
        if( urlParams.length() )
        {
            urlParams += twitCurlDefaults::TWITCURL_URL_SEP_AMP;
        }
        urlParams += twitCurlDefaults::TWITCURL_NEXT_CURSOR + nextCursor;
    }
    if( urlParams.length() )
    {
        buildUrl += twitCurlDefaults::TWITCURL_URL_SEP_QUES + urlParams;
    }

    /* Perform GET */
    return performGet( buildUrl );
}

/*++
* @method: twitCurl::savedSearchGet
*
* @description: gets authenticated user's saved search queries.
*
* @input: none
*
* @output: true if GET is success, otherwise false. This does not check http
*          response by twitter. Use getLastWebResponse() for that.
*
*--*/
bool twitCurl::savedSearchGet( )
{
    /* Perform GET */
    return performGet( twitCurlDefaults::TWITCURL_PROTOCOLS[m_eProtocolType] +
                       twitterDefaults::TWITCURL_SAVEDSEARCHGET_URL +
                       twitCurlDefaults::TWITCURL_EXTENSIONFORMATS[m_eApiFormatType] );
}

/*++
* @method: twitCurl::savedSearchShow
*
* @description: method to retrieve the data for a saved search owned by the authenticating user
*               specified by the given id.
*
* @input: searchId - id in string format of the search to be displayed
*
* @output: true if GET is success, otherwise false. This does not check http
*          response by twitter. Use getLastWebResponse() for that.
*
*--*/
bool twitCurl::savedSearchShow( const std::string& searchId )
{
    /* Prepare URL */
    std::string buildUrl = twitCurlDefaults::TWITCURL_PROTOCOLS[m_eProtocolType] +
                           twitterDefaults::TWITCURL_SAVEDSEARCHSHOW_URL + searchId +
                           twitCurlDefaults::TWITCURL_EXTENSIONFORMATS[m_eApiFormatType];

    /* Perform GET */
    return performGet( buildUrl );
}

/*++
* @method: twitCurl::savedSearchCreate
*
* @description: creates a saved search for the authenticated user
*
* @input: query - the query of the search the user would like to save
*
* @output: true if POST is success, otherwise false. This does not check http
*          response by twitter. Use getLastWebResponse() for that.
*
*--*/
bool twitCurl::savedSearchCreate( const std::string& query )
{
    /* Prepare URL */
    std::string buildUrl = twitCurlDefaults::TWITCURL_PROTOCOLS[m_eProtocolType] +
                           twitterDefaults::TWITCURL_SAVEDSEARCHCREATE_URL +
                           twitCurlDefaults::TWITCURL_EXTENSIONFORMATS[m_eApiFormatType];

    /* Send some dummy data in POST */
    std::string queryStr = twitCurlDefaults::TWITCURL_QUERYSTRING + urlencode( query );

    /* Perform POST */
    return performPost( buildUrl, queryStr );
}


/*++
* @method: twitCurl::savedSearchDestroy
*
* @description: method to destroy a saved search for the authenticated user. The search specified
*               by id must be owned by the authenticating user.
*
* @input: searchId - search id of item to be deleted
*
* @output: true if DELETE is success, otherwise false. This does not check http
*          response by twitter. Use getLastWebResponse() for that.
*
*--*/
bool twitCurl::savedSearchDestroy( const std::string& searchId )
{
    /* Prepare URL */
    std::string buildUrl = twitCurlDefaults::TWITCURL_PROTOCOLS[m_eProtocolType] +
                           twitterDefaults::TWITCURL_SAVEDSEARCHDESTROY_URL + searchId +
                           twitCurlDefaults::TWITCURL_EXTENSIONFORMATS[m_eApiFormatType];

    /* Perform DELETE */
    return performDelete( buildUrl );
}


/*++
* @method: twitCurl::trendsGet()
*
* @description: gets trends.
*
* @input: none
*
* @output: true if GET is success, otherwise false. This does not check http
*          response by twitter. Use getLastWebResponse() for that.
*
*--*/
bool twitCurl::trendsGet()
{
    /* Perform GET */
    return performGet( twitCurlDefaults::TWITCURL_PROTOCOLS[m_eProtocolType] +
                       twitterDefaults::TWITCURL_TRENDS_URL +
                       twitCurlDefaults::TWITCURL_EXTENSIONFORMATS[m_eApiFormatType] );
}


/*++
* @method: twitCurl::trendsDailyGet()
*
* @description: gets daily trends.
*
* @input: none
*
* @output: true if GET is success, otherwise false. This does not check http
*          response by twitter. Use getLastWebResponse() for that.
*
*--*/
bool twitCurl::trendsDailyGet()
{
    /* Perform GET */
    return performGet( twitCurlDefaults::TWITCURL_PROTOCOLS[m_eProtocolType] +
                       twitterDefaults::TWITCURL_TRENDSDAILY_URL +
                       twitCurlDefaults::TWITCURL_EXTENSIONFORMATS[m_eApiFormatType] );
}

/*++
* @method: twitCurl::trendsWeeklyGet()
*
* @description: gets weekly trends.
*
* @input: none
*
* @output: true if GET is success, otherwise false. This does not check http
*          response by twitter. Use getLastWebResponse() for that.
*
*--*/
bool twitCurl::trendsWeeklyGet()
{
    /* Perform GET */
    return performGet( twitCurlDefaults::TWITCURL_PROTOCOLS[m_eProtocolType] +
                       twitterDefaults::TWITCURL_TRENDSWEEKLY_URL +
                       twitCurlDefaults::TWITCURL_EXTENSIONFORMATS[m_eApiFormatType] );
}

/*++
* @method: twitCurl::trendsCurrentGet()
*
* @description: gets current trends.
*
* @input: none
*
* @output: true if GET is success, otherwise false. This does not check http
*          response by twitter. Use getLastWebResponse() for that.
*
*--*/
bool twitCurl::trendsCurrentGet()
{
    /* Perform GET */
    return performGet( twitCurlDefaults::TWITCURL_PROTOCOLS[m_eProtocolType] +
                       twitterDefaults::TWITCURL_TRENDSCURRENT_URL +
                       twitCurlDefaults::TWITCURL_EXTENSIONFORMATS[m_eApiFormatType] );
}

/*++
* @method: twitCurl::trendsAvailableGet()
*
* @description: gets available trends.
*
* @input: none
*
* @output: true if GET is success, otherwise false. This does not check http
*          response by twitter. Use getLastWebResponse() for that.
*
*--*/
bool twitCurl::trendsAvailableGet()
{
    /* Perform GET */
    return performGet( twitCurlDefaults::TWITCURL_PROTOCOLS[m_eProtocolType] +
                       twitterDefaults::TWITCURL_TRENDSAVAILABLE_URL +
                       twitCurlDefaults::TWITCURL_EXTENSIONFORMATS[m_eApiFormatType] );
}

/*++
* @method: twitCurl::getLastWebResponse
*
* @description: method to get http response for the most recent request sent.
*               twitcurl users need to call this method and parse the XML
*               data returned by twitter to see what has happened.
*
* @input: outWebResp - string in which twitter's response is supplied back to caller
*
* @output: none
*
*--*/
void twitCurl::getLastWebResponse( std::string& outWebResp )
{
    outWebResp = "";
    if( m_callbackData.length() )
    {
        outWebResp = m_callbackData;
    }
}

/*++
* @method: twitCurl::getLastCurlError
*
* @description: method to get cURL error response for most recent http request.
*               twitcurl users can call this method if any of the APIs return
*               false.
*
* @input: none
*
* @output: none
*
*--*/
void twitCurl::getLastCurlError( std::string& outErrResp )
{
    m_errorBuffer[twitCurlDefaults::TWITCURL_DEFAULT_BUFFSIZE-1] = twitCurlDefaults::TWITCURL_EOS;
    outErrResp.assign( m_errorBuffer );
}

/*++
* @method: twitCurl::curlCallback
*
* @description: static method to get http response back from cURL.
*               this is an internal method, users of twitcurl need not
*               use this.
*
* @input: as per cURL convention.
*
* @output: size of data stored in our buffer
*
* @remarks: internal method
*
*--*/
int twitCurl::curlCallback( char* data, size_t size, size_t nmemb, twitCurl* pTwitCurlObj )
{
    if( pTwitCurlObj && data )
    {
        /* Save http response in twitcurl object's buffer */
        return pTwitCurlObj->saveLastWebResponse( data, ( size*nmemb ) );
    }
    return 0;
}

/*++
* @method: twitCurl::saveLastWebResponse
*
* @description: method to save http responses. this is an internal method
*               and twitcurl users need not use this.
*
* @input: data - character buffer from cURL,
*         size - size of character buffer
*
* @output: size of data stored in our buffer
*
* @remarks: internal method
*
*--*/
int twitCurl::saveLastWebResponse(  char*& data, size_t size )
{
    if( data && size )
    {
        /* Append data in our internal buffer */
        m_callbackData.append( data, size );
        return (int)size;
    }
    return 0;
}

/*++
* @method: twitCurl::clearCurlCallbackBuffers
*
* @description: method to clear callback buffers used by cURL. this is an
*               internal method and twitcurl users need not use this.
*
* @input: none
*
* @output: none
*
* @remarks: internal method
*
*--*/
void twitCurl::clearCurlCallbackBuffers()
{
    m_callbackData = "";
    memset( m_errorBuffer, 0, twitCurlDefaults::TWITCURL_DEFAULT_BUFFSIZE );
}

/*++
* @method: twitCurl::prepareCurlProxy
*
* @description: method to set proxy details into cURL. this is an internal method.
*               twitcurl users should not use this method, instead use setProxyXxx
*               methods to set proxy server information.
*
* @input: none
*
* @output: none
*
* @remarks: internal method
*
*--*/
void twitCurl::prepareCurlProxy()
{
    if( m_curlProxyParamsSet )
    {
        return;
    }

    /* Reset existing proxy details in cURL */
    curl_easy_setopt( m_curlHandle, CURLOPT_PROXY, NULL );
    curl_easy_setopt( m_curlHandle, CURLOPT_PROXYUSERPWD, NULL );
    curl_easy_setopt( m_curlHandle, CURLOPT_PROXYAUTH, (long)CURLAUTH_ANY );

    /* Set proxy details in cURL */
    std::string proxyIpPort("");
    if( getProxyServerIp().size() )
    {
        utilMakeCurlParams( proxyIpPort, getProxyServerIp(), getProxyServerPort() );
    }
    curl_easy_setopt( m_curlHandle, CURLOPT_PROXY, proxyIpPort.c_str() );

    /* Prepare username and password for proxy server */
    if( m_proxyUserName.length() && m_proxyPassword.length() )
    {
        std::string proxyUserPass;
        utilMakeCurlParams( proxyUserPass, getProxyUserName(), getProxyPassword() );
        curl_easy_setopt( m_curlHandle, CURLOPT_PROXYUSERPWD, proxyUserPass.c_str() );
    }

    /* Set the flag to true indicating that proxy info is set in cURL */
    m_curlProxyParamsSet = true;
}

/*++
* @method: twitCurl::prepareCurlProxy
*
* @description: method to set proxy details into cURL. this is an internal method.
*               twitcurl users should not use this method, instead use setProxyXxx
*               methods to set proxy server information.
*
* @input: none
*
* @output: none
*
* @remarks: internal method
*
*--*/
void twitCurl::prepareCurlInterface()
{
    if( m_curlInterfaseParamSet )
    {
        return;
    }

    /* Reset existing interface details in cURL */
    curl_easy_setopt( m_curlHandle, CURLOPT_INTERFACE, NULL );
    
    if( m_Interface.length() ){
        /* Set interface details in cURL */
        curl_easy_setopt( m_curlHandle, CURLOPT_INTERFACE, m_Interface.c_str() );
    }
    /* Set the flag to true indicating that proxy info is set in cURL */
    m_curlInterfaseParamSet = true;
}

/*++
* @method: twitCurl::prepareCurlCallback
*
* @description: method to set callback details into cURL. this is an internal method.
*               twitcurl users should not use this method.
*
* @input: none
*
* @output: none
*
* @remarks: internal method
*
*--*/
void twitCurl::prepareCurlCallback()
{
    if( m_curlCallbackParamsSet )
    {
        return;
    }

    /* Set buffer to get error */
    curl_easy_setopt( m_curlHandle, CURLOPT_ERRORBUFFER, m_errorBuffer );

    /* Set callback function to get response */
    curl_easy_setopt( m_curlHandle, CURLOPT_WRITEFUNCTION, curlCallback );
    curl_easy_setopt( m_curlHandle, CURLOPT_WRITEDATA, this );

    /* Set the flag to true indicating that callback info is set in cURL */
    m_curlCallbackParamsSet = true;
}

/*++
* @method: twitCurl::prepareCurlUserPass
*
* @description: method to set twitter credentials into cURL. this is an internal method.
*               twitcurl users should not use this method, instead use setTwitterXxx
*               methods to set twitter username and password.
*
* @input: none
*
* @output: none
*
* @remarks: internal method
*
*--*/
void twitCurl::prepareCurlUserPass()
{
    if( m_curlLoginParamsSet )
    {
        return;
    }

    /* Reset existing username and password stored in cURL */
    curl_easy_setopt( m_curlHandle, CURLOPT_USERPWD, "" );

    if( getTwitterUsername().size() )
    {
        /* Prepare username:password */
        std::string userNamePassword;
        utilMakeCurlParams( userNamePassword, getTwitterUsername(), getTwitterPassword() );

        /* Set username and password */
        curl_easy_setopt( m_curlHandle, CURLOPT_USERPWD, userNamePassword.c_str() );
    }

    /* Set the flag to true indicating that twitter credentials are set in cURL */
    m_curlLoginParamsSet = true;
}

/*++
* @method: twitCurl::prepareStandardParams
*
* @description: method to set standard params into cURL. this is an internal method.
*               twitcurl users should not use this method.
*
* @input: none
*
* @output: none
*
* @remarks: internal method
*
*--*/
void twitCurl::prepareStandardParams()
{
    /* Restore any custom request we may have */
    curl_easy_setopt( m_curlHandle, CURLOPT_CUSTOMREQUEST, NULL );

    /* All supported encodings */
    curl_easy_setopt( m_curlHandle, CURLOPT_ENCODING, "" );

    /* Clear callback and error buffers */
    clearCurlCallbackBuffers();

    /* Prepare interface */
    prepareCurlInterface();

    /* Prepare proxy */
    prepareCurlProxy();

    /* Prepare cURL callback data and error buffer */
    prepareCurlCallback();

    /* Prepare username and password for twitter */
    prepareCurlUserPass();
}

/*++
* @method: twitCurl::performGet
*
* @description: method to send http GET request. this is an internal method.
*               twitcurl users should not use this method.
*
* @input: getUrl - url
*
* @output: none
*
* @remarks: internal method
*
*--*/
bool twitCurl::performGet( const std::string& getUrl )
{
    /* Return if cURL is not initialized */
    if( !isCurlInit() )
    {
        return false;
    }

    std::string dataStrDummy;
    std::string oAuthHttpHeader;
    struct curl_slist* pOAuthHeaderList = NULL;

    /* Prepare standard params */
    prepareStandardParams();

    /* Set OAuth header */
    m_oAuth.getOAuthHeader( eOAuthHttpGet, getUrl, dataStrDummy, oAuthHttpHeader );
    if( oAuthHttpHeader.length() )
    {
        pOAuthHeaderList = curl_slist_append( pOAuthHeaderList, oAuthHttpHeader.c_str() );
        if( pOAuthHeaderList )
        {
            curl_easy_setopt( m_curlHandle, CURLOPT_HTTPHEADER, pOAuthHeaderList );
        }
    }

    /* Set http request and url */
    curl_easy_setopt( m_curlHandle, CURLOPT_HTTPGET, 1 );
    curl_easy_setopt( m_curlHandle, CURLOPT_URL, getUrl.c_str() );

    /* Send http request */
    if( CURLE_OK == curl_easy_perform( m_curlHandle ) )
    {
        if( pOAuthHeaderList )
        {
            curl_slist_free_all( pOAuthHeaderList );
        }
        return true;
    }
    if( pOAuthHeaderList )
    {
        curl_slist_free_all( pOAuthHeaderList );
    }
    return false;
}

/*++
* @method: twitCurl::performGetInternal
*
* @description: method to send http GET request. this is an internal method.
*               twitcurl users should not use this method.
*
* @input: const std::string& getUrl, const std::string& oAuthHttpHeader
*
* @output: none
*
* @remarks: internal method
*
*--*/
bool twitCurl::performGetInternal( const std::string& getUrl,
                                   const std::string& oAuthHttpHeader )
{
    /* Return if cURL is not initialized */
    if( !isCurlInit() )
    {
        return false;
    }

    struct curl_slist* pOAuthHeaderList = NULL;

    /* Prepare standard params */
    prepareStandardParams();

    /* Set http request and url */
    curl_easy_setopt( m_curlHandle, CURLOPT_HTTPGET, 1 );
    curl_easy_setopt( m_curlHandle, CURLOPT_URL, getUrl.c_str() );

    /* Set header */
    if( oAuthHttpHeader.length() )
    {
        pOAuthHeaderList = curl_slist_append( pOAuthHeaderList, oAuthHttpHeader.c_str() );
        if( pOAuthHeaderList )
        {
            curl_easy_setopt( m_curlHandle, CURLOPT_HTTPHEADER, pOAuthHeaderList );
        }
    }

    /* Send http request */
    if( CURLE_OK == curl_easy_perform( m_curlHandle ) )
    {
        if( pOAuthHeaderList )
        {
            curl_slist_free_all( pOAuthHeaderList );
        }
        return true;
    }
    if( pOAuthHeaderList )
    {
        curl_slist_free_all( pOAuthHeaderList );
    }
    return false;
}

/*++
* @method: twitCurl::performDelete
*
* @description: method to send http DELETE request. this is an internal method.
*               twitcurl users should not use this method.
*
* @input: deleteUrl - url
*
* @output: none
*
* @remarks: internal method
*
*--*/
bool twitCurl::performDelete( const std::string& deleteUrl )
{
    /* Return if cURL is not initialized */
    if( !isCurlInit() )
    {
        return false;
    }

    std::string dataStrDummy;
    std::string oAuthHttpHeader;
    struct curl_slist* pOAuthHeaderList = NULL;

    /* Prepare standard params */
    prepareStandardParams();

    /* Set OAuth header */
    m_oAuth.getOAuthHeader( eOAuthHttpDelete, deleteUrl, dataStrDummy, oAuthHttpHeader );
    if( oAuthHttpHeader.length() )
    {
        pOAuthHeaderList = curl_slist_append( pOAuthHeaderList, oAuthHttpHeader.c_str() );
        if( pOAuthHeaderList )
        {
            curl_easy_setopt( m_curlHandle, CURLOPT_HTTPHEADER, pOAuthHeaderList );
        }
    }

    /* Set http request and url */
    curl_easy_setopt( m_curlHandle, CURLOPT_CUSTOMREQUEST, "DELETE" );
    curl_easy_setopt( m_curlHandle, CURLOPT_URL, deleteUrl.c_str() );
    curl_easy_setopt( m_curlHandle, CURLOPT_COPYPOSTFIELDS, dataStrDummy.c_str() );

    /* Send http request */
    if( CURLE_OK == curl_easy_perform( m_curlHandle ) )
    {
        if( pOAuthHeaderList )
        {
            curl_slist_free_all( pOAuthHeaderList );
        }
        return true;
    }
    if( pOAuthHeaderList )
    {
        curl_slist_free_all( pOAuthHeaderList );
    }
    return false;
}

/*++
* @method: twitCurl::performPost
*
* @description: method to send http POST request. this is an internal method.
*               twitcurl users should not use this method.
*
* @input: postUrl - url,
*         dataStr - url encoded data to be posted
*
* @output: none
*
* @remarks: internal method
*           data value in dataStr must already be url encoded.
*           ex: dataStr = "key=urlencode(value)"
*
*--*/
bool twitCurl::performPost( const std::string& postUrl, std::string dataStr )
{
    /* Return if cURL is not initialized */
    if( !isCurlInit() )
    {
        return false;
    }

    std::string oAuthHttpHeader;
    struct curl_slist* pOAuthHeaderList = NULL;

    /* Prepare standard params */
    prepareStandardParams();

    /* Set OAuth header */
    m_oAuth.getOAuthHeader( eOAuthHttpPost, postUrl, dataStr, oAuthHttpHeader );
    if( oAuthHttpHeader.length() )
    {
        pOAuthHeaderList = curl_slist_append( pOAuthHeaderList, oAuthHttpHeader.c_str() );
        if( pOAuthHeaderList )
        {
            curl_easy_setopt( m_curlHandle, CURLOPT_HTTPHEADER, pOAuthHeaderList );
        }
    }

    /* Set http request, url and data */
    curl_easy_setopt( m_curlHandle, CURLOPT_POST, 1 );
    curl_easy_setopt( m_curlHandle, CURLOPT_URL, postUrl.c_str() );
    if( dataStr.length() )
    {
        curl_easy_setopt( m_curlHandle, CURLOPT_COPYPOSTFIELDS, dataStr.c_str() );
    }

    /* Send http request */
    if( CURLE_OK == curl_easy_perform( m_curlHandle ) )
    {
        if( pOAuthHeaderList )
        {
            curl_slist_free_all( pOAuthHeaderList );
        }
        return true;
    }
    if( pOAuthHeaderList )
    {
        curl_slist_free_all( pOAuthHeaderList );
    }
    return false;
}

/*++
* @method: utilMakeCurlParams
*
* @description: utility function to build parameter strings in the format
*               required by cURL ("param1:param2"). twitcurl users should
*               not use this function.
*
* @input: inParam1 - first parameter,
*         inParam2 - second parameter
*
* @output: outStr - built parameter
*
* @remarks: internal method
*
*--*/
void utilMakeCurlParams( std::string& outStr, const std::string& inParam1, const std::string& inParam2 )
{
    outStr = inParam1;
    outStr += twitCurlDefaults::TWITCURL_COLON + inParam2;
}

/*++
* @method: utilMakeUrlForUser
*
* @description: utility function to build url compatible to twitter. twitcurl
*               users should not use this function.
*
* @input: baseUrl - base twitter url,
*         userInfo - user name,
*         isUserId - indicates if userInfo contains a user id or scree name
*
* @output: outUrl - built url
*
* @remarks: internal method
*
*--*/
void utilMakeUrlForUser( std::string& outUrl, const std::string& baseUrl, const std::string& userInfo, const bool isUserId )
{
    /* Copy base URL */
    outUrl = baseUrl;

    if( userInfo.length() )
    {
        /* Append username to the URL */
        outUrl += twitCurlDefaults::TWITCURL_URL_SEP_QUES;
        if( isUserId )
        {
            outUrl += twitCurlDefaults::TWITCURL_USERID;
        }
        else
        {
            outUrl += twitCurlDefaults::TWITCURL_SCREENNAME;
        }
        outUrl += userInfo;
    }
}

/*++
* @method: twitCurl::getOAuth
*
* @description: method to get a reference to oAuth object.
*
* @input: none
*
* @output: reference to oAuth object
*
*--*/
oAuth& twitCurl::getOAuth()
{
    return m_oAuth;
}

/*++
* @method: twitCurl::oAuthRequestToken
*
* @description: method to get a request token key and secret. this token
*               will be used to get authorize user and get PIN from twitter
*
* @input: authorizeUrl is an output parameter. this method will set the url
*         in this string. user should visit this link and get PIN from that page.
*
* @output: true if everything went sucessfully, otherwise false
*
*--*/
bool twitCurl::oAuthRequestToken( std::string& authorizeUrl /* out */ )
{
    /* Return if cURL is not initialized */
    if( !isCurlInit() )
    {
        return false;
    }

    /* Get OAuth header for request token */
    std::string oAuthHeader;
    authorizeUrl = "";
    if( m_oAuth.getOAuthHeader( eOAuthHttpGet,
                                twitCurlDefaults::TWITCURL_PROTOCOLS[m_eProtocolType] +
                                oAuthTwitterApiUrls::OAUTHLIB_TWITTER_REQUEST_TOKEN_URL,
                                std::string( "" ),
                                oAuthHeader ) )
    {
        if( performGetInternal( twitCurlDefaults::TWITCURL_PROTOCOLS[m_eProtocolType] +
                                oAuthTwitterApiUrls::OAUTHLIB_TWITTER_REQUEST_TOKEN_URL,
                                oAuthHeader ) )
        {
            /* Tell OAuth object to save access token and secret from web response */
            std::string twitterResp;
            getLastWebResponse( twitterResp );
            m_oAuth.extractOAuthTokenKeySecret( twitterResp );

            /* Get access token and secret from OAuth object */
            std::string oAuthTokenKey;
            m_oAuth.getOAuthTokenKey( oAuthTokenKey );

            /* Build authorize url so that user can visit in browser and get PIN */
            authorizeUrl.assign(twitCurlDefaults::TWITCURL_PROTOCOLS[m_eProtocolType] +
                                oAuthTwitterApiUrls::OAUTHLIB_TWITTER_AUTHORIZE_URL );
            authorizeUrl.append( oAuthTokenKey.c_str() );

            return true;
        }
    }
    return false;
}

/*++
* @method: twitCurl::oAuthAccessToken
*
* @description: method to exchange request token with access token
*
* @input: none
*
* @output: true if everything went sucessfully, otherwise false
*
*--*/
bool twitCurl::oAuthAccessToken()
{
    /* Return if cURL is not initialized */
    if( !isCurlInit() )
    {
        return false;
    }
    /* Get OAuth header for access token */
    std::string oAuthHeader;
    if( m_oAuth.getOAuthHeader( eOAuthHttpGet,
                                twitCurlDefaults::TWITCURL_PROTOCOLS[m_eProtocolType] +
                                oAuthTwitterApiUrls::OAUTHLIB_TWITTER_ACCESS_TOKEN_URL,
                                std::string( "" ),
                                oAuthHeader, true ) )
    {
        if( performGetInternal( twitCurlDefaults::TWITCURL_PROTOCOLS[m_eProtocolType] +
                                oAuthTwitterApiUrls::OAUTHLIB_TWITTER_ACCESS_TOKEN_URL,
                                oAuthHeader ) )
        {
            /* Tell OAuth object to save access token and secret from web response */
            std::string twitterResp;
            getLastWebResponse( twitterResp );
            m_oAuth.extractOAuthTokenKeySecret( twitterResp );

            return true;
        }
    }
    return false;
}

/*++
* ADDED BY ANTIROOT
*
* @method: twitCurl::oAuthHandlePIN
*
* @description: method to handle user's PIN code from the authentiation URLs
*
* @input: none
*
* @output: true if everything went sucessfully, otherwise false
*
*--*/
bool twitCurl::oAuthHandlePIN( const std::string& authorizeUrl /* in */ )
{
    /* Return if cURL is not initialized */
    if( !isCurlInit() )
    {
        return false;
    }

    std::string dataStr;
    std::string oAuthHttpHeader;
    std::string authenticityTokenVal;
    std::string oauthTokenVal;
    std::string pinCodeVal;
    unsigned long httpStatusCode = 0;
    size_t nPosStart, nPosEnd;
    struct curl_slist* pOAuthHeaderList = NULL;
    struct curl_slist* pCookieList = NULL;

    /* Prepare standard params */
    prepareStandardParams();

    /* Set OAuth header */
    m_oAuth.getOAuthHeader( eOAuthHttpGet, authorizeUrl, dataStr, oAuthHttpHeader );
    if( oAuthHttpHeader.length() )
    {
        pOAuthHeaderList = curl_slist_append( pOAuthHeaderList, oAuthHttpHeader.c_str() );
        if( pOAuthHeaderList )
        {
            curl_easy_setopt( m_curlHandle, CURLOPT_HTTPHEADER, pOAuthHeaderList );
        }
    }

    /* Set http request and url */
    curl_easy_setopt( m_curlHandle, CURLOPT_HTTPGET, 1 );
    curl_easy_setopt( m_curlHandle, CURLOPT_URL, authorizeUrl.c_str() );
    curl_easy_setopt( m_curlHandle, CURLOPT_COOKIELIST, &pCookieList);

    /* Send http request */
    if( CURLE_OK == curl_easy_perform( m_curlHandle ) )
    {
        if( pOAuthHeaderList )
        {
            curl_easy_getinfo( m_curlHandle, CURLINFO_HTTP_CODE, &httpStatusCode );
            curl_slist_free_all( pOAuthHeaderList );

            // Now, let's find the authenticity token and oauth token
            nPosStart = m_callbackData.find( oAuthLibDefaults::OAUTHLIB_AUTHENTICITY_TOKEN_TWITTER_RESP_KEY );
            if( std::string::npos == nPosStart )
            {
                return false;
            }
            nPosStart += oAuthLibDefaults::OAUTHLIB_AUTHENTICITY_TOKEN_TWITTER_RESP_KEY.length();
            nPosEnd = m_callbackData.substr( nPosStart ).find( oAuthLibDefaults::OAUTHLIB_TOKEN_END_TAG_TWITTER_RESP );
            if( std::string::npos == nPosEnd )
            {
                return false;
            }
            authenticityTokenVal = m_callbackData.substr( nPosStart, nPosEnd );

            nPosStart = m_callbackData.find( oAuthLibDefaults::OAUTHLIB_TOKEN_TWITTER_RESP_KEY );
            if( std::string::npos == nPosStart )
            {
                return false;
            }
            nPosStart += oAuthLibDefaults::OAUTHLIB_TOKEN_TWITTER_RESP_KEY.length();
            nPosEnd = m_callbackData.substr( nPosStart ).find( oAuthLibDefaults::OAUTHLIB_TOKEN_END_TAG_TWITTER_RESP );
            if( std::string::npos == nPosEnd )
            {
                return false;
            }
            oauthTokenVal = m_callbackData.substr( nPosStart, nPosEnd );
            curl_easy_getinfo( m_curlHandle, CURLINFO_COOKIELIST, &pCookieList );
        }
    }
    else if( pOAuthHeaderList )
    {
        curl_slist_free_all( pOAuthHeaderList );
        return false;
    }

    // Second phase for the authorization
    pOAuthHeaderList = NULL;
    oAuthHttpHeader.clear();

    /* Prepare standard params */
    prepareStandardParams();

    /*
    Now, we need to make a data string for POST operation
    which includes oauth token, authenticity token, username, password.
    */
    dataStr = oAuthLibDefaults::OAUTHLIB_TOKEN_KEY + "=" + oauthTokenVal + "&" +                      \
              oAuthLibDefaults::OAUTHLIB_AUTHENTICITY_TOKEN_KEY + "=" + authenticityTokenVal + "&" +  \
              oAuthLibDefaults::OAUTHLIB_SESSIONUSERNAME_KEY + "=" + getTwitterUsername() + "&" +     \
              oAuthLibDefaults::OAUTHLIB_SESSIONPASSWORD_KEY + "=" + getTwitterPassword();

    /* Set OAuth header */
    m_oAuth.getOAuthHeader( eOAuthHttpPost, authorizeUrl, dataStr, oAuthHttpHeader );
    if( oAuthHttpHeader.length() )
    {
        pOAuthHeaderList = curl_slist_append( pOAuthHeaderList, oAuthHttpHeader.c_str() );
        if( pOAuthHeaderList )
        {
            curl_easy_setopt( m_curlHandle, CURLOPT_HTTPHEADER, pOAuthHeaderList );
        }
    }

    /* Set http request and url */
    curl_easy_setopt( m_curlHandle, CURLOPT_POST, 1 );
    curl_easy_setopt( m_curlHandle, CURLOPT_URL, authorizeUrl.c_str() );
    curl_easy_setopt( m_curlHandle, CURLOPT_COPYPOSTFIELDS, dataStr.c_str() );
    curl_easy_setopt( m_curlHandle, CURLOPT_COOKIELIST, &pCookieList);

    /* Send http request */
    if( CURLE_OK == curl_easy_perform( m_curlHandle ) )
    {
        if( pOAuthHeaderList )
        {
            curl_easy_getinfo( m_curlHandle, CURLINFO_HTTP_CODE, &httpStatusCode );
            curl_slist_free_all( pOAuthHeaderList );

            // Now, let's find the PIN CODE
            nPosStart = m_callbackData.find( oAuthLibDefaults::OAUTHLIB_PIN_TWITTER_RESP_KEY );
            if( std::string::npos == nPosStart )
            {
                return false;
            }
            nPosStart += oAuthLibDefaults::OAUTHLIB_PIN_TWITTER_RESP_KEY.length();
            nPosEnd = m_callbackData.substr( nPosStart ).find( oAuthLibDefaults::OAUTHLIB_PIN_END_TAG_TWITTER_RESP );
            if( std::string::npos == nPosEnd )
            {
                return false;
            }
            pinCodeVal = m_callbackData.substr( nPosStart, nPosEnd );
            getOAuth().setOAuthPin( pinCodeVal );
            return true;
        }
    }
    else if( pOAuthHeaderList )
    {
        curl_slist_free_all( pOAuthHeaderList );
    }
    if( pCookieList )
    {
        curl_slist_free_all( pCookieList );
    }
    return false;
}

