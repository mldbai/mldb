#ifndef __OAUTHLIB_H__
#define __OAUTHLIB_H__

#include "time.h"
#include <cstdlib>
#include <sstream>
#include <iostream>
#include <fstream>
#include <string>
#include <list>
#include <map>

typedef enum _eOAuthHttpRequestType
{
    eOAuthHttpInvalid = 0,
    eOAuthHttpGet,
    eOAuthHttpPost,
    eOAuthHttpDelete
} eOAuthHttpRequestType;

typedef std::list<std::string> oAuthKeyValueList;
typedef std::map<std::string, std::string> oAuthKeyValuePairs;

class oAuth
{
public:
    oAuth();
    ~oAuth();

    /* OAuth public methods used by twitCurl */
    void getConsumerKey( std::string& consumerKey /* out */ );
    void setConsumerKey( const std::string& consumerKey /* in */ );

    void getConsumerSecret( std::string& consumerSecret /* out */ );
    void setConsumerSecret( const std::string& consumerSecret /* in */ );

    void getOAuthTokenKey( std::string& oAuthTokenKey /* out */ );
    void setOAuthTokenKey( const std::string& oAuthTokenKey /* in */ );

    void getOAuthTokenSecret( std::string& oAuthTokenSecret /* out */ );
    void setOAuthTokenSecret( const std::string& oAuthTokenSecret /* in */ );

    void getOAuthScreenName( std::string& oAuthScreenName /* out */ );
    void setOAuthScreenName( const std::string& oAuthScreenName /* in */ );

    void getOAuthPin( std::string& oAuthPin /* out */ );
    void setOAuthPin( const std::string& oAuthPin /* in */ );

    bool getOAuthHeader( const eOAuthHttpRequestType eType, /* in */
                         const std::string& rawUrl, /* in */
                         const std::string& rawData, /* in */
                         std::string& oAuthHttpHeader, /* out */
                         const bool includeOAuthVerifierPin = false /* in */ );

    bool extractOAuthTokenKeySecret( const std::string& requestTokenResponse /* in */ );

    oAuth clone();

private:

    /* OAuth data */
    std::string m_consumerKey;
    std::string m_consumerSecret;
    std::string m_oAuthTokenKey;
    std::string m_oAuthTokenSecret;
    std::string m_oAuthPin;
    std::string m_nonce;
    std::string m_timeStamp;
    std::string m_oAuthScreenName;

    /* OAuth twitter related utility methods */
    void buildOAuthRawDataKeyValPairs( const std::string& rawData, /* in */
                                       bool urlencodeData, /* in */
                                       oAuthKeyValuePairs& rawDataKeyValuePairs /* out */ );

    bool buildOAuthTokenKeyValuePairs( const bool includeOAuthVerifierPin, /* in */
                                       const std::string& oauthSignature, /* in */
                                       oAuthKeyValuePairs& keyValueMap /* out */,
                                       const bool generateTimestamp /* in */ );

    bool getStringFromOAuthKeyValuePairs( const oAuthKeyValuePairs& rawParamMap, /* in */
                                          std::string& rawParams, /* out */
                                          const std::string& paramsSeperator /* in */ );

    bool getSignature( const eOAuthHttpRequestType eType, /* in */
                       const std::string& rawUrl, /* in */
                       const oAuthKeyValuePairs& rawKeyValuePairs, /* in */
                       std::string& oAuthSignature /* out */ );

    void generateNonceTimeStamp();
};

#endif // __OAUTHLIB_H__
