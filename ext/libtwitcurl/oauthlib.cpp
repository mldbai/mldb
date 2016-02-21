#include "twitcurlurls.h"
#include "oauthlib.h"
#include "HMAC_SHA1.h"
#include "base64.h"
#include "urlencode.h"

/*++
* @method: oAuth::oAuth
*
* @description: constructor
*
* @input: none
*
* @output: none
*
*--*/
oAuth::oAuth()
{
}

/*++
* @method: oAuth::~oAuth
*
* @description: destructor
*
* @input: none
*
* @output: none
*
*--*/
oAuth::~oAuth()
{
}

/*++
* @method: oAuth::clone
*
* @description: creates a clone of oAuth object
*
* @input: none
*
* @output: cloned oAuth object
*
*--*/
oAuth oAuth::clone()
{
    oAuth cloneObj;
    cloneObj.m_consumerKey = m_consumerKey;
    cloneObj.m_consumerSecret = m_consumerSecret;
    cloneObj.m_oAuthTokenKey = m_oAuthTokenKey;
    cloneObj.m_oAuthTokenSecret = m_oAuthTokenSecret;
    cloneObj.m_oAuthPin = m_oAuthPin;
    cloneObj.m_nonce = m_nonce;
    cloneObj.m_timeStamp = m_timeStamp;
    cloneObj.m_oAuthScreenName =  m_oAuthScreenName;
    return cloneObj;
}


/*++
* @method: oAuth::getConsumerKey
*
* @description: this method gives consumer key that is being used currently
*
* @input: none
*
* @output: consumer key
*
*--*/
void oAuth::getConsumerKey( std::string& consumerKey )
{
    consumerKey = m_consumerKey;
}

/*++
* @method: oAuth::setConsumerKey
*
* @description: this method saves consumer key that should be used
*
* @input: consumer key
*
* @output: none
*
*--*/
void oAuth::setConsumerKey( const std::string& consumerKey )
{
    m_consumerKey.assign( consumerKey );
}

/*++
* @method: oAuth::getConsumerSecret
*
* @description: this method gives consumer secret that is being used currently
*
* @input: none
*
* @output: consumer secret
*
*--*/
void oAuth::getConsumerSecret( std::string& consumerSecret )
{
    consumerSecret = m_consumerSecret;
}

/*++
* @method: oAuth::setConsumerSecret
*
* @description: this method saves consumer secret that should be used
*
* @input: consumer secret
*
* @output: none
*
*--*/
void oAuth::setConsumerSecret( const std::string& consumerSecret )
{
    m_consumerSecret = consumerSecret;
}

/*++
* @method: oAuth::getOAuthTokenKey
*
* @description: this method gives OAuth token (also called access token) that is being used currently
*
* @input: none
*
* @output: OAuth token
*
*--*/
void oAuth::getOAuthTokenKey( std::string& oAuthTokenKey )
{
    oAuthTokenKey = m_oAuthTokenKey;
}

/*++
* @method: oAuth::setOAuthTokenKey
*
* @description: this method saves OAuth token that should be used
*
* @input: OAuth token
*
* @output: none
*
*--*/
void oAuth::setOAuthTokenKey( const std::string& oAuthTokenKey )
{
    m_oAuthTokenKey = oAuthTokenKey;
}

/*++
* @method: oAuth::getOAuthTokenSecret
*
* @description: this method gives OAuth token secret that is being used currently
*
* @input: none
*
* @output: OAuth token secret
*
*--*/
void oAuth::getOAuthTokenSecret( std::string& oAuthTokenSecret )
{
    oAuthTokenSecret = m_oAuthTokenSecret;
}

/*++
* @method: oAuth::setOAuthTokenSecret
*
* @description: this method saves OAuth token that should be used
*
* @input: OAuth token secret
*
* @output: none
*
*--*/
void oAuth::setOAuthTokenSecret( const std::string& oAuthTokenSecret )
{
    m_oAuthTokenSecret = oAuthTokenSecret;
}

/*++
* @method: oAuth::getOAuthScreenName
*
* @description: this method gives authorized user's screenname
*
* @input: none
*
* @output: screen name
*
*--*/
void oAuth::getOAuthScreenName( std::string& oAuthScreenName )
{
    oAuthScreenName = m_oAuthScreenName;
}

/*++
* @method: oAuth::setOAuthScreenName
*
* @description: this method sets authorized user's screenname
*
* @input: screen name
*
* @output: none
*
*--*/
void oAuth::setOAuthScreenName( const std::string& oAuthScreenName )
{
    m_oAuthScreenName = oAuthScreenName;
}

/*++
* @method: oAuth::getOAuthPin
*
* @description: this method gives OAuth verifier PIN
*
* @input: none
*
* @output: OAuth verifier PIN
*
*--*/
void oAuth::getOAuthPin( std::string& oAuthPin )
{
    oAuthPin = m_oAuthPin;
}

/*++
* @method: oAuth::setOAuthPin
*
* @description: this method sets OAuth verifier PIN
*
* @input: OAuth verifier PIN
*
* @output: none
*
*--*/
void oAuth::setOAuthPin( const std::string& oAuthPin )
{
    m_oAuthPin = oAuthPin;
}

/*++
* @method: oAuth::generateNonceTimeStamp
*
* @description: this method generates nonce and timestamp for OAuth header
*
* @input: none
*
* @output: none
*
* @remarks: internal method
*
*--*/
void oAuth::generateNonceTimeStamp()
{
    char szTime[oAuthLibDefaults::OAUTHLIB_BUFFSIZE];
    char szRand[oAuthLibDefaults::OAUTHLIB_BUFFSIZE];
    memset( szTime, 0, oAuthLibDefaults::OAUTHLIB_BUFFSIZE );
    memset( szRand, 0, oAuthLibDefaults::OAUTHLIB_BUFFSIZE );
    srand( (unsigned int)time( NULL ) );
    sprintf( szRand, "%x", rand()%1000 );
    sprintf( szTime, "%ld", time( NULL ) );

    m_nonce.assign( szTime );
    m_nonce.append( szRand );
    m_timeStamp.assign( szTime );
}

/*++
* @method: oAuth::buildOAuthRawDataKeyValPairs
*
* @description: this method prepares key-value pairs from the data part of the URL
*               or from the URL post fields data, as required by OAuth header
*               and signature generation.
*
* @input: rawData - Raw data either from the URL itself or from post fields.
*                   Should already be url encoded.
*         urlencodeData - If true, string will be urlencoded before converting
*                         to key value pairs.
*
* @output: rawDataKeyValuePairs - Map in which key-value pairs are populated
*
* @remarks: internal method
*
*--*/
void oAuth::buildOAuthRawDataKeyValPairs( const std::string& rawData,
                                          bool urlencodeData,
                                          oAuthKeyValuePairs& rawDataKeyValuePairs )
{
    /* Raw data if it's present. Data should already be urlencoded once */
    if( rawData.empty() )
    {
        return;
    }

    size_t nSep = std::string::npos;
    size_t nPos = std::string::npos;
    std::string dataKeyVal;
    std::string dataKey;
    std::string dataVal;

    /* This raw data part can contain many key value pairs: key1=value1&key2=value2&key3=value3 */
    std::string dataPart = rawData;
    while( std::string::npos != ( nSep = dataPart.find_first_of("&") ) )
    {
        /* Extract first key=value pair */
        dataKeyVal = dataPart.substr( 0, nSep );

        /* Split them */
        nPos = dataKeyVal.find_first_of( "=" );
        if( std::string::npos != nPos )
        {
            dataKey = dataKeyVal.substr( 0, nPos );
            dataVal = dataKeyVal.substr( nPos + 1 );

            /* Put this key=value pair in map */
            rawDataKeyValuePairs[dataKey] = urlencodeData ? urlencode( dataVal ) : dataVal;
        }
        dataPart = dataPart.substr( nSep + 1 );
    }

    /* For the last key=value */
    dataKeyVal = dataPart.substr( 0, nSep );

    /* Split them */
    nPos = dataKeyVal.find_first_of( "=" );
    if( std::string::npos != nPos )
    {
        dataKey = dataKeyVal.substr( 0, nPos );
        dataVal = dataKeyVal.substr( nPos + 1 );

        /* Put this key=value pair in map */
        rawDataKeyValuePairs[dataKey] = urlencodeData ? urlencode( dataVal ) : dataVal;
    }
}

/*++
* @method: oAuth::buildOAuthTokenKeyValuePairs
*
* @description: this method prepares key-value pairs required for OAuth header
*               and signature generation.
*
* @input: includeOAuthVerifierPin - flag to indicate whether oauth_verifer key-value
*                                   pair needs to be included. oauth_verifer is only
*                                   used during exchanging request token with access token.
*         oauthSignature - base64 and url encoded OAuth signature.
*         generateTimestamp - If true, then generate new timestamp for nonce.
*
* @output: keyValueMap - map in which key-value pairs are populated
*
* @remarks: internal method
*
*--*/
bool oAuth::buildOAuthTokenKeyValuePairs( const bool includeOAuthVerifierPin,
                                          const std::string& oauthSignature,
                                          oAuthKeyValuePairs& keyValueMap,
                                          const bool generateTimestamp )
{
    /* Generate nonce and timestamp if required */
    if( generateTimestamp )
    {
        generateNonceTimeStamp();
    }

    /* Consumer key and its value */
    keyValueMap[oAuthLibDefaults::OAUTHLIB_CONSUMERKEY_KEY] = m_consumerKey;

    /* Nonce key and its value */
    keyValueMap[oAuthLibDefaults::OAUTHLIB_NONCE_KEY] = m_nonce;

    /* Signature if supplied */
    if( oauthSignature.length() )
    {
        keyValueMap[oAuthLibDefaults::OAUTHLIB_SIGNATURE_KEY] = oauthSignature;
    }

    /* Signature method, only HMAC-SHA1 as of now */
    keyValueMap[oAuthLibDefaults::OAUTHLIB_SIGNATUREMETHOD_KEY] = std::string( "HMAC-SHA1" );

    /* Timestamp */
    keyValueMap[oAuthLibDefaults::OAUTHLIB_TIMESTAMP_KEY] = m_timeStamp;

    /* Token */
    if( m_oAuthTokenKey.length() )
    {
        keyValueMap[oAuthLibDefaults::OAUTHLIB_TOKEN_KEY] = m_oAuthTokenKey;
    }

    /* Verifier */
    if( includeOAuthVerifierPin && m_oAuthPin.length() )
    {
        keyValueMap[oAuthLibDefaults::OAUTHLIB_VERIFIER_KEY] = m_oAuthPin;
    }

    /* Version */
    keyValueMap[oAuthLibDefaults::OAUTHLIB_VERSION_KEY] = std::string( "1.0" );

    return !keyValueMap.empty();
}

/*++
* @method: oAuth::getSignature
*
* @description: this method calculates HMAC-SHA1 signature of OAuth header
*
* @input: eType - HTTP request type
*         rawUrl - raw url of the HTTP request
*         rawKeyValuePairs - key-value pairs containing OAuth headers and HTTP data
*
* @output: oAuthSignature - base64 and url encoded signature
*
* @remarks: internal method
*
*--*/
bool oAuth::getSignature( const eOAuthHttpRequestType eType,
                          const std::string& rawUrl,
                          const oAuthKeyValuePairs& rawKeyValuePairs,
                          std::string& oAuthSignature )
{
    std::string rawParams;
    std::string paramsSeperator;
    std::string sigBase;

    /* Initially empty signature */
    oAuthSignature = "";

    /* Build a string using key-value pairs */
    paramsSeperator = "&";
    getStringFromOAuthKeyValuePairs( rawKeyValuePairs, rawParams, paramsSeperator );

    /* Start constructing base signature string. Refer http://dev.twitter.com/auth#intro */
    switch( eType )
    {
    case eOAuthHttpGet:
        {
            sigBase.assign( "GET&" );
        }
        break;

    case eOAuthHttpPost:
        {
            sigBase.assign( "POST&" );
        }
        break;

    case eOAuthHttpDelete:
        {
            sigBase.assign( "DELETE&" );
        }
        break;

    default:
        {
            return false;
        }
        break;
    }
    sigBase.append( urlencode( rawUrl ) );
    sigBase.append( "&" );
    sigBase.append( urlencode( rawParams ) );

    /* Now, hash the signature base string using HMAC_SHA1 class */
    CHMAC_SHA1 objHMACSHA1;
    std::string secretSigningKey;
    unsigned char strDigest[oAuthLibDefaults::OAUTHLIB_BUFFSIZE_LARGE];

    memset( strDigest, 0, oAuthLibDefaults::OAUTHLIB_BUFFSIZE_LARGE );

    /* Signing key is composed of consumer_secret&token_secret */
    secretSigningKey.assign( m_consumerSecret );
    secretSigningKey.append( "&" );
    if( m_oAuthTokenSecret.length() )
    {
        secretSigningKey.append( m_oAuthTokenSecret );
    }
  
    objHMACSHA1.HMAC_SHA1( (unsigned char*)sigBase.c_str(),
                           sigBase.length(),
                           (unsigned char*)secretSigningKey.c_str(),
                           secretSigningKey.length(),
                           strDigest ); 

    /* Do a base64 encode of signature */
    std::string base64Str = base64_encode( strDigest, 20 /* SHA 1 digest is 160 bits */ );

    /* Do an url encode */
    oAuthSignature = urlencode( base64Str );

    return !oAuthSignature.empty();
}

/*++
* @method: oAuth::getOAuthHeader
*
* @description: this method builds OAuth header that should be used in HTTP requests to twitter
*
* @input: eType - HTTP request type
*         rawUrl - raw url of the HTTP request
*         rawData - HTTP data (post fields)
*         includeOAuthVerifierPin - flag to indicate whether or not oauth_verifier needs to included
*                                   in OAuth header
*
* @output: oAuthHttpHeader - OAuth header
*
*--*/
bool oAuth::getOAuthHeader( const eOAuthHttpRequestType eType,
                            const std::string& rawUrl,
                            const std::string& rawData,
                            std::string& oAuthHttpHeader,
                            const bool includeOAuthVerifierPin )
{
    oAuthKeyValuePairs rawKeyValuePairs;
    std::string rawParams;
    std::string oauthSignature;
    std::string paramsSeperator;
    std::string pureUrl( rawUrl );

    /* Clear header string initially */
    oAuthHttpHeader = "";
    rawKeyValuePairs.clear();

    /* If URL itself contains ?key=value, then extract and put them in map */
    size_t nPos = rawUrl.find_first_of( "?" );
    if( std::string::npos != nPos )
    {
        /* Get only URL */
        pureUrl = rawUrl.substr( 0, nPos );

        /* Get only key=value data part */
        std::string dataPart = rawUrl.substr( nPos + 1 );

        /* Split the data in URL as key=value pairs */
        buildOAuthRawDataKeyValPairs( dataPart, true, rawKeyValuePairs );
    }

    /* Split the raw data if it's present, as key=value pairs. Data should already be urlencoded once */
    buildOAuthRawDataKeyValPairs( rawData, false, rawKeyValuePairs );

    /* Build key-value pairs needed for OAuth request token, without signature */
    buildOAuthTokenKeyValuePairs( includeOAuthVerifierPin, std::string( "" ), rawKeyValuePairs, true );

    /* Get url encoded base64 signature using request type, url and parameters */
    getSignature( eType, pureUrl, rawKeyValuePairs, oauthSignature );

    /* Clear map so that the parameters themselves are not sent along with the OAuth values */
    rawKeyValuePairs.clear();

    /* Now, again build key-value pairs with signature this time */
    buildOAuthTokenKeyValuePairs( includeOAuthVerifierPin, oauthSignature, rawKeyValuePairs, false );

    /* Get OAuth header in string format */
    paramsSeperator = ",";
    getStringFromOAuthKeyValuePairs( rawKeyValuePairs, rawParams, paramsSeperator );

    /* Build authorization header */
    oAuthHttpHeader.assign( oAuthLibDefaults::OAUTHLIB_AUTHHEADER_STRING );
    oAuthHttpHeader.append( rawParams );

    return !oAuthHttpHeader.empty();
}

/*++
* @method: oAuth::getStringFromOAuthKeyValuePairs
*
* @description: this method builds a sorted string from key-value pairs
*
* @input: rawParamMap - key-value pairs map
*         paramsSeperator - sepearator, either & or ,
*
* @output: rawParams - sorted string of OAuth parameters
*
* @remarks: internal method
*
*--*/
bool oAuth::getStringFromOAuthKeyValuePairs( const oAuthKeyValuePairs& rawParamMap,
                                             std::string& rawParams,
                                             const std::string& paramsSeperator )
{
    rawParams = "";
    if( rawParamMap.empty() )
    {
        return false;
    }

    oAuthKeyValueList keyValueList;
    std::string dummyStr;

    /* Push key-value pairs to a list of strings */
    keyValueList.clear();
    oAuthKeyValuePairs::const_iterator itMap = rawParamMap.begin();
    for( ; itMap != rawParamMap.end(); itMap++ )
    {
        dummyStr.assign( itMap->first );
        dummyStr.append( "=" );
        if( paramsSeperator == "," )
        {
            dummyStr.append( "\"" );
        }
        dummyStr.append( itMap->second );
        if( paramsSeperator == "," )
        {
            dummyStr.append( "\"" );
        }
        keyValueList.push_back( dummyStr );
    }

    /* Sort key-value pairs based on key name */
    keyValueList.sort();

    /* Now, form a string */
    dummyStr = "";
    oAuthKeyValueList::iterator itKeyValue = keyValueList.begin();
    for( ; itKeyValue != keyValueList.end(); itKeyValue++ )
    {
        if( dummyStr.length() )
        {
            dummyStr.append( paramsSeperator );
         }
         dummyStr.append( itKeyValue->c_str() );
    }
    rawParams = dummyStr;
    return !rawParams.empty();
}

/*++
* @method: oAuth::extractOAuthTokenKeySecret
*
* @description: this method extracts oauth token key and secret from
*               twitter's HTTP response
*
* @input: requestTokenResponse - response from twitter
*
* @output: none
*
*--*/
bool oAuth::extractOAuthTokenKeySecret( const std::string& requestTokenResponse )
{
    if( requestTokenResponse.empty() )
    {
        return false;
    }

    size_t nPos = std::string::npos;
    std::string strDummy;

    /* Get oauth_token key */
    nPos = requestTokenResponse.find( oAuthLibDefaults::OAUTHLIB_TOKEN_KEY );
    if( std::string::npos != nPos )
    {
        nPos = nPos + oAuthLibDefaults::OAUTHLIB_TOKEN_KEY.length() + strlen( "=" );
        strDummy = requestTokenResponse.substr( nPos );
        nPos = strDummy.find( "&" );
        if( std::string::npos != nPos )
        {
            m_oAuthTokenKey = strDummy.substr( 0, nPos );
        }
    }

    /* Get oauth_token_secret */
    nPos = requestTokenResponse.find( oAuthLibDefaults::OAUTHLIB_TOKENSECRET_KEY );
    if( std::string::npos != nPos )
    {
        nPos = nPos + oAuthLibDefaults::OAUTHLIB_TOKENSECRET_KEY.length() + strlen( "=" );
        strDummy = requestTokenResponse.substr( nPos );
        nPos = strDummy.find( "&" );
        if( std::string::npos != nPos )
        {
            m_oAuthTokenSecret = strDummy.substr( 0, nPos );
        }
    }

    /* Get screen_name */
    nPos = requestTokenResponse.find( oAuthLibDefaults::OAUTHLIB_SCREENNAME_KEY );
    if( std::string::npos != nPos )
    {
        nPos = nPos + oAuthLibDefaults::OAUTHLIB_SCREENNAME_KEY.length() + strlen( "=" );
        strDummy = requestTokenResponse.substr( nPos );
        m_oAuthScreenName = strDummy;
    }

    return true;
}

