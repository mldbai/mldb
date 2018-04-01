// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/* aws_test.cc
   Jeremy Barnes, 12 May 2013
   Copyright (c) 2013 mldb.ai inc.  All rights reserved.

   Test of the basic functionality of authenticating AWS requests.
*/


#define BOOST_TEST_MAIN
#define BOOST_TEST_DYN_LINK

#include <boost/test/unit_test.hpp>
#include "mldb/vfs_handlers/aws/sqs.h"
#include "mldb/arch/file_functions.h"
#include <iostream>
#include <stdlib.h>
#include "mldb/utils/vector_utils.h"
#include "mldb/utils/pair_utils.h"


using namespace std;
using namespace MLDB;

// These are all of those on http://docs.amazonwebservices.com/AmazonS3/2006-03-01/dev/RESTAuthentication.html?r=1821

string accessKeyId = "AKIAIOSFODNN7EXAMPLE";
string accessKey   = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY";



BOOST_AUTO_TEST_CASE( test_signing_v4 )
{
    // Test cases are from
    // http://docs.aws.amazon.com/general/latest/gr/sigv4-calculate-signature.html
    
    string sampleSigningKey = "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY";
    string digest = AwsApi::signingKeyV4(sampleSigningKey, "20110909","us-east-1","iam","aws4_request");
    string hexDigest = AwsApi::hexEncodeDigest(digest);

    BOOST_CHECK_EQUAL(hexDigest, "98f1d889fec4f4421adc522bab0ce1f82e6929c262ed15e5a94c90efd1e3b0e7");

    
    string stringToSign =
        "AWS4-HMAC-SHA256\n"
        "20110909T233600Z\n"
        "20110909/us-east-1/iam/aws4_request\n"
        "3511de7e95d28ecd39e9513b642aee07e54f4941150d8df8bf94b328ef7e55e2";

    string signature = AwsApi::signV4(stringToSign, sampleSigningKey, "20110909", "us-east-1", "iam", "aws4_request");

    BOOST_CHECK_EQUAL(signature, "ced6826de92d2bdeed8f846f0bf508e8559e98e4b0199114b84c54174deb456c");
}


BOOST_AUTO_TEST_CASE( check_canonical_request_v4 )
{
    // See here:

    // http://docs.aws.amazon.com/general/latest/gr/sigv4-signed-request-examples.html

    /*
      POST http://iam.amazonaws.com/ HTTP/1.1
      host: iam.amazonaws.com
      Content-type: application/x-www-form-urlencoded; charset=utf-8
      x-amz-date: 20110909T233600Z

      Action=ListUsers&Version=2010-05-08
    */

    // Authorization: AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20110909/us-east-1/iam/aws4_request, SignedHeaders=content-type;host;x-amz-date, Signature=ced6826de92d2bdeed8f846f0bf508e8559e98e4b0199114b84c54174deb456c

    
    //QueryParams params;
    //params.push_back({"Action","ListUsers"});
    //params.push_back({"Version","2010-05-08"});

    string accessKeyId = "AKIDEXAMPLE";
    string accessKey   = "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY";

    AwsApi::BasicRequest request;
    request.method = "POST";
    request.relativeUri = "";
    request.headers.push_back({"host", "iam.amazonaws.com"});
    request.headers.push_back({"Content-Type", "application/x-www-form-urlencoded; charset=utf-8"});
    //request.headers.push_back({"x-amz-date", "20110909T233600Z"});
    request.payload = "Action=ListUsers&Version=2010-05-08";


    AwsApi::addSignatureV4(request, "iam", "us-east-1",
                           accessKeyId, accessKey,
                           Date(2011,9,9,23,36,00), AwsApi::PLD_IMPLICIT);
    
    Utf8String auth;

    for (auto h: request.headers)
        if (h.first == "Authorization")
            auth = h.second;

    BOOST_CHECK_EQUAL(auth, "AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20110909/us-east-1/iam/aws4_request, SignedHeaders=content-type;host;x-amz-date, Signature=ced6826de92d2bdeed8f846f0bf508e8559e98e4b0199114b84c54174deb456c");
}

BOOST_AUTO_TEST_CASE( check_canonical_request_v4_again )
{
    /*
      GET /jeremytest/hello.txt HTTP/1.1
      Host: s3.amazonaws.com
      Accept-Encoding: identity
      x-amz-content-sha256: e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855
      Authorization: AWS4-HMAC-SHA256 Credential=PSFBZZZZZZZZ/20180329/US/s3/aws4_request,SignedHeaders=host;x-amz-content-sha256;x-amz-date,Signature=f81209a792f55e50f1b59c3257474127e299e6663e29687ac8394c982ca674d2
      x-amz-date: 20180329T192537Z

      Key Id: PSFBZZZZZZZZ
      Key:    0123/456789AB/CDEF
    */

    string accessKeyId = "PSFBZZZZZZZZ";
    string accessKey   = "0123/456789AB/CDEF";

    AwsApi::BasicRequest request;
    request.method = "GET";
    request.relativeUri = "jeremytest/hello.txt";
    request.headers.push_back({"Host", "s3.amazonaws.com"});
    //request.headers.push_back({"x-amz-content-sha256", "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"});

    AwsApi::addSignatureV4(request, "s3", "US", accessKeyId, accessKey, Date(2018,3,29,19,25,37));

    string auth;

    for (auto h: request.headers)
        if (h.first == "Authorization")
            auth = h.second.rawString();

    /*
      canonical request GET
      /jeremytest/hello.txt

      host:s3.amazonaws.com
      x-amz-content-sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855
      x-amz-date:20180329T201205Z

      host;x-amz-content-sha256;x-amz-date
      e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855
      signed request e3a078a4523f99862dd67af882f6e7f83beb4ffdb4ae9a85642d66e091dc34da
      string_to_sign AWS4-HMAC-SHA256
      20180329T201205Z
      20180329/US/s3/aws4_request
      e3a078a4523f99862dd67af882f6e7f83beb4ffdb4ae9a85642d66e091dc34da

    */
    
    string authExpected = "AWS4-HMAC-SHA256 Credential=PSFBZZZZZZZZ/20180329/US/s3/aws4_request, SignedHeaders=host;x-amz-content-sha256;x-amz-date, Signature=f81209a792f55e50f1b59c3257474127e299e6663e29687ac8394c982ca674d2";

    cerr << auth << endl;
    cerr << authExpected << endl;
    
    BOOST_CHECK_EQUAL(auth, authExpected);
    BOOST_CHECK_EQUAL_COLLECTIONS(auth.begin(), auth.end(),
                                  authExpected.begin(), authExpected.end());
}
