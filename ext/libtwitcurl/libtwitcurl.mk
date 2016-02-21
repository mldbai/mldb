
libtwitcurl_sources= \
	oauthlib.cpp urlencode.cpp base64.cpp HMAC_SHA1.cpp SHA1.cpp twitcurl.cpp

$(eval $(call library,libtwitcurl,$(libtwitcurl_sources),))

