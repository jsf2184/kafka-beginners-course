package com.jefff.udemy.kafka.twitter;

import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;

public class TwitterUtility {

    //  On twitter page: Consumer API Keys -> API key
    public static final String OAUTH_CONSUMER_KEY = "utVoJ3gu0pfAh5QIgoSBGBIsr";

    //  On twitter page: Consumer API Keys -> API Secret key
    public static final String OAUTH_CONSUMER_SECRET = "7u93wPFQL3BfJhZf8cwsPFgX7DvxrM47zp3F7iS6XAghAvoasB";


    //  On twitter page: Access token & access token secret -> Access token
    public static final String OAUTH_ACCESS_TOKEN = "4700456831-7ZmHfxSZWbohj8FGDAPw1QsRoiQrM4r42v8p4DM";

    //  On twitter page: Access token & access token secret -> Access token secret
    public static final String OAUTH_ACCESS_TOKEN_SECRET = "OPwQVloCvGJjxREUrJOgb4LZuao6tdR1ALvPH9OaxEvzZ";


    public static Authentication createAuthentication() {
        Authentication res = new OAuth1( // Application values
                                         OAUTH_CONSUMER_KEY,
                                         OAUTH_CONSUMER_SECRET,
                                         // User values
                                         OAUTH_ACCESS_TOKEN,
                                         OAUTH_ACCESS_TOKEN_SECRET);
        return res;
    }


}
