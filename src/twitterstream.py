import oauth2 as oauth
import urllib2 as urllib

# Setting maximum tweets number per request
MAX_TWEETS_PER_REQUEST = 1

# See Assginment 6 instructions or README for how to get these credentials
access_token_key = "<>"
access_token_secret = "<>"

consumer_key = "<>"
consumer_secret = "<>"

_debug = 0

oauth_token    = oauth.Token(key=access_token_key, secret=access_token_secret)
oauth_consumer = oauth.Consumer(key=consumer_key, secret=consumer_secret)

signature_method_hmac_sha1 = oauth.SignatureMethod_HMAC_SHA1()

http_method = "GET"


http_handler  = urllib.HTTPHandler(debuglevel=_debug)
https_handler = urllib.HTTPSHandler(debuglevel=_debug)

class TwitterFetcher():

  def __init__(self):
    """
      Init method that creates connection and iterates data folder.
    """


 
  def twitterreq(self, url, method, parameters):
    """"
    Construct, sign, and open a twitter request
    using the hard-coded credentials above.
    """
    req = oauth.Request.from_consumer_and_token(oauth_consumer,
                                               token=oauth_token,
                                               http_method=http_method,
                                               http_url=url, 
                                               parameters=parameters)

    req.sign_request(signature_method_hmac_sha1, oauth_consumer, oauth_token)

    headers = req.to_header()

    if http_method == "POST":
      encoded_post_data = req.to_postdata()
    else:
      encoded_post_data = None
      url = req.to_url()

    opener = urllib.OpenerDirector()
    opener.add_handler(http_handler)
    opener.add_handler(https_handler)

    response = opener.open(url, encoded_post_data)

    return response

  def fetchsamples(self, max_tweets_per_request = MAX_TWEETS_PER_REQUEST):

    #url = "https://stream.twitter.com/1/statuses/sample.json"
    url = "https://stream.twitter.com/1.1/statuses/sample.json"
    parameters = []
    response = self.twitterreq(url, "GET", parameters)
    count = 0
    fetched_tweets = list()
    for line in response:    
      count = count + 1
      if count > max_tweets_per_request:
        break
      fetched_tweets.append(line.strip())

    return fetched_tweets

if __name__ == '__main__':
  fetcher = TwitterFetcher()
  fetcher.fetchsamples()
