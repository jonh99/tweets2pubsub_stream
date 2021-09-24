import requests
import os
import json
import datetime
#-------------------------------------PUBSUB CODE-----------------------------------------

from google.cloud import pubsub_v1

#configure pubsub connection
publisher = pubsub_v1.PublisherClient()

topic_path = publisher.topic_path('stream2bq', 'tweet-stream1')

#write output to pubsub function
def write_to_pubsub(data):
    try:
            
        # publish to the topic, don't forget to encode everything at utf8!
        publisher.publish(topic_path, data=json.dumps({
        
            "text": data["text"],
            "id": data["id"]

            #,"posted_at": datetime.datetime.fromtimestamp(data["created_at"]).strftime('%Y-%m-%d %H:%M:%S')
        }).encode("utf-8"), tweet_id=str(data["id"]).encode("utf-8"))
        
    except Exception as e:
        print(e)
        raise

#------------------------------------------END OF PUBSUB CODE-----------------------------


#The rest of this code is the program to stream tweets into pubsub publisher


# To set your enviornment variables in your terminal run the following line:
# export 'BEARER_TOKEN'='<your_bearer_token>'
bearer_token = os.environ.get("BEARER_TOKEN")


def bearer_oauth(r):
    """
    Method required by bearer token authentication.
    """

    r.headers["Authorization"] = f"Bearer {bearer_token}"
    r.headers["User-Agent"] = "v2FilteredStreamPython"
    return r


def get_rules():
    response = requests.get(
        "https://api.twitter.com/2/tweets/search/stream/rules", auth=bearer_oauth
    )
    if response.status_code != 200:
        raise Exception(
            "Cannot get rules (HTTP {}): {}".format(response.status_code, response.text)
        )
    print(json.dumps(response.json()))
    return response.json()


def delete_all_rules(rules):
    if rules is None or "data" not in rules:
        return None

    ids = list(map(lambda rule: rule["id"], rules["data"]))
    payload = {"delete": {"ids": ids}}
    response = requests.post(
        "https://api.twitter.com/2/tweets/search/stream/rules",
        auth=bearer_oauth,
        json=payload
    )
    if response.status_code != 200:
        raise Exception(
            "Cannot delete rules (HTTP {}): {}".format(
                response.status_code, response.text
            )
        )
    print(json.dumps(response.json()))


def set_rules(delete):
    # You can adjust the rules if needed
    sample_rules = [
        {"value": "#chrispratt", "tag": "chris pratt"},
        #{"value": "cat has:images -grumpy", "tag": "cat pictures"},
    ]
    payload = {"add": sample_rules}
    response = requests.post(
        "https://api.twitter.com/2/tweets/search/stream/rules",
        auth=bearer_oauth,
        json=payload,
    )
    if response.status_code != 201:
        raise Exception(
            "Cannot add rules (HTTP {}): {}".format(response.status_code, response.text)
        )
    print(json.dumps(response.json()))


def get_stream(set):
    response = requests.get(
        "https://api.twitter.com/2/tweets/search/stream", auth=bearer_oauth, stream=True,
    )
    print(response.status_code)
    if response.status_code != 200:
        raise Exception(
            "Cannot get stream (HTTP {}): {}".format(
                response.status_code, response.text
            )
        )

    for response_line in response.iter_lines():
        if response_line:   
            json_response = json.loads(response_line)
            write_to_pubsub(json.dumps(json_response))
            
    
            
    #line above was formerly a combo of the two below
    #print(json.dumps(json_response, indent=4, sort_keys=True))
    #write_to_pubsub(reformat_tweet(data._json))


def main():
    rules = get_rules()
    delete = delete_all_rules(rules)
    set = set_rules(delete)
    get_stream(set)


if __name__ == "__main__":
    main()


#need to put this into exception handler:
'''
if isinstance(latest_result, dict):
    # just one result, wrap it in a single-element list
    latest_result = [latest_result]

'''