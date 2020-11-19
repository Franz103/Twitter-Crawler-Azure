import requests
import os, time, json, re
import pandas as pd
import kv_secrets, upload_lake

# To set your enviornment variables in your terminal run the following line:
# export '<NAME>'='<VALUE>'


def create_headers(bearer_token):
    headers = {"Authorization": "Bearer {}".format(bearer_token)}
    return headers


def get_rules(headers, bearer_token):
    response = requests.get(
        "https://api.twitter.com/2/tweets/search/stream/rules", headers=headers
    )
    if response.status_code != 200:
        raise Exception(
            "Cannot get rules (HTTP {}): {}".format(response.status_code, response.text)
        )
    print(json.dumps(response.json()))
    return response.json()


def delete_all_rules(headers, bearer_token, rules):
    if rules is None or "data" not in rules:
        return None

    ids = list(map(lambda rule: rule["id"], rules["data"]))
    payload = {"delete": {"ids": ids}}
    response = requests.post(
        "https://api.twitter.com/2/tweets/search/stream/rules",
        headers=headers,
        json=payload
    )
    if response.status_code != 200:
        raise Exception(
            "Cannot delete rules (HTTP {}): {}".format(
                response.status_code, response.text
            )
        )
    print(json.dumps(response.json()))


def set_rules(headers, bearer_token):
    # You can adjust the rules if needed
    sample_rules = [
        {"value": "(azure OR #Azure OR @AzDataFactory OR @AzureSupport OR #azurefamily OR #AzureDevOps OR @AzureDevOps) (lang:en OR lang:de)", "tag": "azure as topic"},
        {"value": "((amazon web services) OR #AWS OR #AmazonWebServices OR @AWS_DACH OR @AWS) (lang:en OR lang:de) ", "tag": "aws as topic"},
        {"value": "((google cloud) OR @googlecloud OR @GCPcloud OR GoogleCloud_DACH OR #GoogleCloud ) (lang:en OR lang:de)", "tag": "google as topic"},
        {"value": "(business intelligence OR #PowerBI OR PowerBI OR #BusinessIntelligence) (lang:en OR lang:de)", "tag": "BI as topic"},
        {"value": "(blockchain OR bitcoin OR ethereum OR cardano OR #Blockchain OR #Bitcoin OR #Ethereum OR #Cardano OR #SmartContract) (lang:en OR lang:de) ", "tag": "blockchain as topic"}
    ]
    payload = {"add": sample_rules}
    response = requests.post(
        "https://api.twitter.com/2/tweets/search/stream/rules",
        headers=headers,
        json=payload,
    )
    if response.status_code != 201:
        raise Exception(
            "Cannot add rules (HTTP {}): {}".format(response.status_code, response.text)
        )
    print(json.dumps(response.json()))


def get_stream(headers, bearer_token):
    base_url = "https://api.twitter.com/2/tweets/search/stream"
    tweet_fields = ["author_id","created_at", "in_reply_to_user_id", "lang", "source","conversation_id","possibly_sensitive", "public_metrics"]
    expansion = "author_id"
    user_fields = ["created_at", "location", "verified", "public_metrics"]
    stream_url = base_url + "?tweet.fields=" + ",".join(tweet_fields) + "&expansions=" + expansion +"&user.fields=" + ",".join(user_fields)
    #with requests.get("https://api.twitter.com/2/tweets/search/stream", headers=headers, stream=True,) as response:
    response_fields = ["id", "text", *[f for f in tweet_fields[:-1]]]
    tweet_metric_fields = ["retweet_count", "reply_count", "like_count", "quote_count"]
    user_response = ["id", "name", "username", *[f for f in user_fields[:-1]]]
    user_metric_fields = ["followers_count", "following_count", "tweet_count", "listed_count"]
    with requests.get(stream_url, headers=headers, stream=True,) as response:
        print(response.status_code)
        if response.status_code != 200:
            raise Exception(
                "Cannot delete rules (HTTP {}): {}".format(
                    response.status_code, response.text
                )
            )
        stream_frame = None
        tweet_counter = 0
        for response_line in response.iter_lines():
            if response_line:
                json_response = json.loads(response_line)
                #print(json_response["matching_rules"].keys())
                #print(json.dumps(json_response, indent=4, sort_keys=True))
                try:
                    for rule in json_response["matching_rules"]:
                        created_dict = dict()
                        for field in response_fields:
                            if field in json_response["data"].keys():
                                created_dict["tweet."+field] = json_response["data"][field]
                            else:
                                created_dict["tweet."+field] = "None"
                        created_dict["rule.id"] = rule["id"]
                        created_dict["rule.tag"]= rule["tag"]
                        tweet_metric_dict = {"tweet."+field : json_response["data"]["public_metrics"][field] for field in tweet_metric_fields}
                        user_dict = dict()
                        for field in user_response:
                            if field in json_response["includes"]["users"][0].keys():
                                user_dict["user."+field] = json_response["includes"]["users"][0][field]
                            else:
                                user_dict["user."+field] = "None"
                        user_metric_dict = {"user."+field : json_response["includes"]["users"][0]["public_metrics"][field] for field in user_metric_fields}
                        created_dict.update(tweet_metric_dict)
                        created_dict.update(user_dict)
                        created_dict.update(user_metric_dict)
                        created_dict["tweet.text"] = preprocess_text(created_dict["tweet.text"])
                        created_dict["user.name"] = preprocess_text(created_dict["user.name"])
                        created_dict["user.username"] = preprocess_text(created_dict["user.username"])
                        created_dict["user.location"] = preprocess_text(created_dict["user.location"])
                        try:
                            f = get_path()
                            if not isinstance(stream_frame ,pd.DataFrame):
                                stream_frame = pd.DataFrame(columns=list(created_dict.keys()))
                            
                            stream_frame = stream_frame.append(created_dict, ignore_index = True)
                            stream_frame.set_index("tweet.id")
                            if tweet_counter % 1000 == 0:
                                if os.path.isfile(f):
                                    old_frame = pd.read_csv(f, header=0, delimiter=";")
                                    frames = [old_frame, stream_frame]
                                    n_frame = pd.concat(frames)
                                    n_frame.to_csv(f, sep=",", index=False)
                                else:
                                    stream_frame.to_csv(f, sep=";", index=False)
                            tweet_counter += 1
                            #with open(f, 'w', encoding="utf-8") as csvfile:
                                # csvfile.write('{}";"{}";"{}";"{}\n'.format(created_dict["tweet_id"],preprocess_text(created_dict["tweet_text"]),\
                                #                                         created_dict["rule_id"], created_dict["rule_tag"]))
                        except Exception as e:
                            print(e)
                            continue
                except Exception as e:
                    print(e)
                    continue
                
def remove_emojis(text):
    emoji_pattern = re.compile("["
                            u"\U0001F600-\U0001F64F"  # emoticons
                            u"\U0001F300-\U0001F5FF"  # symbols & pictographs
                            u"\U0001F680-\U0001F6FF"  # transport & map symbols
                            u"\U0001F1E0-\U0001F1FF"  # flags (iOS)
                            u"\U00002702-\U000027B0"
                            u"\U000024C2-\U0001F251"
                            "]+", flags=re.UNICODE)
    return emoji_pattern.sub(r'', text)

def preprocess_text(text):
    replaced_newline = text.replace("\n"," ").replace("\t"," ")
    removed_emojis = remove_emojis(replaced_newline)
    removed_special_chars = re.sub(r"[^a-zA-Z0-9\.,; \_\@\#]*","",removed_emojis, flags=re.DOTALL)
    replace_multiple_whitespaces = re.sub(r" +", " ", removed_special_chars)
    return replace_multiple_whitespaces

def get_path():
    file_name = time.strftime("%Y-%m-%d.csv")
    directory = "data"
    path = "data/" + file_name
    if os.path.isfile(path) == False:
        for local_file in os.listdir(directory):
            print(local_file)
            upload_lake.upload(directory+"/"+local_file)
        #with open(path, 'w') as csvfile:
        #    csvfile.write('tweet_id";"tweet_text";"rule_id";"rule_tag\n')
        #csvfile.close()
    return path

def main():
    bearer_token = kv_secrets.get_bearer_token()
    headers = create_headers(bearer_token)
    rules = get_rules(headers, bearer_token)
    delete_all_rules(headers, bearer_token, rules)
    set_rules(headers, bearer_token)
    get_stream(headers, bearer_token)


if __name__ == "__main__":
    main()
