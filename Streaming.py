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
        {"value": "azure lang:en", "tag": "azure as topic"},
        {"value": "(aws OR (amazon web services)) lang:en", "tag": "aws as topic"},
        {"value": "(google cloud OR gcp) lang:en", "tag": "google as topic"},
        {"value": "(business intelligence OR \"BI\") lang:en", "tag": "BI as topic"},
        {"value": "(blockchain OR bitcoin OR ethereum OR cardano) lang:en ", "tag": "blockchain as topic"}
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
    tweet_fields = ["author_id","created_at", "in_reply_to_user_id", "lang", "source", "public_metrics"]
    expansion = "author_id"
    user_fields = ["created_at", "location", "verified", "public_metrics"]
    stream_url = base_url + "?tweet.fields=" + ",".join(tweet_fields) + "&expansion=" + expansion +"&user.fields=" + ",".join(user_fields)
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
        for response_line in response.iter_lines():
            if response_line:
                json_response = json.loads(response_line)
                #print(json_response["matching_rules"].keys())
                #print(json.dumps(json_response, indent=4, sort_keys=True))
                try:
                    for rule in json_response["matching_rules"]:
                        created_dict = {"tweet."+field: json_response["data"][field] for field in response_fields}
                        created_dict["rule_id"] = rule["id"]
                        created_dict["rule_tag"]= rule["tag"]
                        tweet_metric_dict = {"tweet."+field : json_response["data"]["public_metrics"][field] for field in tweet_metric_fields}
                        user_dict = {"user."+field : json_response["includes"]["users"][field] for field in user_response}
                        user_metric_dict = {"user."+field : json_response["includes"]["users"]["public_metrics"][field] for field in user_metric_fields}
                        created_dict.update(tweet_metric_dict)
                        created_dict.update(user_dict)
                        created_dict.update(user_metric_dict)
                        try:
                            f = get_path()
                            if isinstance(stream_frame ,pd.DataFrame):
                                stream_frame = stream_frame.append(pd.DataFrame.from_dict(created_dict), ignore_index = True)
                            else:
                                stream_frame = pd.DataFrame.from_dict(created_dict)
                            stream_frame.to_csv(f, sep=";")
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
    replaced_newline = text.replace('\n' , ' ')
    removed_emojis = remove_emojis(replaced_newline)
    removed_special_chars = re.sub(r"[^a-zA-Z0-9\.,;- \"\_\@\#]*","",removed_emojis, flags=re.DOTALL)
    replace_multiple_whitespaces = removed_special_chars.replace(r" +"," ")
    return replace_multiple_whitespaces

def get_path():
    file_name = time.strftime("%Y-%m-%d.csv")
    directory = "data"
    path = "data/" + file_name
    if os.path.isfile(path) == False:
        for local_file in os.listdir(directory):
            print(local_file)
            upload_lake.upload(directory+"/"+local_file)
        with open(path, 'w') as csvfile:
            csvfile.write('tweet_id";"tweet_text";"rule_id";"rule_tag\n')
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
