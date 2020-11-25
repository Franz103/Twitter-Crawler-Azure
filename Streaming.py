import requests
import os, time, json, re
import pandas as pd
import kv_secrets, upload_lake
import pprint
import numpy as np

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
    tweet_fields = ["author_id","created_at", "lang", "source","possibly_sensitive","conversation_id","in_reply_to_user_id","referenced_tweets","entities","public_metrics"]
    expansions = ["author_id","in_reply_to_user_id","referenced_tweets.id","entities.mentions.username"]
    user_fields = ["created_at", "location", "verified", "public_metrics"]
    stream_url = base_url + "?tweet.fields=" + ",".join(tweet_fields) + "&expansions=" + ",".join(expansions) +"&user.fields=" + ",".join(user_fields)
    #with requests.get("https://api.twitter.com/2/tweets/search/stream", headers=headers, stream=True,) as response:
    response_fields = ["id", "text", *[f for f in tweet_fields[:-3]]]
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
        tweet_frame = None
        user_frame = None
        tweet_match_frame = pd.DataFrame(columns=["tweet.id","rule.id"])
        user_match_frame = pd.DataFrame(columns=["user.id","rule.id"])
        #rule_frame = pd.DataFrame(columns=["id","tag"])
        hashtag_frame = pd.DataFrame(columns=["tweet.id","rule.id","tag"])
        annotation_frame = pd.DataFrame(columns=["tweet.id","rule.id","text","type"])
        mention_frame = pd.DataFrame(columns=["tweet.id","rule.id","username"])
        tweet_counter = 0
        for response_line in response.iter_lines():
            if response_line:
                json_response = json.loads(response_line)
                #print(json_response["matching_rules"].keys())
                #print(json.dumps(json_response, indent=4, sort_keys=True))
                try:
                    for rule in json_response["matching_rules"]:
                        #if rule["id"] not in rule_frame["id"].values.tolist():
                        #    rule_frame = rule_frame.append({"id" : rule["id"], "tag" : rule["tag"]}, ignore_index=True)
                            
                        tweet_dict, hashtag_frame, annotation_frame, mention_frame = add_tweet_to_data(json_response["data"],rule,  response_fields, tweet_metric_fields, hashtag_frame, annotation_frame, mention_frame)
                        
                        if not isinstance(tweet_frame ,pd.DataFrame):
                                tweet_frame = pd.DataFrame(columns=list(tweet_dict.keys()))
                        
                        if tweet_dict["id"] not in tweet_frame["id"].values.tolist():
                            tweet_frame = tweet_frame.append(tweet_dict, ignore_index = True)
                            
                        tweet_match_frame = tweet_match_frame.append({"tweet.id" : tweet_dict["id"], "rule.id" : rule["id"]},ignore_index = True)
                        if "tweets" in list(json_response["includes"].keys()):
                            for tweet_data in json_response["includes"]["tweets"]:
                                tweet_dict, hashtag_frame, annotation_frame, mention_frame = add_tweet_to_data(tweet_data,rule, response_fields, tweet_metric_fields,hashtag_frame, annotation_frame, mention_frame, mentioned_by=json_response["data"]["id"])
                                
                                if tweet_dict["id"] not in tweet_frame["id"].values.tolist():
                                    tweet_frame = tweet_frame.append(tweet_dict, ignore_index = True)
                                    
                                tweet_match_frame = tweet_match_frame.append({"tweet.id" : tweet_dict["id"], "rule.id" : rule["id"]},ignore_index = True)
                        if "users" in list(json_response["includes"].keys()):
                            for user_data in json_response["includes"]["users"]:
                                if len(tweet_frame.loc[tweet_frame["author_id"]==user_data["id"], "id"].values.tolist()) > 0:
                                    tweet_id = tweet_frame.loc[tweet_frame["author_id"]==user_data["id"], "id"].values.tolist()[0]
                                    utype = "author"
                                else:
                                    tweet_id = json_response["data"]["id"]
                                    utype = "referenced"
                                user_dict = add_user_to_data(user_data,  tweet_id, utype, user_response, user_metric_fields )
                                
                                if not isinstance(user_frame ,pd.DataFrame):
                                        user_frame = pd.DataFrame(columns=list(user_dict.keys()))
                                        
                                if user_dict["id"] not in user_frame["id"].values.tolist():
                                    user_frame = user_frame.append(user_dict, ignore_index = True)
                                
                                user_match_frame = user_match_frame.append({"rule.id" : rule["id"], "user.id": user_dict["id"]},ignore_index = True)
                        try:
                            tf,uf, hf, af, mf , tma , uma= get_path()
                            frames = [tweet_frame,user_frame, hashtag_frame, annotation_frame, mention_frame, tweet_match_frame, user_match_frame]
                            if tweet_counter % 1000 == 0:
                                for idx,path in enumerate([tf, uf, hf, af, mf, tma, uma]):
                                    local_file = path.split("/")[1]
                                    old_frame = upload_lake.download(local_file)
                                    #if os.path.isfile(path):
                                    if old_frame is not None:
                                        #old_frame = pd.read_csv(path, header=0, delimiter=";")
                                        fr = [old_frame, frames[idx]]
                                        n_frame = pd.concat(fr)
                                        n_frame = n_frame.drop_duplicates()
                                        n_frame.to_csv(path, sep=";", index=False)
                                        upload_lake.upload(path)
                                    else:
                                        fr = frames[idx]
                                        fr.to_csv(path, sep=";", index=False)
                                        upload_lake.upload(path)
                                #rule_frame.to_csv(rf, sep=";", index=False)
                                #upload_lake.upload(rf)
                            tweet_counter += 1
                            #with open(f, 'w', encoding="utf-8") as csvfile:
                                # csvfile.write('{}";"{}";"{}";"{}\n'.format(created_dict["tweet_id"],preprocess_text(created_dict["tweet_text"]),\
                                #                                         created_dict["rule_id"], created_dict["rule_tag"]))
                        except Exception as e:
                            print(e)
                            #continue
                except Exception as e:
                    print(e)
                    #continue    
                
def add_tweet_to_data(data, rule, response_fields, tweet_metric_fields,hashtag_frame, annotation_frame,mention_frame, mentioned_by=None):
    tweet_dict = dict()
    for field in response_fields:
        if field in data.keys():
            tweet_dict[field] = data[field]
        else:
            tweet_dict[field] = "None"
    #tweet_dict["rule.id"] = rule["id"]
    #tweet_dict["rule.tag"]= rule["tag"]
    tweet_metric_dict = {field : data["public_metrics"][field] for field in tweet_metric_fields}
    if not mentioned_by:
        if "referenced_tweets" in list(data.keys()):
            reference_dict = {"referenced_tweets."+ field : data["referenced_tweets"][0][field] for field in ["id","type"]}
            tweet_dict["refered_to_by"] = "None"
        else:
            reference_dict = {"referenced_tweets."+ field : "None" for field in ["id","type"]}
            tweet_dict["refered_to_by"] = "None"
    elif mentioned_by is not None:
        reference_dict = {"referenced_tweets."+ field : "None" for field in ["id","type"]}
        tweet_dict["refered_to_by"] = mentioned_by
    else:
        reference_dict = {"referenced_tweets."+ field : "None" for field in ["id","type"]}
        tweet_dict["refered_to_by"] = "None"

    tweet_dict.update(tweet_metric_dict)
    tweet_dict.update(reference_dict)
    tweet_dict["text"] = preprocess_text(tweet_dict["text"])
    
    if "entities" in list(data.keys()):
        if "hashtags" in list(data["entities"].keys()):
            for h in data["entities"]["hashtags"]:
                h_dict = {"tweet.id":data["id"], "rule.id":rule["id"], "tag":h["tag"]}
                hashtag_frame= hashtag_frame.append(h_dict,ignore_index=True)
        if "annotations" in list(data["entities"].keys()):
            for a in data["entities"]["annotations"]:
                a_dict = {"tweet.id":data["id"], "rule.id":rule["id"], "text":a["normalized_text"], "type":a["type"]}
                annotation_frame=annotation_frame.append(a_dict,ignore_index=True)
        if "mentions" in list(data["entities"].keys()):
            for m in data["entities"]["mentions"]:
                m_dict = {"tweet.id":data["id"], "rule.id":rule["id"], "username":m["username"]}
                mention_frame=mention_frame.append(m_dict, ignore_index=True)
    
    return tweet_dict, hashtag_frame, annotation_frame, mention_frame

def add_user_to_data(user_data, tweet_id, type, user_response, user_metric_fields):
    user_dict = dict()
    for field in user_response:
        if field in user_data.keys():
            user_dict[field] = user_data[field]
        else:
            user_dict[field] = "None"
    user_metric_dict = {field : user_data["public_metrics"][field] for field in user_metric_fields}
    user_dict["name"] = preprocess_text(user_dict["name"])
    user_dict["username"] = preprocess_text(user_dict["username"])
    user_dict["location"] = preprocess_text(user_dict["location"])
    user_dict["tweet.id"] = tweet_id
    user_dict["reference_type"] = type
    user_dict.update(user_metric_dict)
    return user_dict
                
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
    user_file_name = "users-" + time.strftime("%Y-%m-%d.csv")
    tweet_file_name = "tweets-" + time.strftime("%Y-%m-%d.csv")
    hashtag_file_name = "hashtags-" +time.strftime("%Y-%m-%d.csv")
    annotation_file_name = "annotations-" + time.strftime("%Y-%m-%d.csv")
    mention_file_name = "mentions-" + time.strftime("%Y-%m-%d.csv")
    tweet_match_file_name = "tweet_matches-"+ time.strftime("%Y-%m-%d.csv")
    user_match_file_name = "user_matches-"+ time.strftime("%Y-%m-%d.csv")
    #rule_file_name = "rules-" + time.strftime("%Y-%m-%d.csv")
    directory = "data/"
    u_path = directory + user_file_name
    t_path = directory + tweet_file_name
    h_path =directory + hashtag_file_name
    a_path =directory + annotation_file_name
    m_path = directory + mention_file_name
    tma_path = directory + tweet_match_file_name
    uma_path = directory + user_match_file_name
    #r_path = directory + rule_file_name
    #if os.path.isfile(t_path) == False:
    #for local_file in os.listdir(directory):
    #    print(local_file)
    #    upload_lake.upload(directory+"/"+local_file)
        #with open(path, 'w') as csvfile:
        #    csvfile.write('tweet_id";"tweet_text";"rule_id";"rule_tag\n')
        #csvfile.close()
    return t_path, u_path, h_path, a_path, m_path, tma_path, uma_pat

def main():
    bearer_token = kv_secrets.get_bearer_token()
    headers = create_headers(bearer_token)
    rules = get_rules(headers, bearer_token)
    delete_all_rules(headers, bearer_token, rules)
    set_rules(headers, bearer_token)
    get_stream(headers, bearer_token)


if __name__ == "__main__":
    main()
