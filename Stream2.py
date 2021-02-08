import requests
import os, time, json, re
import pandas as pd
import kv_secrets, upload_lake
import numpy as np
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
import dateutil.parser

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

reference_types = {"replied_to":20000021,
                   "quoted":20000022,
                   "retweeted":20000023,
                   }

interaction_types = {"author":30000051,
                     "mention":30000052,
                     "reply":30000053,}

occurence_types = {"hashtag":40000071,
                   "annotation":40000072,
                   "other":40000073,
                   }

en_stop_words = set(stopwords.words('english'))
    
de_stop_words = set(stopwords.words('german'))


def get_stream(headers, bearer_token):
    base_url = "https://api.twitter.com/2/tweets/search/stream"
    tweet_fields = ["author_id","created_at", "lang", "source","possibly_sensitive","conversation_id","in_reply_to_user_id","referenced_tweets","entities","public_metrics"]
    expansions = ["author_id","in_reply_to_user_id","referenced_tweets.id","entities.mentions.username","referenced_tweets.id.author_id"]
    user_fields = ["created_at", "location", "verified", "public_metrics"]
    stream_url = base_url + "?tweet.fields=" + ",".join(tweet_fields) + "&expansions=" + ",".join(expansions) +"&user.fields=" + ",".join(user_fields)
    with requests.get(stream_url, headers=headers, stream=True,) as response:
        print(response.status_code)
        if response.status_code != 200:
            raise Exception(
                "Cannot delete rules (HTTP {}): {}".format(
                    response.status_code, response.text
                )
            )
        tweet_frame = pd.DataFrame(columns=["id", "text","author_id","created_at", "lang", "source","possibly_sensitive","conversation_id","retweet_count", "reply_count", "like_count", "quote_count"])
        tweet_frame = tweet_frame.astype({
            "id":"int64",
            "text":"object",
            "author_id":"int64",
            "created_at":"datetime64",
            "lang":"object",
            "source":"object",
            "possibly_sensitive":"bool",
            "conversation_id":"int64",
            "retweet_count":"int64", 
            "reply_count":"int64", 
            "like_count":"int64", 
            "quote_count":"int64",
        })
        
        user_frame = pd.DataFrame(columns=["id", "name", "username","created_at", "location", "verified","followers_count", "following_count", "tweet_count", "listed_count"])
        user_frame = user_frame.astype({
            "id":"int64", 
            "name":"object", 
            "username":"object",
            "created_at":"datetime64", 
            "location":"object", 
            "verified":"bool",
            "followers_count":"int64", 
            "following_count":"int64",
            "tweet_count":"int64", 
            "listed_count":"int64",
        })
        
        rule_match_frame = pd.DataFrame(columns=["tweet.id","rule.id"],dtype="int64")
        
        rule_frame = pd.DataFrame(columns=["id","tag"])
        rule_frame = rule_frame.astype({"id":"int64", "tag":"object"})
        
        user_tweet_interaction_frame = pd.DataFrame(columns=["user.id","tweet.id","username","interaction_type.id"])
        user_tweet_interaction_frame = user_tweet_interaction_frame.astype({"user.id":"int64", "tweet.id":"int64", "username":"object", "interaction_type.id":"int64"})
        
        interaction_type_frame = pd.DataFrame(columns=["id","value"])
        interaction_type_frame = interaction_type_frame.astype({"id":"int64", "value":"object"})
        
        reference_frame = pd.DataFrame(columns=["post.id","referral.id","reference_type.id"], dtype="int64")
        
        reference_type_frame = pd.DataFrame(columns=["id","value"])
        reference_type_frame = reference_type_frame.astype({"id":"int64", "value":"object"})
        
        word_frame = pd.DataFrame(columns=["id","value"])
        word_frame = word_frame.astype({"id":"int64", "value":"object"})
        
        word_occurence_frame = pd.DataFrame(columns=["word.id","tweet.id","annotation_type","occurence_type.id"])
        word_occurence_frame = word_occurence_frame.astype({"word.id":"int64", "tweet.id":"int64", "annotation_type":"object", "occurence_type.id":"int64"})
        
        occurence_type_frame = pd.DataFrame(columns=["id","value"])
        occurence_type_frame = occurence_type_frame.astype({"id":"int64", "value":"object"})
        
        for entry in interaction_types:
            interaction_type_frame = interaction_type_frame.append({"id": interaction_types[entry], "value": entry}, ignore_index=True)
            
        for entry in reference_types:
            reference_type_frame = reference_type_frame.append({"id": reference_types[entry], "value": entry}, ignore_index=True)
        
        for entry in occurence_types:
            occurence_type_frame = occurence_type_frame.append({"id": occurence_types[entry], "value": entry}, ignore_index=True)
        
        
        tweet_fields = ["author_id","created_at", "lang", "source","possibly_sensitive","conversation_id","in_reply_to_user_id","referenced_tweets","entities","public_metrics"]
        response_fields = ["id", "text", *[f for f in tweet_fields[:-4]]]
        tweet_metric_fields = ["retweet_count", "reply_count", "like_count", "quote_count"]
        
        user_fields = ["created_at", "location", "verified", "public_metrics"]
        user_response_fields = ["id", "name", "username", *[f for f in user_fields[:-1]]]
        user_metric_fields = ["followers_count", "following_count", "tweet_count", "listed_count"]
        
        
        tweet_counter = 0
        json_files = ["tweet_id", "user_id", "word_id_count", "word_id"]
        for ft in json_files:
            path  = ft+".json"
            upload_lake.download_json(path)
        
        for response_line in response.iter_lines():
            if response_line:
                json_response = json.loads(response_line)
                #print(json_response["matching_rules"].keys())
                #print(json.dumps(json_response, indent=4, sort_keys=True))
                #try:
                for rule in json_response["matching_rules"]:
                    if rule["id"] not in rule_frame["id"].values.tolist():
                        rule_frame = rule_frame.append({"id" : rule["id"], "tag" : rule["tag"]}, ignore_index=True)
                    
                    word_frame, word_occurence_frame, user_frame, user_tweet_interaction_frame, reference_frame, tweet_frame, rule_match_frame = add_tweet_to_data(json_response, response_fields,rule, tweet_metric_fields, user_response_fields, user_metric_fields, word_frame, word_occurence_frame, user_frame, user_tweet_interaction_frame, reference_frame, tweet_frame, rule_match_frame)
                            
                    #try:
                    if tweet_counter % 1000 == 0:
                        t_path, u_path, w_path, r_path, ut_path, rm_path, o_path, it_path, ot_path, rt_path, ru_path= get_path()
                        frames = [word_frame, word_occurence_frame, user_frame, user_tweet_interaction_frame, reference_frame, tweet_frame, rule_match_frame, interaction_type_frame, occurence_type_frame, reference_type_frame, rule_frame]
                        for idx,path in enumerate([w_path, o_path, u_path, ut_path, r_path, t_path, rm_path, it_path, ot_path, rt_path, ru_path]):
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
                        json_files = ["tweet_id", "user_id", "word_id_count", "word_id"]
                        for ft in json_files:
                            path  = "json/"+ft+".json"
                            upload_lake.upload_json(path)
                        for ft in json_files:
                            path  = ft+".json"
                            upload_lake.download_json(path)
                    tweet_counter += 1
                                #with open(f, 'w', encoding="utf-8") as csvfile:
                                    # csvfile.write('{}";"{}";"{}";"{}\n'.format(created_dict["tweet_id"],preprocess_text(created_dict["tweet_text"]),\
                                    #                                         created_dict["rule_id"], created_dict["rule_tag"]))
                        #except Exception as e:
                            #print(e)
                        #   continue
                #except Exception as e:
                    #print(e)
                    #continue    
                
def add_tweet_to_data(json_response, response_fields,rule, tweet_metric_fields, user_response_fields, user_metric_fields, word_frame, occurence_frame, user_frame, u_t_frame, r_frame, t_frame, rule_match_frame):

    data = json_response["data"]
    
    word_frame, occurence_frame, u_t_frame, r_frame, t_frame, rule_match_frame = add_tweet_to_frame(json_response["data"], json_response,rule, response_fields, tweet_metric_fields, word_frame, occurence_frame,u_t_frame, r_frame, t_frame, rule_match_frame)
                                                                                                    
    if "users" in list(json_response["includes"].keys()):
        for user_data in json_response["includes"]["users"]:
            user_dict = dict()
            for field in user_response_fields:
                if field in user_data.keys():
                    user_dict[field] = user_data[field]
                else:
                    user_dict[field] = None
            user_metric_dict = {field : user_data["public_metrics"][field] for field in user_metric_fields}
            user_dict["location"] = preprocess_text(user_dict["location"])
            user_dict.update(user_metric_dict)
            
            ca = dateutil.parser.parse(user_dict["created_at"])
            user_dict["created_at"] = ca.strftime("%Y-%m-%d")
            
            with open("json/user_id.json", "r") as f:
                user_id_file= json.load(f)
            f.close()
            
            if user_dict["id"] not in user_id_file:
                user_frame = user_frame.append(user_dict, ignore_index = True)
                user_id_file[user_dict["id"]] = 1
                with open("json/user_id.json", "w") as f:
                    json.dump(user_id_file, f)
                f.close()
            
            if user_dict["id"] == data["author_id"]:
                u_t_dict = {"user.id":user_dict["id"], "tweet.id":data["id"], "username":user_dict["username"], "interaction_type.id":interaction_types["author"]}
                u_t_frame = u_t_frame.append(u_t_dict, ignore_index = True)
                
            if "tweets" in list(json_response["includes"].keys()): 
                for tweet_data in json_response["includes"]["tweets"]:
                    if user_dict["id"] == tweet_data["author_id"]:
                        u_t_dict = {"user.id":user_dict["id"], "tweet.id":tweet_data["id"], "username":user_dict["username"], "interaction_type.id":interaction_types["author"]}
                        u_t_frame = u_t_frame.append(u_t_dict, ignore_index = True)
                    
                    word_frame, occurence_frame, u_t_frame, r_frame, t_frame, rule_match_frame = add_tweet_to_frame(tweet_data, json_response,rule, response_fields, tweet_metric_fields, word_frame, occurence_frame, u_t_frame, r_frame, t_frame,rule_match_frame)

    return word_frame, occurence_frame, user_frame, u_t_frame, r_frame, t_frame, rule_match_frame

def add_tweet_to_frame(data, json_response,rule, response_fields,tweet_metric_fields, word_frame, occurence_frame, u_t_frame, r_frame, t_frame,rule_match_frame):
    data = json_response["data"]
    
    with open("json/tweet_id.json", "r") as f:
        tweet_id_file = json.load(f)
    f.close()
    
    if data["id"] in tweet_id_file:
        if rule["id"] in tweet_id_file[data["id"]]:
            return word_frame, occurence_frame, u_t_frame, r_frame, t_frame, rule_match_frame
        else:
            rm_dict = {"tweet.id":data["id"], "rule.id": rule["id"]}
            rule_match_frame = rule_match_frame.append(rm_dict, ignore_index= True)
            tweet_id_file[data["id"]].append(rule["id"])
            with open("json/tweet_id.json", "w") as f:
                tweet_id_file = json.dump(tweet_id_file, f)
            f.close()
            return word_frame, occurence_frame, u_t_frame, r_frame, t_frame, rule_match_frame
    else:
        tweet_id_file[data["id"]] = [rule["id"]]
        with open("json/tweet_id.json", "w") as f:
            json.dump(tweet_id_file, f)
        f.close()
        rm_dict = {"tweet.id":data["id"], "rule.id": rule["id"]}
        rule_match_frame = rule_match_frame.append(rm_dict, ignore_index= True)
        tweet_dict = dict()
        for field in response_fields:
            if field in data.keys():
                tweet_dict[field] = data[field]
            else:
                tweet_dict[field] = None

        tweet_metric_dict = {field : data["public_metrics"][field] for field in tweet_metric_fields}
        tweet_dict.update(tweet_metric_dict)
        
        if "referenced_tweets" in list(data.keys()):
            for t_entry in data["referenced_tweets"]:
                r_dict = {"post.id": tweet_dict["id"], "referral.id": t_entry["id"], "reference_type.id":reference_types[t_entry["type"]]}
                r_frame = r_frame.append(r_dict, ignore_index=True)
                
        if "in_reply_to_user_id" in list(data.keys()):
            username = None
            if "users" in list(json_response["includes"].keys()):
                for user_data in json_response["includes"]["users"]:
                    if data["in_reply_to_user_id"] == user_data["id"]:
                        username = user_data["username"]
            u_t_dict = {"user.id":data["in_reply_to_user_id"], "tweet.id":data["id"], "username":username, "interaction_type.id":interaction_types["reply"]}
            u_t_frame = u_t_frame.append(u_t_dict, ignore_index = True)
        
        tweet_dict["text"] = preprocess_text(tweet_dict["text"])
        
        ca = dateutil.parser.parse(tweet_dict["created_at"])
        tweet_dict["created_at"] = ca.strftime("%Y-%m-%d")
        
        
        t_frame = t_frame.append(tweet_dict, ignore_index = True)
        
        if tweet_dict["lang"] == "de":
            word_list = word_tokenize(tweet_dict["text"], "german")
            word_list = [re.sub(r"[^a-zA-Z]*","",w, flags=re.DOTALL).lower() for w in word_list if w not in de_stop_words]
            word_set = set(word_list)
        else:
            word_list = word_tokenize(tweet_dict["text"])
            word_list = [re.sub(r"[^a-zA-Z]*","",w, flags=re.DOTALL).lower() for w in word_list if w not in en_stop_words]
            word_set = set(word_list)
        
        with open("json/word_id_count.json", "r") as f:
            word_id_file = json.load(f)
        f.close()
        
        if "latest" not in word_id_file:
            word_id_file = dict({
                                "start":60000093,
                                "latest":60000093
                            })
        
        count = word_id_file["latest"]
        
        with open("json/word_id.json", "r") as f:
            word_list_file = json.load(f)
        f.close()
        
        for word in word_set:
            if word not in word_list_file:
                count += 1
                w_dict = {"id":count, "value":word}
                word_frame = word_frame.append(w_dict, ignore_index = True)
                word_list_file[word]  = count
                word_id_file["latest"] = count
                
        with open("json/word_id.json", "w") as f:
            json.dump(word_list_file,f)
        f.close()
                    
        h_list = []
        a_list = []
        if "entities" in list(data.keys()):
            if "hashtags" in list(data["entities"].keys()):
                for h in data["entities"]["hashtags"]:
                    if h["tag"] in word_list_file:
                        o_dict = {"word.id":word_list_file[h["tag"]], "tweet.id":data["id"], "annotation_type":None, "occurence_type.id": occurence_types["hashtag"]}
                    else:
                        count += 1
                        o_dict = {"word.id": count, "tweet.id":data["id"], "annotation_type":None, "occurence_type.id": occurence_types["hashtag"]}   
                        w_dict = {"id":count, "value":h["tag"].lower()}
                        word_frame = word_frame.append(w_dict, ignore_index = True)
                        word_list_file[h["tag"]]  = count
                    h_list.append(h["tag"].lower())
                        
                    occurence_frame = occurence_frame.append(o_dict, ignore_index = True)
                word_id_file["latest"] = count
                with open("json/word_id.json", "w") as f:
                    json.dump(word_list_file,f)
                f.close()
            if "annotations" in list(data["entities"].keys()):
                for a in data["entities"]["annotations"]:
                    if a["normalized_text"].lower() in word_list_file:
                        o_dict = {"word.id":word_list_file[a["normalized_text"].lower()], "tweet.id":data["id"], "annotation_type":a["type"], "occurence_type.id": occurence_types["annotation"]}
                    else:
                        count += 1
                        o_dict = {"word.id": count, "tweet.id":data["id"], "annotation_type":a["type"], "occurence_type.id": occurence_types["annotation"]}
                        w_dict = {"id":count, "value":a["normalized_text"].lower()}
                        word_frame = word_frame.append(w_dict, ignore_index = True)
                        word_list_file[a["normalized_text"].lower()]  = count
                    a_list.append(a["normalized_text"].lower())
                    occurence_frame = occurence_frame.append(o_dict, ignore_index = True)
                    
                word_id_file["latest"] = count
                with open("json/word_id.json", "w") as f:
                    json.dump(word_list_file,f)
                f.close()
                
            if "mentions" in list(data["entities"].keys()):
                for m in data["entities"]["mentions"]:
                    user_id = None
                    if "users" in list(json_response["includes"].keys()):
                        for user_data in json_response["includes"]["users"]:
                            if m["username"] == user_data["username"]:
                                user_id = user_data["id"]
                                
                    u_t_dict = {"user.id":user_id, "tweet.id":data["id"], "username":m["username"], "interaction_type.id":interaction_types["mention"]}
                    u_t_frame = u_t_frame.append(u_t_dict, ignore_index = True)
                    
            for word in word_list:
                if word not in a_list and word not in h_list:
                    o_dict = {"word.id":word_list_file[word], "tweet.id":data["id"], "annotation_type":None, "occurence_type.id": occurence_types["other"]}
                    occurence_frame = occurence_frame.append(o_dict, ignore_index = True)
        
        with open("json/word_id_count.json", "w") as f:
            json.dump(word_id_file, f)
        f.close()

        return word_frame, occurence_frame, u_t_frame, r_frame, t_frame, rule_match_frame
                
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
    if isinstance(text, str):
        text = text.lower()
        replaced_newline = text.replace("\n"," ").replace("\t"," ")
        removed_emojis = remove_emojis(replaced_newline)
        removed_special_chars = re.sub(r"[^a-zA-Z0-9\. ,]*","",removed_emojis, flags=re.DOTALL)
        replace_multiple_whitespaces = re.sub(r" +", " ", removed_special_chars)
        return replace_multiple_whitespaces
    else:
        return text


def get_path():
    user_file_name = "users-" + time.strftime("%Y-%m-%d.csv")
    tweet_file_name = "tweets-" + time.strftime("%Y-%m-%d.csv")
    word_file_name = "words-" +time.strftime("%Y-%m-%d.csv")
    reference_file_name = "references-" + time.strftime("%Y-%m-%d.csv")
    user_tweet_interaction_file_name = "user_tweet_interactions-" + time.strftime("%Y-%m-%d.csv")
    rule_match_file_name = "rule_matches-"+ time.strftime("%Y-%m-%d.csv")
    word_occurences_file_name = "word_occurences-"+ time.strftime("%Y-%m-%d.csv")
    iteraction_type_file_name = "interaction_types-"+time.strftime("%Y-%m-%d.csv")
    occurence_type_file_name = "occurence_types-"+time.strftime("%Y-%m-%d.csv")
    reference_type_file_name = "reference_types-"+time.strftime("%Y-%m-%d.csv")
    rule_file_name = "rules-"+ time.strftime("%Y-%m-%d.csv")
    
    #user_match_file_name = "user_matches-"+ time.strftime("%Y-%m-%d-%H.csv")
    #rule_file_name = "rules-" + time.strftime("%Y-%m-%d.csv")
    directory = "data/"
    u_path = directory + user_file_name
    t_path = directory + tweet_file_name
    w_path =directory + word_file_name
    r_path =directory + reference_file_name
    ut_path = directory + user_tweet_interaction_file_name
    rm_path = directory + rule_match_file_name
    o_path = directory + word_occurences_file_name
    it_path = directory +iteraction_type_file_name
    ot_path = directory +occurence_type_file_name
    rt_path = directory +reference_type_file_name
    ru_path = directory + rule_file_name
    #uma_path = directory + user_match_file_name
    #r_path = directory + rule_file_name
    #if os.path.isfile(t_path) == False:
    #for local_file in os.listdir(directory):
    #    print(local_file)
    #    upload_lake.upload(directory+"/"+local_file)
        #with open(path, 'w') as csvfile:
        #    csvfile.write('tweet_id";"tweet_text";"rule_id";"rule_tag\n')
        #csvfile.close()
    return t_path, u_path, w_path, r_path, ut_path, rm_path, o_path, it_path, ot_path, rt_path, ru_path

def main():
    for i in range(0,10):
        counter = 0
        while True:
            if counter > 100:
                break
            try:
                bearer_token = kv_secrets.get_bearer_token()
                headers = create_headers(bearer_token)
                rules = get_rules(headers, bearer_token)
                delete_all_rules(headers, bearer_token, rules)
                set_rules(headers, bearer_token)
                get_stream(headers, bearer_token)
            except:
                print("Broke Down, Retrying")
                counter += 1
                continue
            break

if __name__ == "__main__":
    main()
