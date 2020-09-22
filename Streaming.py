import requests
import os, time, json, csv
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
        {"value": "azure", "tag": "azure as topic"},
        {"value": "aws", "tag": "aws as topic"},
        {"value": "google cloud", "tag": "google as topic"},
        {"value": "business intelligence", "tag": "BI as topic"},
        {"value": "blockchain", "tag": "blockchain as topic"}
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
    with requests.get("https://api.twitter.com/2/tweets/search/stream", headers=headers, stream=True,) as response:
        print(response.status_code)
        if response.status_code != 200:
            raise Exception(
                "Cannot delete rules (HTTP {}): {}".format(
                    response.status_code, response.text
                )
            )
        for response_line in response.iter_lines():
            if response_line:
                json_response = json.loads(response_line)
                #print(json_response["matching_rules"].keys())
                #print(json.dumps(json_response, indent=4, sort_keys=True))
                for rule in json_response["matching_rules"]:
                    created_dict = {"tweet_id": json_response["data"]["id"], "tweet_text": json_response["data"]["text"],\
                                    "rule_id": rule["id"], "rule_tag": rule["tag"]}
                    try:
                        f = get_path()
                        with open(f, 'a', encoding="utf-8") as csvfile:
                            csvfile.write('"{}";"{}";"{}";"{}"\n'.format(created_dict["tweet_id"],created_dict["tweet_text"],\
                                                                        created_dict["rule_id"], created_dict["rule_tag"]))
                    except Exception as e:
                        print(e)
                        continue

def get_path():
    file_name = time.strftime("%Y-%m-%d.csv")
    directory = "data"
    path = "data/" + file_name
    if os.path.isfile(path) == False:
        for local_file in os.listdir(directory):
            print(local_file)
            upload_lake.upload(directory+"/"+local_file)
        with open(path, 'w') as csvfile:
            csvfile.write("tweet_id,tweet_text,rule_id,rule_tag\n")
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
