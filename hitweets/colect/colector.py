import requests
import os
import json
from io import StringIO
import time
import threading
import colect_exceptions
import pandas as pd

class Colector():
    """
    Constructor that manages all actions related to interface with Twitter API.
    It will call an enviornment variable called 'BEARER_TOKEN' in which the
    bearer token from your account API needs to be stored.
    To set your enviornment variable open your terminal and run the following 
    line:

    export 'BEARER_TOKEN'='<your_bearer_token>'

    Replace <your_bearer_token> with your own bearer token.
    """
    def __init__(self):
        self.data = []
        self.stream = []
        self.response = None
        self.rules = None
        self.counter = 0
        self.__bearer_token = os.environ.get("BEARER_TOKEN")
        self.url_search_rules = "https://api.twitter.com/2/tweets/search/stream/rules"
        self.url_search_stream = "https://api.twitter.com/2/tweets/search/stream"
        self.max_tweets_json = 100
    
    def run(self):
        try:
            self.delete_rules()
            self.set_rules()
        except:
            raise colect_exceptions.GetException()
        try:
            self.get_stream()
            connection_daemon = threading.Thread(target=self.__stream_whatchdogs)
            connection_daemon.setDaemon(True)
            connection_daemon.start()
            datalake_daemon = threading.Thread(target=self.save_stream)
            datalake_daemon.setDaemon(True)
            datalake_daemon.start()
        except:
            connection_daemon.join()
            datalake_daemon.join()

    def __bearer_oauth(self,r):
        """11
        Private method for Twitter's Bearer Token authentication.

        :return: It returns a 
        """
        r.headers["Authorization"] = f"Bearer {self.__bearer_token}"
        r.headers["User-Agent"] = "v2FilteredStreamPython"
        return r


    def get_rules(self):
        """
        Method to ask for the rules of a twitter's filtered stream

        :raises Exception: Failed to connect to Twitter.
        Probably due to lack of API bearer token on OS ambient variables.
        :return: Response in JSON received from Twitter API endpoint.  
        :rtype: [type]
        """
        self.response = requests.get(
            self.url_search_rules, 
            auth=self.__bearer_oauth,
        )
        if self.response.status_code != 200:
            raise Exception(
                "Cannot get rules (HTTP {}): {}".format(self.response.status_code,
                                                        self.response.text)
            )
        self.rules = self.response.json()
        #print(json.dumps(self.rules))
        return self.response


    def delete_rules(self):
        self.get_rules()
        if self.rules is None or "data" not in self.rules:
            return None
        ids = list(map(lambda rule: rule["id"], self.rules["data"]))

        payload = {"delete": {"ids": ids}}
        self.response = requests.post(
            self.url_search_rules,
            auth=self.__bearer_oauth,
            json=payload,
        )
        if self.response.status_code != 200:
            raise Exception(
                "Cannot delete rules (HTTP {}): {}".format(
                    self.response.status_code, self.response.text
                )
            )
        self.get_rules()
        print(json.dumps(self.response.json()))
        return self.response


    def set_rules(self):
        rules_for_filter = [
            {"value": "COVID lang:pt", "tag": "Covid rule"},
            {"value": "Saúde lang:pt", "tag": "Saúde rule"},
        ]
        payload = {"add": rules_for_filter}
        try:
            self.response = requests.post(
                self.url_search_rules,
                auth=self.__bearer_oauth,
                json=payload,
            )
        except:
            raise Exception
        if self.response.status_code != 201:
            raise Exception(
                "Cannot add rules (HTTP {}): {}".format(
                    self.response.status_code,
                    self.response.text
                )
            )
        self.get_rules()
        print(json.dumps(self.response.json()))
        return self.response


    def get_stream(self):
        self.stream = requests.get(
            self.url_search_stream,
            auth=self.__bearer_oauth,
            params={"tweet.fields": "created_at",
                    "expansions": "author_id",
                    "backfill_minutes": 2},
            stream=True,
        )
        self.whatchdog_timer = time.perf_counter()
        print(self.stream.status_code)
        if self.stream.status_code != 200:
            raise Exception(
                "Cannot get stream (HTTP {}): {}".format(
                    self.stream.status_code, self.stream.text
                )
            )
        return self.stream
    
    #@thread
    def __stream_whatchdogs(self):
        try:
            for response_line in self.stream.iter_lines():
                if self.whatchdog_timer + 20 > time.perf_counter() and "\r\n" in str(response_line):
                    self.whatchdog_timer = time.perf_counter()
                else:
                    try:
                        time.sleep(30)
                        self.get_rules()
                        self.delete_rules()
                        self.set_rules()
                        self.get_stream()
                    except:
                        return False
        except:
            return False

    def save_stream(self):
        try:
            for response_line in self.stream.iter_lines():
                if response_line:
                    json_response = json.loads(response_line)
                    print(json.dumps(json_response, indent=4, sort_keys=True))
                    self.data.append(json_response)
                    if len(self.data) % self.max_tweets_json == 0:
                        print('storing data')
                        pd.read_json(StringIO(json.dumps(self.data)),
                                    orient='records').to_json(os.path.join(
                                        os.path.dirname(os.path.realpath(__file__)),
                                        'data',
                                        f'db_data_lake_tw_covid_saude_{self.counter}.json'),
                                        orient='records')
                        self.data = []
                        self.counter +=1
        # except StreamConsumedError:
        #     print("Deu ruim...")
        
        except AttributeError:
            print("Stream not started.\n")
            print("Starting stream.\n")
            self.get_stream()




def main():

    colector = Colector()
    colector.delete_rules()
    colector.set_rules()
    colector.get_stream()
    colector.save_stream()



if __name__ == "__main__":
    main()