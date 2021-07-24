import os
import sys
sys.path.insert(0, os.path.abspath('.'))
import requests
import json
from io import StringIO
import time
import colect_exceptions
import pandas as pd


class Colector():
    """**Colector class for Tweets streaming colect.**

    Constructor that manages all actions related to interface with Twitter API.
    It will call an enviornment variable called 'BEARER_TOKEN' in which the
    bearer token from your account API needs to be stored.
    
    To start data colection run:
    
    ``>>> Colector.run()``

    To stop data colection press:

    ``CTRL + C``

    Or close bash session.

    **To set your enviornment variable open your terminal and run the 
    following line on bash:**

    ``export 'BEARER_TOKEN'='<your_bearer_token>'``

    *Replace <your_bearer_token> with your own bearer token.*
    """
    def __init__(self):
        self._data = []
        self.stream = []
        self.response = None
        self.rules = None
        self.rules_for_filter = [
            {"value": "COVID lang:pt", "tag": "Covid rule"},
            {"value": "Saúde lang:pt", "tag": "Saúde rule"},
            ]
        self.batch_number = 0
        self.file_number = 0
        self.archive_name_prefix = 'db_datalake_tw_covid_saude_'
        self.archive_extension = '.json'
        self._bearer_token = os.environ.get("BEARER_TOKEN")
        self.url_base = "https://api.twitter.com/2/tweets/"
        self.url_search_rules = self.url_base + "search/stream/rules"
        self.url_search_stream = self.url_base + "search/stream"
        self.max_tweets_json = 200
        self.timer_start = 0
        self.batch_time_window_in_minuts = 30
        self.waiting_seconds = 60
    

    def run(self):
        """**Subprocess to start the streaming and loading data into datalake
        as JSON files containing 200 tweets each.**

        The JSON files will be stored at .\data\ as 
        'db_datalake_tw_covid_saude_<batch_number>_<file_number>.json'.


        :raises colect_exceptions.GetException: Authentication error occurred
            when communication with Twitter API does not succeed.

        """
        try:
            self.delete_rules()
            self.set_rules()
        except:
            raise colect_exceptions.GetException()
        try:
            self.timer_start = time.perf_counter()
            self.get_stream()
            self.save_stream()
            self._stream_whatchdogs()
        except:
            pass


    def _bearer_oauth(self,r):
        """**Private method for Twitter's Bearer Token authentication.**

        :return: It returns a Twitter API request class
            This class will only be defined and used at 
            Twitter's API server-side for security reasons.

        """
        r.headers["Authorization"] = f"Bearer {self._bearer_token}"
        r.headers["User-Agent"] = "v2FilteredStreamPython"
        return r


    def get_rules(self):
        """**HTTP Method to get rules of a twitter's filtered stream configured
        on server side.**

        :raises Exception: Failed to connect to Twitter.
            Probably due to lack of API bearer token on OS ambient variables.
        :return: Response from Twitter API with rules on server side.  
        :rtype: request.Response

        """
        self.response = requests.get(
            self.url_search_rules, 
            auth=self._bearer_oauth,
        )
        if self.response.status_code != 200:
            raise Exception(
                "Cannot get rules (HTTP {}): {}".format(
                    self.response.status_code,
                    self.response.text)
            )
        self.rules = self.response.json()
        print(json.dumps(self.rules))
        return self.response


    def delete_rules(self):
        """**HTTP Method to delete rules of a twitter's filtered stream configured
        on server side.**

        :raises Exception: Failed to connect to Twitter.
            Probably due to lack of API bearer token on OS ambient variables.
        :return: Response from Twitter API with informations of deleted rules 
            or errors on server side.
        :rtype: request.Response
        """
        self.get_rules()
        if self.rules is None or "data" not in self.rules:
            return None
        ids = list(map(lambda rule: rule["id"], self.rules["data"]))

        payload = {"delete": {"ids": ids}}
        self.response = requests.post(
            self.url_search_rules,
            auth=self._bearer_oauth,
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
        """**HTTP Method to set rules of a twitter's filtered stream configured
        on server side.**

        :raises Exception: Failed to connect to Twitter.
            Probably due to lack of API bearer token on OS ambient variables.
        :return: Response from Twitter API with informations of rules setted 
            or errors on server side.
        :rtype: request.Response
        """
        payload = {"add": self.rules_for_filter}
        try:
            self.response = requests.post(
                self.url_search_rules,
                auth=self._bearer_oauth,
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
        """**HTTP method to get stream data.**
        
        This HTTP method gets data from the twitter's filtered stream with its
        configured parameters and rules for filtering.

        :return: Response from Twitter API with stream data.
        :rtype: request.Response
        """
        self.stream = requests.get(
            self.url_search_stream,
            auth=self._bearer_oauth,
            params={"tweet.fields": "created_at",
                    "expansions": "author_id",
                    "backfill_minutes": 3},
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
    

    def _stream_whatchdogs(self):
        """**Keep-alive signal monitoring.**
        
        This watchdog monitors the keep-alive signal from Twitter's streaming 
        and restarts the streaming if it disconnects.

        :return: False to end process
        :rtype: Bolean
        """
        try:
            for response_line in self.stream.iter_lines():
                if (self.whatchdog_timer 
                    + self.waiting_seconds 
                    > time.perf_counter() 
                    and "\r\n" in str(response_line)):
                    self.whatchdog_timer = time.perf_counter()
                else:
                    try:
                        time.sleep(self.waiting_seconds)
                        self.get_rules()
                        self.delete_rules()
                        self.set_rules()
                        self.run()
                    except:
                        return False
        except:
            return False
    

    def save_stream(self):
        """**Saves the stream into a list.**
        
        The list (<self._data>) is a private variable and each tweet is saved 
        in one element of it.
        
        Every time <self._data> reaches <self.max_tweets_json> numbers of
        items, the data is saved into an JSON file withsave_jason_file()
        method.

        Every 30 minutes a new batch of files are created for db control
        purposes.
        """
        try:
            for response_line in self.stream.iter_lines():
                if response_line:
                    json_response = json.loads(response_line)
                    #print(json.dumps(json_response, indent=4, sort_keys=True))
                    if "\r\n" in str(response_line):
                        print(json.dumps(json_response))
                    self._data.append(json_response)
                    if len(self._data) % self.max_tweets_json == 0:
                        print('storing data on batch {}, file {}'.format(
                            self.batch_number,
                            self.file_number))
                        if self.timer_30_minutes():
                            self.save_json_file()
                        else:
                            self.timer_start = time.perf_counter()
                            self.save_json_file()
                            self.batch_number +=1
        
        except AttributeError:
            print("Stream not started.\n")
            time.sleep(self.waiting_seconds)
            print("Starting stream.\n")
            self.run()


    def save_json_file(self):
        """**Create JSON file from stream data.**
        
        Saves the stream into JSON files with the folowing name structure:
        <self.archive_name_prefix>_<self.batch_number>_<self.file_number>.json
        After .json creation, the <self._data> list is resetted.
        """
        self.archive_name = (self.archive_name_prefix 
                        + str(self.batch_number)
                        + '_' 
                        + str(self.file_number) 
                        + self.archive_extension)
        pd.read_json(StringIO(json.dumps(self._data)),
                    orient='records').to_json(os.path.join(
                        os.path.dirname(os.path.realpath(__file__)),
                        'data',
                        self.archive_name),
                        orient='records')
        self._data = []
        self.file_number +=1


    def timer_30_minutes(self):
        """**Timer function for 30 minutes.**

        It will return True if the timer is running and False otherwise.

        :return: True if time counter is less than 30 minutes and False
            otherwise.
        :rtype: Bolean
        """
        timer_end = self.timer_start + self.batch_time_window_in_minuts*60
        timer_now = time.perf_counter()
        return timer_now < timer_end


def main():
    colector = Colector()
    colector.run()


if __name__ == "__main__":
    main()