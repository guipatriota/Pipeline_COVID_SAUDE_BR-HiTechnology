import os
import signal
import sys
sys.path.insert(0, os.path.abspath(os.path.realpath('.')))
sys.path.insert(1, os.path.abspath(os.path.dirname(os.path.realpath(__file__))))

import requests
import urllib.request
import json
from io import StringIO
import time
import colect_exceptions
import pandas as pd
from datetime import datetime, timezone
import threading


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
        self.url_base = "https://api.twitter.com"
        self.url_search_rules = self.url_base + "/2/tweets/search/stream/rules"
        self.url_search_stream = self.url_base + "/2/tweets/search/stream"
        self.max_tweets_json = 200
        self.timer_start = 0
        self.batch_time_window_in_minutes = 30
        self.waiting_seconds = 60
        self.response_line = b''
        self.whatchdog_counter = 0
        self.keep_alive = True
        self.path_to_save = os.path.join(os.path.dirname(
            os.path.realpath(__file__)), 'data')

    def signal_handler(self, signal, frame):
        sys.exit(0)


    def thread_signal_handler(self, signal, frame):
        os.kill(os.getpid(), signal.SIGINT)


    def run(self):
        """**Subprocess to start the streaming and loading data into datalake
        as JSON files containing 200 tweets each.**

        The JSON files will be stored at data folder as 
        'db_datalake_tw_covid_saude_<batch_number>_<file_number>.json'.


        :raises colect_exceptions.GetException: Authentication error occurred
            when communication with Twitter API does not succeed.

        """
        signal.signal(signal.SIGINT, self.signal_handler)
        attempts = 1
        if self.connection_check() and attempts >= 1:
            try:
                try:
                    self.delete_rules()
                    self.set_rules()
                except:
                    raise colect_exceptions.GetException()
                try:
                    self.timer_start = time.perf_counter()
                    self.get_stream()
                    print('\nStarting whatchdog.\n')
                    whatchdog = threading.Thread(target=self._stream_whatchdogs)
                    # signal.signal(signal.SIGINT, self.thread_signal_handler)
                    whatchdog.start()
                    print('\nStarting JSON files creation.\n')
                    save_stream_process = threading.Thread(target=self.save_stream)
                    # signal.signal(signal.SIGINT, self.thread_signal_handler)
                    save_stream_process.start()
                    attempts = 0
                    
                    #self.save_stream()
                except:
                    print('Whatchdog start and data saving failed.')
            except:
                print('No internet connection.\n')
                print('Verify if API bearer token on OS ambient variables.')
                print('Verify your internet connection.')
                time.sleep(30)
                # whatchdog.join()
                # save_stream_process.join()
            time.sleep(5)
        else:
            # whatchdog.join()
            # save_stream_process.join()
            print('\nInternet connection down.\n')
            print('\nRetry in 30 seconds...\n')
            time.sleep(30)
            print('\nRestarting processes.\n')
            self.run()




    def _bearer_oauth(self,r):
        """**Private method for Twitter's Bearer Token authentication.**

        :return: It returns a Twitter API request class
            This class will only be defined and used at 
            Twitter's API server-side for security reasons.

        """
        r.headers["Authorization"] = f"Bearer {self._bearer_token}"
        r.headers["User-Agent"] = "v2FilteredStreamPython"
        return r


    def connection_check(self):
        try:
            urllib.request.urlopen('https://twitter.com')
            return True
        except:
            return False


    def get_rules(self):
        """**HTTP Method to get rules of a twitter's filtered stream configured
        on server side.**

        :raises Exception: Failed to connect to Twitter.
            Probably due to lack of API bearer token on OS ambient variables.
        :return: Response from Twitter API with rules on server side.  
        :rtype: request.Response

        """
        print('\nGetting rules from Twitter API server:\n')
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
        print('\nDeleting rules:\n')
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
            print('\nSetting rules for Twitter API:\n')
            self.response = requests.post(
                self.url_search_rules,
                auth=self._bearer_oauth,
                json=payload,
            )
        except:
            print('\nSet rules failed. Verify your \
                bearer token and connection.\n')
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
        print('\nStart streamming.\n')
        self.whatchdog_timer = time.perf_counter()
        self.stream = requests.get(
            self.url_search_stream,
            auth=self._bearer_oauth,
            params={"tweet.fields": "created_at",
                    "expansions": "author_id",
                    "backfill_minutes": 3},
            stream=True,
        )
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
            keep_alive_period = time.perf_counter() - self.whatchdog_timer
            if (keep_alive_period > self.waiting_seconds/6 
                                and self.whatchdog_counter < 1):
                print('\nKeep-alive whatchdog says:')
                print('ping\n')
                self.whatchdog_counter += 1
                return True
             
            elif (keep_alive_period > self.waiting_seconds
                and self.keep_alive):
                self.keep_alive = False
                return True

            elif (keep_alive_period < self.waiting_seconds):
                return True

            else:
                try:
                    print('\nConnection lost. Waiting...\n')
                    self.whatchdog_counter = 0
                    self.keep_alive = True
                    self.whatchdog_timer = time.perf_counter()
                    time.sleep(self.waiting_seconds)
                    print('Try reconnecting.')
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
            for self.response_line in self.stream.iter_lines():
                if self.response_line == b'':
                    print('\nTwitter says:')
                    print('pong\n')
                    self.keep_alive = True
                    self.whatchdog_timer = time.perf_counter()
                    self.whatchdog_counter = 0
                self._stream_whatchdogs()
                if self.response_line:
                    json_response = json.loads(self.response_line)
                    #print(json.dumps(json_response, indent=4, sort_keys=True))
                    self._data.append(json_response)
                    if len(self._data) % self.max_tweets_json == 0:
                        print('Storing data on batch {}, file {}'.format(
                            self.batch_number,
                            self.file_number))
                        if self.timer_30_minutes():
                            self.save_json_file()
                        else:
                            self.timer_start = time.perf_counter()
                            self.save_json_file()
                            self.batch_number +=1
                            self.file_number = 0
        
        except AttributeError:
            print("\nStream not started.\n")
            time.sleep(self.waiting_seconds)
            print("Starting stream.\n")
            self.run()


    def save_json_file(self):
        """**Create JSON file from stream data.**
        
        Saves the stream into JSON files with the folowing name structure:
        <self.archive_name_prefix>_<self.batch_number>_<self.file_number>.json
        After .json creation, the <self._data> list is resetted.
        """
        date = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
        self.archive_name = (self.archive_name_prefix 
                        + str(self.batch_number)
                        + '_' 
                        + str(self.file_number)
                        + '_'
                        + date 
                        + self.archive_extension)
        pd.read_json(StringIO(json.dumps(self._data)),
                    orient='records').to_json(os.path.join(
                        self.path_to_save,
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
        timer_end = self.timer_start + self.batch_time_window_in_minutes*60
        timer_now = time.perf_counter()
        return timer_now < timer_end


def main():
    colector = Colector()
    colector.run()


if __name__ == "__main__":
    main()