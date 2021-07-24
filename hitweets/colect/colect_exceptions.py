"""
User-defined exceptions to handle errors from conections to Twitter API.
"""
class GetException(Exception):
	
	def __init__(self):
		pass

	def __str__(self):
			return 'Auth error: Make sure that your BEARER_TOKEN is in ambient variables.'

class ConnectionLostException(Exception):
	
	def __init__(self, err):
			self.err = err
	
	def __str__(self):
			return 'Connection Lost: {} - Keep-alive signal lost.'.format(self.err)