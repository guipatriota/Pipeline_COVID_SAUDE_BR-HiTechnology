"""
User-defined exceptions to handle errors from conections to Twitter API.
"""
class GetException(Exception):
	"""Generates exception mesage for get requests

	:param Exception: Inherit all Exception methods and does not stop the thread.
	:type Exception: Built-in Exception
	"""
	
	def __init__(self):
		pass

	def __str__(self):
			return 'Auth error: Put your BEARER_TOKEN in ambient variables.'

class ConnectionLostException(Exception):
	"""Generates exception message when connection is lost.

	:param Exception: Inherit all Exception methods and does not stop the thread.
	:type Exception: Built-in Exception
	"""
	def __init__(self, err):
			self.err = err
	
	def __str__(self):
			return 'Connection Lost: {} - Keep-alive signal lost.'.format(self.err)