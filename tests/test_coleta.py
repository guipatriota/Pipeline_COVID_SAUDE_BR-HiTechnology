from hitweets.colect.colector import Colector
import time

class TestTwitterComunication:
	
	def test_Colector_set_rules(self):
		colector_test = Colector()
		assert colector_test.set_rules().status_code != 400, "Error 400 received from Twitter server."

	def test_Colector_delete_rules(self):
		colector_test = Colector()
		colector_test.delete_rules()
		assert colector_test.response.status_code != 400, "Error 400 received from Twitter server."

	def test_Colector_get_rules(self):
		colector_test = Colector()
		colector_test.delete_rules()
		colector_test.set_rules()
		assert "data" in colector_test.rules and "errors" not in colector_test.rules

	def test_Colector_count_rules(self):
		colector_test = Colector()
		colector_test.get_rules()
		ids = list(map(lambda rule: rule["id"], colector_test.rules["data" or "errors"]))
		assert len(ids) == 2

class TestTwitterStream:
	def test_Colector_filtered_stream(self):
		response = Colector().get_stream()
		#time.sleep(10)
		assert response.status_code == 200