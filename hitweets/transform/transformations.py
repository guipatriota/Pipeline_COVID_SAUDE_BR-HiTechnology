import pandas as pd
import os

class Transform():
	"""Class to get JSON files and prepare them to DB.

	First, this process automatcaly reads all JSON files from one Batch and put the data
	into an organized way using pandas dataframe.

	After compiling all data from a batch of 30 minutes it searches for
	duplicates and solve tham.

	Finaly all cleaned data are sent to DB.
	"""
	def __init__(self):
		self.input_df         = []
		self.output_df        = []
		self.time_frame       = 20
		self.batch_time_frame = 60*25
		self.batch_number     = []
		self.file_number      = []
		self.timestamp        = []
		self.index            = []
		self.created_at       = []
		self.covid            = []
		self.saude            = []
		self.data             = []

	def run(self):
		filenames = self.get_filenames()
		
		for filename in filenames:
			self.batch_number.append(self.find_batch_number(filename))
			self.file_number.append(self.find_file_number(filename))
			self.timestamp.append(self.find_timestamp(filename))
		
		files_df = self.return_df_ordered_file(filenames)
		df = self.get_data_from_files_df(files_df)
		self.output_df = self.verify_duplicates(df)

		return self.output_df

	def get_filenames(self):
		filenames = next(os.walk(os.path.abspath(
												os.path.realpath('../colect/data'))),
																												(None, None, []))[2]
		return filenames
			

	def get_data_from_files_df(self, files_df):
		data_folder = os.path.abspath(
													os.path.realpath('../colect/data'))
		self.index      = []
		self.created_at = []
		self.covid      = []
		self.saude      = []
		self.data       = {}
		for filename in files_df.filenames:
			input_not_parsed_df = []
			input_not_parsed_df =  pd.read_json(os.path.join(data_folder, filename))
			self.get_index_from_file_df(input_not_parsed_df)
			self.get_created_at_from_file_df(input_not_parsed_df)
			self.get_covid_from_file_df(input_not_parsed_df)
			self.get_saude_from_file_df(input_not_parsed_df)
		self.data = {'id':self.index, 
								'created_at':self.created_at, 
								'covid':self.covid, 
								'saude':self.saude}
		data_parsed_df = pd.DataFrame(self.data)
		return data_parsed_df


	def get_index_from_file_df(self, input_not_parsed_df):
		for line in range(len(input_not_parsed_df)):
				self.index.append(input_not_parsed_df['data'][line]['id'])


	def get_created_at_from_file_df(self, input_not_parsed_df):
		for line in range(len(input_not_parsed_df)):
				self.created_at.append(
							pd.to_datetime(input_not_parsed_df['data'][line]['created_at']))


	def get_covid_from_file_df(self, input_not_parsed_df):
		for line in range(len(input_not_parsed_df)):
				self.covid.append(False)
				for tag_number in range(
							len(input_not_parsed_df['matching_rules'][line])):
					tag = input_not_parsed_df['matching_rules'][line][tag_number]['tag']
					if 'Covid' in tag:
						self.covid[line]=True

	def get_saude_from_file_df(self, input_not_parsed_df):
		for line in range(len(input_not_parsed_df)):
				self.saude.append(False)
				for tag_number in range(
							len(input_not_parsed_df['matching_rules'][line])):
					tag = input_not_parsed_df['matching_rules'][line][tag_number]['tag']
					if 'Saúde' in tag:
						self.saude[line]=True


	def find_batch_number(self, filename):
		end = filename[filename.find('saude_')+6:]
		batch_number = end[0:end.find('_')]
		return int(batch_number)


	def find_file_number(self, filename):
		end = filename[filename.find('saude_')
															+ 7 
															+ len(str(self.find_batch_number(filename))):]
		file_number = end[0:end.find('_')]
		return int(file_number)


	def find_timestamp(self, filename):
		end = filename[filename.find('saude_')
															+ 8 
															+ len(str(self.find_batch_number(filename)))
															+ len(str(self.find_file_number(filename))):]
		timestamp = end[0:end.find('.')]
		return timestamp


	def return_df_ordered_file(self, filenames):
		data = {'timestamp': self.timestamp,
						'batch_number': self.batch_number,
						'file_number':self.file_number,
						'filenames': filenames}
		df = pd.DataFrame(data).set_index('timestamp').sort_index()
		return df


	def verify_duplicates(self, data_parsed_df):
		cleaned_data_parsed_df = data_parsed_df.drop_duplicates(subset=['id'])
		return cleaned_data_parsed_df


	def group_by_rule_and_time_frame_batchs(self):
		pass


	def create_json_output_file(self):
		pass


def main():
    transf = Transform()
    transf.run()


if __name__ == "__main__":
    main()