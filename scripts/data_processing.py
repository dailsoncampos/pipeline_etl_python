import json
import csv

class DataProcessor:
	def __init__(self, path, data_type):
		self.path = path
		self.data_type = data_type
		self.data = self.load_DataProcessor()
		self.columns_name = self.get_columns()

	def load_json(self):
		data_json = []
		with open(self.path, 'r') as file:
			data_json = json.load(file)
		return data_json

	def load_csv(self):
		data_csv = []
		with open(self.path, 'r') as file:
			spamreader = csv.DictReader(file, delimiter=',')
			for row in spamreader:
				data_csv.append(row)
		return data_csv

	def load_DataProcessor(self):
		if self.data_type == 'csv':
			data = self.load_csv()
		elif self.data_type == 'json':
			data = self.load_json()
		elif self.data_type == 'list':
			data = self.path
			self.path = 'list in memory'
		return data

	def get_columns(self):
		return list(self.data[-1].keys())

	def rename_columns(self, key_mapping):
		new_columns = []

		for old_dict in self.data:
			dict_temp = {}
			for old_key, value in old_dict.items():
				dict_temp[key_mapping[old_key]] = value
			new_columns.append(dict_temp)

		self.data = new_columns

		self.columns_name = self.get_columns()

	def join(company_a, company_b):
		combined_list = []
		combined_list.extend(company_a.data)
		combined_list.extend(company_b.data)

		return DataProcessor(combined_list, 'list')

	def transform_data_table(self):
		combined_data_table = [self.columns_name]

		for row in self.data:
			line = []
			for column in self.columns_name:
				line.append(row.get(column, 'Indiponível'))
			combined_data_table.append(line)

		return combined_data_table

	def save_to_csv(self, path):
		combined_data_table = self.transform_data_table()

		with open(path, 'w') as file:
			writer = csv.writer(file)
			writer.writerows(combined_data_table)
