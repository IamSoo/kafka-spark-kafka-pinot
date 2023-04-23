import csv
import json

import pandas as pd
import uuid


def convert_csv_to_json(input_path:str, output_path:str):
    df_csv = pd.read_csv(input_path)
    df_csv.to_json(output_path)


def convert_csv_to_json_by_line(input_path:str, output_path:str):
    data_dict = []
    with open(input_path, encoding='utf-8') as csv_file_handler:
        csv_reader = csv.DictReader(csv_file_handler)
        for rows in csv_reader:
            # data = {}
            # key = str(uuid.uuid4())
            data_dict.append(rows)

    with open(output_path, 'w', encoding='utf-8') as json_file_handler:
        json_file_handler.write(json.dumps(data_dict, indent=4))






if __name__ == "__main__":
    input_path = "../../data/Car_sales.csv"
    output_path = "../../data/json/Car_sales.json"
    # convert_csv_to_json(input_path, output_path)
    convert_csv_to_json_by_line(input_path, output_path)