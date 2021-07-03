import csv
import json
from pyspark import SparkContext, SparkConf
import sys

class Preprocesser:
    def main(self):
        def toCSV(data_tuple):
            return [str(data_tuple[0]), str(data_tuple[1])]
        review_input_path = sys.argv[1]
        business_input_path = sys.argv[2]
        output_path = sys.argv[3]
        state = sys.argv[4]

        sc = SparkContext.getOrCreate()

        review_data_rdd = sc.textFile(review_input_path).map(lambda row: json.loads(row))
        business_data_rdd = sc.textFile(business_input_path).map(lambda row: json.loads(row))

        filtered_business_data = business_data_rdd.map(lambda row: (row['business_id'], row['state'])) \
            .filter(lambda tuple: tuple[1] == state)
        
        user_data = review_data_rdd.map(lambda row: (row['business_id'], row['user_id']))

        extracted_data = filtered_business_data.join(user_data).map(lambda keyVal: toCSV((keyVal[1][1], keyVal[0]))).collect()

        with open(output_path, 'w+',newline="") as of:
            writer = csv.writer(of)
            writer.writerow(['user_id', 'business_id'])
            for line in extracted_data:
                writer.writerow(line)

if __name__ == "__main__":
    p = Preprocesser()
    p.main()

