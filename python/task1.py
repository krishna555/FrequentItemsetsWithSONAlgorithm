from collections import defaultdict
from pyspark import SparkContext
import sys
from math import ceil, floor
from pyspark import SparkConf
from itertools import islice, combinations, chain
import time

class task1:

    def extract_uid_bid(self, rdd_tuple):
        line = rdd_tuple[0]
        filtered_data = line.split(",")
        return (filtered_data[0], filtered_data[1])

    def extract_bid_uid(self, rdd_tuple):
        line = rdd_tuple[0]
        filtered_data = line.split(",")
        return (filtered_data[1], filtered_data[0])

    def select_valid_candidates(self, candidates_dict, support):
        keys = candidates_dict.keys()
        valid_candidates = []
        for key in keys:
            if candidates_dict[key] >= support:
                valid_candidates.append(key)
        valid_candidates.sort()
        return valid_candidates

    def generate_next_iteration(self, records, valid_candidates, size, candidates_dict, support):
        result = defaultdict(int)
        for record in records:
            frequent_granular_records = set(record).intersection(set(valid_candidates))
            for item in combinations(frequent_granular_records, size):
                item_tuple = tuple(sorted(list(item)))
                
                # Task 1 : Generate All immediate subsets and validate if they are themselves frequent
                all_immediate_subsets_valid = True
                for small_item in combinations(item, size - 1):
                    if size - 1 == 1:
                        item_tuple_small = small_item[0]
                    else:
                        item_tuple_small = tuple(sorted(list(small_item)))
                    if candidates_dict[item_tuple_small] < support:
                        all_immediate_subsets_valid = False
                        break
                # Task 2: If all immediate subsets are valid add corresponding count to result dictionary
                if all_immediate_subsets_valid:
                    result[item_tuple] += 1
        return result


    def generate_candidates(self, partition, support, total_records):
        res = []
        records = list(partition)

        num_records_in_partition = len(records) / float(total_records)
        support_threshold = ceil(num_records_in_partition * support)
        # Generate First Set of Candidates:
        candidates_dict = defaultdict(int)
        for record in records:
            for elem in record:
                candidates_dict[elem] += 1
        valid_candidates = self.select_valid_candidates(candidates_dict, support_threshold)
        res.append([(item,) for item in valid_candidates])
        size = 2
        set_of_candidates = valid_candidates
        # print(candidates_dict)
        # print(size)
        print(support_threshold)
        while True:
            candidates_dict = self.generate_next_iteration(records, set_of_candidates, size, candidates_dict, support_threshold)
            # print(size, candidates_dict, support_threshold)
            valid_candidates = self.select_valid_candidates(candidates_dict, support_threshold)
            if len(valid_candidates) == 0:
                break
            res.append(valid_candidates)
            set_of_candidates = set(chain.from_iterable(valid_candidates))
            size += 1    

        return res

    def pass_2_son(self, records, candidates):
        res = defaultdict(int)

        for record in records:
            for candidate in candidates:
                if set(candidate).issubset(record):
                    res[candidate] += 1
        return [(key, val) for key, val in res.items()]
        

    def formatter(self, output_data):
        res = ''
        prev = 1
        for item in output_data:
            item_length = len(item)
            if item_length != prev:
                res += '\n\n'
            if item_length == 1:
                res += str(item).replace(",", "") + ","
                prev = 1
            else:
                res += str(item) + ","
                prev = item_length
        
        res = res.replace(",\n\n", "\n\n")[:-1]
        return res

    def process_input(self, input_path, sc, support, case_no, output_file_path):
        start_time = time.time()
        input_rdd = sc.textFile(input_path)
        metadata = input_rdd.first()
        extracted_data_rdd = input_rdd.zipWithIndex().filter(lambda tuple_data: tuple_data[1] > 0)
        if case_no == 1:
            all_baskets = self.get_bid_aggr_by_uid(extracted_data_rdd)
        else:
            all_baskets = self.get_uid_aggr_by_bid(extracted_data_rdd)
        total_records = all_baskets.count()
        candidates = all_baskets \
            .mapPartitions(lambda partition: self.generate_candidates(partition, support, total_records)) \
            .flatMap(lambda x: x) \
            .distinct() \
            .sortBy(lambda x: (len(x), x)).collect()

        freq_items = all_baskets \
            .mapPartitions(lambda partition: self.pass_2_son(partition, candidates)) \
            .reduceByKey(lambda val1, val2: val1 + val2)\
            .filter(lambda tup: tup[1] >= support)\
            .map(lambda tup: tup[0])\
            .sortBy(lambda x: (len(x), x)).collect()

        with open(output_file_path, "w+") as fout:
            fout.write('Candidates:\n' + self.formatter(candidates) + '\n\n' + 'Frequent Itemsets:\n' + self.formatter(freq_items))
        
        end_time = time.time()
        print('Duration: {}'.format(end_time - start_time))

    def get_uid_aggr_by_bid(self, input_data_rdd):
        return input_data_rdd \
            .map(lambda record: self.extract_bid_uid(record)) \
            .groupByKey() \
            .map(lambda aggr_tuple: (aggr_tuple[0], sorted(list(set(list(aggr_tuple[1])))))) \
            .map(lambda item_users: item_users[1])

    def get_bid_aggr_by_uid(self, input_data_rdd):

        return input_data_rdd \
            .map(lambda record: self.extract_uid_bid(record)) \
            .groupByKey()\
            .map(lambda aggr_tuple: (aggr_tuple[0], sorted(list(set(list(aggr_tuple[1])))))) \
            .map(lambda item_users: item_users[1])

    def main(self):
        case_no = int(sys.argv[1])
        support = int(sys.argv[2])
        input_file_path = sys.argv[3]
        output_path = sys.argv[4]
        conf = SparkConf().setAppName("task1").setMaster("local[*]")
        sc = SparkContext(conf=conf).getOrCreate()
        sc.setLogLevel("ERROR")
        self.process_input(input_file_path, sc, support, case_no, output_path)

if __name__ == "__main__":
    t1 = task1()
    t1.main()