import json
import math
from multiprocessing import Process

import filehandler


class MapReduce:

    def __init__(self, input_file_path, num_of_mappers, num_of_reducers, directory_input="input",
                 directory_output="output"):

        self.directory_input = directory_input
        self.directory_output = directory_output

        filehandler.delete_files(self.directory_input)
        filehandler.delete_files(self.directory_output)
        filehandler.split_file_by_lines(input_file_path, num_of_mappers, directory_input)

        self.num_of_mappers = num_of_mappers
        self.num_of_reducers = num_of_reducers

    def run_mapper(self, index):

        with open(rf"{self.directory_input}\file_{index}.txt", "r") as input_split_file:
            key = input_split_file.readline()
            value = input_split_file.read()

        mapper_result = self.mapper(key, value)
        for reducer_index, chunk in enumerate(
                filehandler.split_to_part(mapper_result, math.ceil(len(mapper_result) / self.num_of_reducers))):
            with open(rf"{self.directory_output}\map_{index}_{reducer_index}.txt", "w+") as map_file:
                json.dump([(key, value) for (key, value) in chunk], map_file)

    def run_reducer(self, index):

        key_values_map = {}
        for mapper_index in range(self.num_of_mappers):
            with open(rf"{self.directory_output}\map_{mapper_index}_{index}.txt", "r") as map_file:
                mapper_results = json.load(map_file)
                for (key, value) in mapper_results:
                    if not (key in key_values_map):
                        key_values_map[key] = []
                    key_values_map[key].append(value)

        key_value_list = []
        for key in key_values_map:
            key_value_list.append(self.reducer(key, key_values_map[key]))

        with open(rf"{self.directory_output}\reduce_{index}.txt", "w+") as output_file:
            json.dump(key_value_list, output_file)

    def mapper(self, key, value):
        results = []

        from nltk.tokenize import word_tokenize
        data = word_tokenize(value.lower())
        for word in data:
            results.append((word, 1))

        return results

    def reducer(self, key, values):

        wordcount = sum(value for value in values)
        return key, wordcount

    def run(self):

        filehandler.create_directory(self.directory_input)
        filehandler.create_directory(self.directory_output)

        map_workers = []
        rdc_workers = []

        for thread_id in range(self.num_of_mappers):
            p = Process(target=self.run_mapper, args=(thread_id,))
            p.start()
            map_workers.append(p)

        [t.join() for t in map_workers]

        for thread_id in range(self.num_of_reducers):
            p = Process(target=self.run_reducer, args=(thread_id,))
            p.start()
            rdc_workers.append(p)

        [t.join() for t in rdc_workers]


if __name__ == "__main__":
    num_mapper = 4
    num_reducer = 2
    map_reduce = MapReduce('shakespeare.txt', num_mapper, num_reducer)
    map_reduce.run()
    filehandler.join_key_value_files(num_reducer)
