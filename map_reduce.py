import json
from multiprocessing import Process
from file_handler import FileHandler


class MapReduce():
    def __init__(self, *input_file_path, number_of_lines, directory_input = "input", directory_output = "output"):

        self.directory_input = directory_input
        self.directory_output = directory_output

        FileHandler.delete_files(self.directory_input)
        FileHandler.delete_files(self.directory_output)

        self.n_mappers = FileHandler(*input_file_path).split_file_by_lines(number_of_lines, directory_input)
        self.n_reducers = 3

    def run_mapper(self, index):

        with open(rf"{self.directory_input}\file{index}.txt", "r") as input_split_file:
            key = input_split_file.readline()
            value = input_split_file.read()

        mapper_result = self.mapper(key, value)

        with open(f"{self.directory_output}\map{index}.txt", "w+") as temp_map_file:
            json.dump([(key, value) for (key, value) in mapper_result], temp_map_file)

    def run_reducer(self, index):
        print(index)
        key_values_map = {}
        with open(f"{self.directory_output}\map{index}.txt", "r") as temp_map_file:
            mapper_results = json.load(temp_map_file)
            for (key, value) in mapper_results:
                if not (key in key_values_map):
                    key_values_map[key] = []
                else:
                    key_values_map[key].append(value)

        key_value_list = []
        for key in key_values_map:
            key_value_list.append(self.reducer(key, key_values_map[key]))

        with open(rf"{self.directory_output}\reduce{index}.txt","w+") as output_file:
            json.dump(key_value_list, output_file)

    def mapper(self, key, value):
        results = []

        from nltk.tokenize import word_tokenize
        data = word_tokenize(value.lower())
        for word in data:
            results.append((word,1))

        return results

    def reducer(self, key, values):

        wordcount = sum(value for value in values)
        return key, wordcount

    def run(self):

        FileHandler.create_directory(self.directory_input)
        FileHandler.create_directory(self.directory_output)

        map_workers = []
        rdc_workers = []

        for thread_id in range(self.n_mappers):
            p = Process(target=self.run_mapper, args=(thread_id,))
            p.start()
            map_workers.append(p)

        [t.join() for t in map_workers]

        for thread_id in range(self.n_reducers):
            p = Process(target=self.run_reducer, args=(thread_id,))
            p.start()
            rdc_workers.append(p)
        [t.join() for t in rdc_workers]

if __name__ == "__main__":
    map_reduce = MapReduce('shakespeare.txt', number_of_lines=1000)
    map_reduce.run()