import mapreduce, filehandler


class CountCharacters(mapreduce.MapReduce):

    def mapper(self, key, value):
        results = []

        for char in value:
            results.append((char.lower(), 1))

        return results


if __name__ == "__main__":
    num_mapper = 5
    num_reducer = 2
    map_reduce = CountCharacters('shakespeare.txt', num_mapper, num_reducer)
    map_reduce.run()
    filehandler.join_key_value_files(num_reducer)
