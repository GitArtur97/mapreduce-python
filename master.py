from nltk.tokenize import word_tokenize
from map import Map
from reduce import Reduce
from os import listdir
from os.path import isfile, join
import ast
import itertools

class Master():

    def __init__(self, *files):
        self.files = files
        self.data = []
        self.chunks = []
        self.mapThreads = []
        self.reduceThreads = []

    def split(self):
        for path in self.files:
            with open(path, 'r') as file:
                self.data = word_tokenize(file.read().lower())
                for chunk in self.splitFile(self.data, 1000):
                    self.chunks.append(chunk)

    def splitFile(self, lst, n):
        for i in range(0, len(lst), n):
            yield lst[i:i + n]

    def mapper(self):
        for id, chunk in enumerate(self.chunks):
            mapThread = Map(chunk, id)
            mapThread.start()
            self.mapThreads.append(mapThread)

        for thread in self.mapThreads:
            thread.join()
        print("Map done")

    def reducer(self):
        open("output.txt", "w")
        for id, data in enumerate(self.data):
            reduceThread = Reduce(data, id)
            reduceThread.start()
            self.reduceThreads.append(reduceThread)

        for thread in self.reduceThreads:
            thread.join()
        print("Reduce done")

    def shuffle(self):
        shuffleData = []
        filePaths = [f for f in listdir("data_map") if isfile(join("data_map", f))]
        for path in filePaths:
            with open(f"data_map\{path}", 'r') as file:
                shuffleData.append(ast.literal_eval(file.read()))
        shuffleData = list(itertools.chain.from_iterable(shuffleData))
        shuffleData.sort(key=lambda k: k['key'])
        shuffleData = [list(j) for i, j in itertools.groupby(shuffleData)]
        self.data = shuffleData
        print("Shuffle done")

if __name__ == "__main__":
    mapReduce = Master('shakespeare.txt')
    mapReduce.split()
    mapReduce.mapper()
    mapReduce.shuffle()
    mapReduce.reducer()
