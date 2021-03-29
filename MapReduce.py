import threading

class MapReduce():

    def __init__(self, values, function):
        self.values = values
        self.function = function
        self.result = []

    def thread(self, val):
        self.result.append(self.function(val))

    def mapper(self):
        for val in self.values:
            t = threading.Thread(target=self.thread, args=(val,))
            t.start()

if __name__ == "__main__":
    mapReduce = MapReduce([1,2,3,4], lambda x : x**2)
    mapReduce.mapper()
    print(mapReduce.result)