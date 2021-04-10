import threading

class Map(threading.Thread):

    def __init__(self, chunk, threadID):
        threading.Thread.__init__(self)
        self.chunk = chunk
        self.threadID = threadID

    def run(self):
        self.map()

    def map(self):
        data = []
        for key in self.chunk:
            data.append({'key': key, 'value':1})
        with open(f'data_map\map{self.threadID}.txt', 'w') as file:
            file.write(str(data))




