import threading

class Reduce(threading.Thread):

    def __init__(self, data, threadID):
        threading.Thread.__init__(self)
        self.data = data
        self.threadID = threadID

    def run(self):
        self.reduce()

    def reduce(self):
        reduced = str({self.data[0]['key']: len(self.data)})
        with open('output.txt','a') as file:
            file.write(f"{reduced}\n")