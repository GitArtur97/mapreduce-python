import os

class FileHandler():

    def __init__(self, *input_file_path):
        self.input_file_path = input_file_path

    def split_file_by_lines(self, number_of_lines, directory):
        file_number = 0
        self.create_directory(directory)
        for file_path in self.input_file_path:
            with open(file_path, "r") as file:
                for chunk in self.__split_to_part(file.readlines(), number_of_lines):
                    with open(rf"{directory}\file{file_number}.txt", "w") as new_file:
                        new_file.write(str(file_number)+"\n")
                        for line in chunk:
                            new_file.write(line)
                    file_number += 1
        return file_number

    def __split_to_part(self, lst, n):
        for i in range(0, len(lst), n):
            yield lst[i:i + n]

    @staticmethod
    def create_directory(directory):
        if not os.path.exists(directory):
            os.makedirs(directory)

    @staticmethod
    def delete_files(directory_path):
        if os.path.exists(directory_path):
            filePaths = [f for f in os.listdir(directory_path) if os.path.isfile(os.path.join(directory_path, f))]
            for path in filePaths:
                os.remove(f"{directory_path}\{path}")

if __name__ == "__main__":
    file = FileHandler('shakespeare.txt')
    file.delete_files("input")
    file.split_file_by_lines(1000)




