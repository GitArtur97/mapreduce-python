import os, math

def split_file_by_lines(input_file_path, number_of_splits, dir_to_save ="input"):
    file_number = 0
    create_directory(dir_to_save)

    with open(input_file_path, "r") as file:
        number_of_lines = len(file.readlines())
        file.seek(0)
        number_of_lines_to_split = math.ceil(number_of_lines / number_of_splits)
        for chunk in split_to_part(file.readlines(), number_of_lines_to_split):
            with open(rf"{dir_to_save}\file_{file_number}.txt", "w") as new_file:
                new_file.write(str(len(chunk))+"\n")
                for line in chunk:
                    new_file.write(line)
            file_number += 1
    return file_number

def split_to_part(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


def create_directory(directory):
    if not os.path.exists(directory):
        os.makedirs(directory)


def delete_files(directory_path):
    if os.path.exists(directory_path):
        filePaths = [f for f in os.listdir(directory_path) if os.path.isfile(os.path.join(directory_path, f))]
        for path in filePaths:
            os.remove(f"{directory_path}\{path}")

if __name__ == "__main__":
    delete_files("input")
    split_file_by_lines('shakespeare.txt', 4)




