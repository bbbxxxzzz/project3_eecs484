from deepdiff import DeepDiff

def read_file(file_path):
    with open(file_path, 'r') as file:
        return file.read()

file1_path = 'sample.json'
file2_path = 'output.json'

file1_content = read_file(file1_path)
file2_content = read_file(file2_path)

differences = DeepDiff(file1_content, file2_content)
print(differences)