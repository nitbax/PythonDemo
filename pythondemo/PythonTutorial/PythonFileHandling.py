import os

fileptr = open(
    "C:/Users/nitbax/TUTORIALS/Python Tutorial/readFile.txt",
    "a+",
)
if fileptr:
    print("File Opened in read mode")
lineContent = fileptr.readline()
print(lineContent)
print("Read whole file in loop.")
for i in fileptr:
    print(i)
fileptr.write(
    "There are 2 ways to read the content, 1 via read method and 2nd via readline Method"
)
print("Whole File read")
content = fileptr.read()
print(content)
print(type(content))
print("File :", fileptr)
print("File type :", type(fileptr))
basename = os.path.basename(str(fileptr))
file_extension = basename.split(".")[1]
print("File extension", file_extension)
print("File basename:", basename)
fileptr.close()
newFileCreated = open(
    "C:/Users/nitbax/TUTORIALS/Python Tutorial/CreateFile.txt",
    "a",
)
newFileCreated.write("New File created via append mode.")
newFileCreated.close()
with open(
    "C:/Users/nitbax/TUTORIALS/Python Tutorial/CreateFile.txt",
    "r",
) as f:
    print("file pointer position before reading:", f.tell())
    createdContent = f.read()
    print(createdContent)
    print("file pointer position after reading:", f.tell())
    f.seek(10)
    print("file pointer position after modifying file pointer:", f.tell())
os.remove("C:/Users/nitbax/TUTORIALS/Python Tutorial/CreateFile2.txt")
os.rename(
    "C:/Users/nitbax/TUTORIALS/Python Tutorial/CreateFile.txt",
    "C:/Users/nitbax/TUTORIALS/Python Tutorial/CreateFile2.txt",
)
os.rmdir("C:/Users/nitbax/TUTORIALS/Python Tutorial/CheckFolderCreation")
os.mkdir("C:/Users/nitbax/TUTORIALS/Python Tutorial/CheckFolderCreation")
print(os.getcwd())
