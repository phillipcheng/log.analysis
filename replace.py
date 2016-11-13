import os, fnmatch, sys
def findReplace(directory, find, replace):
    for path, dirs, files in os.walk(os.path.abspath(directory)):
        for filename in files:
            if filename.endswith(('.xml', '.java', '.sh', '.bat', '.template')):
              filepath = os.path.join(path, filename)
              try:
                with open(filepath) as f:
                  s = f.read()
                  if find in s:
                    s = s.replace(find, replace)
                    with open(filepath, "w") as f:
                      f.write(s)
              except UnicodeDecodeError:
                print(filename + " not readable")

print(sys.argv[0])     
findReplace(sys.argv[1], sys.argv[2], sys.argv[3])