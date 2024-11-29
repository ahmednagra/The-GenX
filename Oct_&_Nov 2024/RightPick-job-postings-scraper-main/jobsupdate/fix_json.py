import re
import sys

# To run this script from the command line, you can use the following command:
# python fix_json.py global_jobs.json

# Or you can import the fix_json_file function and call it with the filename as an argument:
# from fix_json import fix_json_file
# fix_json_file('global_jobs.json')

# This function will read the content of the specified file, replace all occurrences of '][' (with or without a newline in between) with a comma, and write the fixed content back to the file.

def fix_json_file(filename):
    with open(filename, 'r') as f:
        content = f.read()

    # Replace '][' (with or without newline in between) with ','
    content_fixed = re.sub(r']\s*\[', ',', content)

    with open(filename, 'w') as f:
        f.write(content_fixed)

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print(f"Usage: {sys.argv[0]} <filename>")
        sys.exit(1)

    filename = sys.argv[1]
    fix_json_file(filename)