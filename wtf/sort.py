#!/usr/bin/env python

import argparse
import fileinput

def pretty_print(followers, max_value, output_file):
  f = open(output_file, 'w')
  for i in range(1,max_value):
    if (followers.get(i) == None):
      f.write(str(i)+'\n')
    else:
      f.write(str(i)+'\t'+followers[i] +'\n')
  f.close()

if __name__ == "__main__":
  parser= argparse.ArgumentParser(description="Takes the malformatted output and makes it good.")
  parser.add_argument("input_file", type=str, help="File with the text to be modified")
  parser.add_argument("output_file", type=str, default="out.txt", help="File where text is outputted.")
  args = parser.parse_args()
  output_file = args.output_file
  input_file = args.input_file
  max_value = 0
  followers = {}

  file = open(input_file,'r')
  line = file.readline()

  while line:
    entries = line.split()
    key = int(entries[0])
    if(key > max_value):
      max_value = key
    if(followers.get(key) == None):
      followers[key] = entries[1]
    else:
      followers[key] = (followers[key] + ' ' +(entries[1]))
    line=file.readline()
  file.close()

  #print all to file
  max_value+=1
  pretty_print(followers, max_value, output_file)
