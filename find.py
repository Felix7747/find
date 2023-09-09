#!/usr/bin/env python3
import sys
import os
import hashlib
import ntpath
import os
import argparse
import atexit
import pickle
import logging
import time
start_time = time.time()





class duplicateItem:
	def __init__(self):
		self.hash = 0
		self.filename = list()
		self.dupeNo = -1
		self.path = list()
		self.name = list()
		self.dupeCount = 0

def goodbye():
	parser.print_help(sys.stderr)
	sys.exit(0)

def getName(filename):
	return os.path.basename(filename)

def getPath(filename):
	return os.path.split(filename)[0]

def chunk_reader(fobj, chunk_size=1024):
	"""Generator that reads a file in chunks of bytes"""
	while True:
		chunk = fobj.read(chunk_size)
		if not chunk:
			return
		yield chunk

def get_hash(filename, first_chunk_only=False, hash=hashlib.sha1):
	hashobj = hash()
	file_object = open(filename, 'rb')

	if first_chunk_only:
		size = int(args.miniSize[0])
		hashobj.update(file_object.read(size))
	else:
		for chunk in chunk_reader(file_object):
			hashobj.update(chunk)
	hashed = hashobj.digest()
	file_object.close()
	return hashed

dupeList = list()
def checkHashExists(full_hash):
	i = 0
	for dupe in dupeList:
		if full_hash == dupe.hash:
			return i
		i = i + 1
	return None

vmFileList = [".vmdk",".vmx",".vmsd",".vmxf",".lck",".appinfo",".nvram",".vmem",".vmss"]
vmPathList = list()
def check_not_in_exclude(dirpath, filename):
	full_path = os.path.join(dirpath, filename).lower()
	if(args.exclude):
		for excludeString in args.exclude:
			if not (full_path.find(excludeString) == -1):
				logging.info ("Excluding due to \"", excludeString, "\" being found in ", full_path)
				return False
	for excludePath in vmPathList:
		if not (dirpath.find(excludePath) == -1):
			logging.info ("Excluding due to \"", excludePath, "\" path found in ", full_path)
			return False
	if(args.incVMs):
		for excludeString in vmFileList:
			if not (full_path.find(excludeString) == -1):
				logging.info ("Excluding due to \"", excludeString, "\" being found in ", full_path)
				vmPathList.append(dirpath)
				return False
	return True
	
def check_in_include(dirpath, filename):
	full_path = os.path.join(dirpath, filename).lower()
	if(args.include):
		for includeString in args.include:
			if not (full_path.find(includeString.lower()) == -1):
				return True
	else:
		return True
	return False

def check_for_duplicates(paths, hash=hashlib.sha1):
	hashes_by_size = {}
	hashes_on_1k = {}
	hashes_full = {}
	fileCount = 0
	searchedMiniHashCount = 0
	searchedHashCount = 0
	duplicateFileCount = 0
	dupeHashCount = 0 #Estimated amount to be deleted
	print("=========================================================================")
	print ("Checking for duplicate file sizes", paths)
	for path in paths:
		for dirpath, dirnames, filenames in os.walk(path):
			for filename in filenames:
				full_path = os.path.join(dirpath, filename)
				#If not in exclude lists process.
				if (check_not_in_exclude(dirpath, filename) & check_in_include(dirpath, filename)):
					#Get path and size
					try:
						# if the target is a symlink (soft one), this will 
						# dereference it - change the value to the actual target file
						full_path = os.path.realpath(full_path)
						file_size = os.path.getsize(full_path)
					except (OSError,):
						# not accessible (permissions, etc) - pass on
						continue
					#Return index of matched if duplicate exists
					duplicate = hashes_by_size.get(file_size)

					if duplicate:
						hashes_by_size[file_size].append(full_path)
					else:
						hashes_by_size[file_size] = []	# create the list for this file size
						hashes_by_size[file_size].append(full_path)
					fileCount += 1
	print("Searched",fileCount ,"files.")
	print("Found",len(hashes_by_size),"different file sizes.")
	print("=========================================================================")
	# For all files with the same file size, get their hash on the 1st 1024 bytes
	print ("Checking for duplicate mini hash")
	for __, files in hashes_by_size.items():
		if len(files) < 2:
			continue	# this file size is unique, no need to spend cpy cycles on it

		for filename in files:
			try:
				small_hash = get_hash(filename, first_chunk_only=True)
			except (OSError,):
				# the file access might've changed till the exec point got here 
				continue

			duplicate = hashes_on_1k.get(small_hash)
			if duplicate:
				hashes_on_1k[small_hash].append(filename)
			else:
				hashes_on_1k[small_hash] = []		  # create the list for this 1k hash
				hashes_on_1k[small_hash].append(filename)
			searchedMiniHashCount += 1

	print("Searched",searchedMiniHashCount, "files.")
	print("Found",len(hashes_on_1k) ,"unique files with possible duplicates.")
	print("=========================================================================")
	# For all files with the hash on the 1st 1024 bytes, get their hash on the full file - collisions will be duplicates
	print ("Checking for duplicate full hash")
	for __, files in hashes_on_1k.items():
		if len(files) < 2:
			continue	# this hash of fist 1k file bytes is unique, no need to spend cpy cycles on it

		for filename in files:
			try: 
				full_hash = get_hash(filename, first_chunk_only=False)
			except (OSError,):
				# the file access might've changed till the exec point got here 
				continue
			duplicate = hashes_full.get(full_hash)
			if duplicate:
				duplicateFileCount += 1
				dupeHashCount += 1
				exists = checkHashExists(full_hash)
				if (exists != None) :	#Add to existing duplicate list
					dupeList[exists].filename.append(filename)				
					dupeList[exists].name.append(getName(filename))
					dupeList[exists].path.append(getPath(filename))
					dupeList[exists].dupeCount += 1
				else:	#New Duplicate Entry
					newDupe = duplicateItem()
					newDupe.hash = full_hash

					newDupe.filename.append(filename)
					newDupe.name.append(getName(filename))
					newDupe.path.append(getPath(filename))

					newDupe.filename.append(duplicate)
					newDupe.name.append(getName(duplicate))
					newDupe.path.append(getPath(duplicate))
					newDupe.dupeCount = 2
					dupeList.append(newDupe)
					dupeHashCount += 1
			else:
				hashes_full[full_hash] = filename
			searchedHashCount += 1

	print("Searched",searchedHashCount, "files.")
	print("Found",dupeHashCount ,"duplicate files(total).")
	print("Found",duplicateFileCount ,"duplicates viable for deletion.")
	
	print("=========================================================================")

def getDupePaths():
	pathList = list()
	for dupe in dupeList:
		for path in dupe.path:
			pathList.append(path)
	return sorted(list(set(pathList)), key=len, reverse=True)

delListPath = list()
delList = list()
delPaths = list()
def addToDeletion():
	print ("Unique Files: ", len(dupeList))
	for dupe in dupeList:	#Loop through each set of duplicates
		print ("Count of this file: ", dupe.dupeCount)
		tryCount = 0	#Overall attempt count
		delIndex = 0	#Index of paths to check through
		maxCheckLen = len(delListPath)*len(dupe.filename)
		check = True
		while (len(dupe.filename) > 1 and check ):	#Loop until only one item left or break.
			try:
				index = dupe.path.index(delListPath[delIndex])	#If path is (ith) delete path return index.
			except:
				delIndex += 1
				if delIndex > len(delListPath):
					check = False
				continue
			else:
				#Add file and path to delete list
				delList.append(dupe.filename[index])
				delPaths.append(dupe.path[index])
				#Find & remove from current dupe set
				dupe.filename.remove(dupe.filename[index])
				dupe.name.remove(dupe.name[index])
				dupe.path.remove(dupe.path[index])
			tryCount += 1
			if(tryCount > maxCheckLen):
				raise ValueError('addToDeletion: tryCount exceeded macCheckLen. This should not happen.')
				check = False
		print ("Try Count: ",tryCount, "CheckLen: ", maxCheckLen)

def pathsToFile():
	with open('dupelicate_files_path_list.txt', 'w') as filehandle:
		for listitem in getDupePaths():
			filehandle.write('%s\n' % listitem)

def pathsFromFile():
	with open('dupelicate_files_path_list.txt', 'r') as filehandle:
		for line in filehandle:
			currentPlace = line[:-1]
			delListPath.append(currentPlace)

def filesToFile():
	with open('duplicate_file_list.txt', 'w') as filehandle:
		for dupe in dupeList:
			filehandle.write('%s\n' % dupe.filename)

def deleteToFile():
	with open('delete_file_list.txt', 'w') as filehandle:
		for fname in delList:
			filehandle.write('%s\n' % fname)
			
def hashedListToFile():
	with open('hashed_list.dat', 'wb') as filehandle:
		pickle.dump(dupeList, filehandle)
		filehandle.close()

def hashedListFromFile():
	with open('hashed_list.dat', 'rb') as filehandle:
		newList = pickle.load(filehandle)
		filehandle.close()
	return newList

def deleteItems():
	logging.info ("Number of items for deletion: ", len(delList))
	for item in delList:
		os.remove(item)
		pass

def checkArgs(targets):
	#Check for duplicates
	for target in targets:
		if (targets.count(target) > 1):
			logging.error("ERROR: duplicate paths not allowed.")
			goodbye()
	#Check for substring - Forward
	targetList = targets.copy()
	while(len(targetList) > 0):
		test = targetList.pop()
		if any(test in s for s in targetList):
			logging.error("ERROR: Target folder list must not contain their own children.")
			goodbye()
	#Check for substring - Backward
	targetList = targets.copy()
	while(len(targetList) > 0):
		test = targetList.pop(0)
		if any(test in s for s in targetList):
			logging.error("ERROR: Target folder list must not contain their own children.")
			goodbye()

# Argument parser and program initiliisiiaizion
parser = argparse.ArgumentParser(description='Check for duplicates and delete them.')
parser.add_argument('--mode', metavar='[find/delete/dryrun]', type=str, dest='mode', nargs=1, default='find', required=True,
					help='[find] Find duplicates.\n [delete] delete duplicates.\n [dryrun] Run delete without actually deleting.')
parser.add_argument('--target', metavar='target',dest='target', type=str, nargs='*', required=False,
					help='destinations(s) to check for duplicates.')
parser.add_argument('--exclude', metavar='str',dest='exclude', type=str, nargs='*', required=False,
					help="""Strings in the filename or path to exlcude from duplicate list.
					Any in the list will result in exclusion. \"OR function\".
					Case insensitive. Exclude overrides Include.""")
parser.add_argument('--include', metavar='str',dest='include', type=str, nargs='*', required=False,
					help="""Strings which must be present in filename or path to add to duplicate list.
					Any in the list will result in inclusion. \"OR function\".
					Case insensitive. Exclude overrides Include.""")
parser.add_argument('--miniHashSize', metavar='bytes',dest='miniSize', type=int, nargs=1, required=False, default=[1024],
					help='Size (in Bytes) to use for Mini Hash check.')
parser.add_argument('--incvms', dest='incVMs', default=True, const=False, nargs='?',
					required=False, help='Setting [True] allows deletion of detected Virtual Machine files which may have valid duplicates.')

args = parser.parse_args()
logging.basicConfig(filename='find.log',level=logging.ERROR)

if (args.mode[0].lower() == "find"):
	targetList = args.target
	print ("Find Mode")
	checkArgs(targetList)
	check_for_duplicates(args.target)
	hashedListToFile()
	pathsToFile()
	filesToFile()
elif (args.mode[0].lower() == "delete"):
	print ("Delete Mode")
	dupeList = hashedListFromFile()
	pathsFromFile()
	addToDeletion()
	deleteToFile()
	deleteItems()
elif (args.mode[0].lower() == "dryrun"):
	print ("Dry Run")
	dupeList = hashedListFromFile()
	pathsFromFile()
	addToDeletion()
	deleteToFile()
else:
	goodbye()
print("%.3f seconds" % (time.time() - start_time))
