import datetime
import pymongo as pm
import subprocess
import json
import os
import multiprocessing as mp

#https://stackoverflow.com/questions/431684/how-do-i-change-the-working-directory-in-python
class cd:
	"""Context manager for changing the current working directory"""
	def __init__(self, newPath):
		self.newPath = os.path.expanduser(newPath)

	def __enter__(self):
		self.savedPath = os.getcwd()
		os.chdir(self.newPath)

	def __exit__(self, etype, value, traceback):
		os.chdir(self.savedPath)

OUTPUT = "output"
LAST = "last"

if __name__ == "__main__":
	#parse config file
	with open("config.json", "r") as f:
		config = json.load(f)

	#make directories if they don't exist
	if not os.path.exists(OUTPUT):
		print("making output folder")
		os.makedirs(OUTPUT)
	print("starting")
	#check when last run; if missed then try immediately
	#setup listener socket

	#move this to a thread eventually that will sleep and wake up and do the following
	subprocess.run(["mongodump", "--db", config["database"], "--out", OUTPUT], check=True)
	print("dump OK")
	with cd(os.path.join(OUTPUT, config["database"])):
		subprocess.run(["git", "init"], check=True)
		subprocess.run(["git", "add", "-A"], check=True)
		status = subprocess.run(["git", "status"], check=True, stdout=subprocess.PIPE)
		if b"Changes to be commited" in status.stdout:
			print("changes detected, so pushing")
			subprocess.run(["git", "commit", "-m", str(datetime.datetime.now())], check=True)
			subprocess.run(["git", "remote", "add", "origin", config["remote"]], check=True)
			subprocess.run(["git", "push", "-u", "origin", config["branch"]], check=True)
			print("pushed changes!")
	#update last run