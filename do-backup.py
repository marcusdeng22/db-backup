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

	'''
	#make directories if they don't exist
	if not os.path.exists(OUTPUT):
		print("making output folder")
		os.makedirs(OUTPUT)
	'''

	print("starting")
	#check when last run; if missed then try immediately
	#setup listener socket

	#make dump folder
	gitPath = os.path.join(OUTPUT, config["database"])
	if not os.path.exists(gitPath):
		os.makedirs(gitPath)

	with cd(gitPath):
		subprocess.run(["git", "init"], check=True)
		#check if local username/email set
		try:
			subprocess.run(["grep", "\[user\]", ".git/config"], check=True)
		except subprocess.CalledProcessError:
			print("initializing username")
			subprocess.run(["git", "config", "--local", "user.name", config["username"]])
			print("initializing email")
			subprocess.run(["git", "config", "--local", "user.email", config["email"]])

		try:
			subprocess.run(["git", "remote", "add", "origin", config["remote"]], check=True)
		except subprocess.CalledProcessError:
			print("remote already set")

		subprocess.run(["git", "fetch", "origin"])
		subprocess.run(["git", "checkout", config["branch"]])

	#move this to a thread eventually that will sleep and wake up and do the following
	subprocess.run(["mongodump", "--db", config["database"], "--out", OUTPUT], check=True)
	print("dump OK")
	with cd(gitPath):
		subprocess.run(["git", "pull"])

		print("git add")
		subprocess.run(["git", "add", "-A"], check=True)
		print("git status")
		status = subprocess.run(["git", "status"], check=True, stdout=subprocess.PIPE)

		if b"Changes to be committed" in status.stdout:
			print("changes detected, so pushing")
			subprocess.run(["git", "commit", "-m", str(datetime.datetime.now())], check=True)
			subprocess.run(["git", "push", "-u", "origin", config["branch"]], check=True)
			print("pushed changes!")
		elif b"No commits yet" in status.stdout:
			print("no commits yet, so pushing")
			subprocess.run(["git", "commit", "-m", str(datetime.datetime.now())], check=True)
			subprocess.run(["git", "push", "-u", "origin", config["branch"]], check=True)
		else:
			print(status.stdout)
	#update last run

