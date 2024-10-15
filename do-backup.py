import subprocess
import json
import os
from multiprocessing.connection import Listener
from threading import Lock, Event, Thread
import sys
import datetime as dt
from datetime import datetime, timedelta
import sched
import time

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
config = {}
period = 0
runat = 0
gitPath = ""
listener = Listener(("localhost", 9999,), authkey=b"test")
scheduler = sched.scheduler(time.time, time.sleep)
schedEvent = Event()
scheduledJobTime = None
backupLock = Lock()

def configure():
	global config, period, runat
	backupLock.acquire()
	#parse config file
	try:
		#load a temp config
		with open("config.json", "r") as f:
			tempConfig = json.load(f)
		if tempConfig["period"] >= 24 or tempConfig["period"] < 1:
			raise TypeError("Invalid period (1-23)")
		period = timedelta(hours=tempConfig["period"])
		if tempConfig["runat"] < 0 or tempConfig["runat"] > 23:
			raise TypeError("Invalid runat (0-23)")
		runat = dt.time(hour=tempConfig["runat"])
		#cancel all backup events
		for event in scheduler.queue:
			scheduler.cancel(event)
		print("Reset scheduler:", len(scheduler.queue))
		#notify?	TEST
		schedEvent.set()
		#set mongo address
		if "mongodb-ip" not in tempConfig:
			tempConfig["mongodb-ip"] = "localhost"
		if "mongodb-port" not in tempConfig:
			tempConfig["mongodb-port"] = "27017"
		#set new config
		config = tempConfig
		return (True, "Configuration OK",)
	except IOError:
		print("No config.json file found; exiting")
		sys.exit()
	except Exception as e:
		return (False, "Bad configuration " + str(e))
	finally:
		backupLock.release()

def lastJobTime():
	expectedStart = datetime.combine(dt.date.today(), runat)
	curRuntime = expectedStart - timedelta(days=1)
	while curRuntime + period < expectedStart and curRuntime + period < datetime.today():
		curRuntime += period

	if curRuntime + period > expectedStart and curRuntime + period < datetime.today():
		curRuntime = expectedStart

	while curRuntime + period < expectedStart + timedelta(days=1) and curRuntime + period < datetime.today():
		curRuntime += period
	return curRuntime

def nextJobTime(curRuntime):
	if curRuntime.time() < runat and (curRuntime + period).time() > runat:
		ret = (curRuntime + period).replace(hour=runat.hour)
	else:
		ret = curRuntime + period
	return ret

def checkLastRun():
	#sets the next scheduled run time at end of function
	global scheduledJobTime
	if not os.path.isfile(LAST):
		print("No last run found; performing a backup now")
		doBackup()
	#last file now exists
	with open(LAST, "r") as f:
		lastTime = datetime.strptime(f.readline(), "%Y-%m-%d %H:%M")
		print("Running a backup at", config["runat"], "with a period of", config["period"])
		print("Last backup run:", lastTime)
		# expectedStart = datetime.combine(dt.date.today(), runat)
		# print("Last backup run:", lastTime)

		# curRuntime = expectedStart - timedelta(days=1)
		# while curRuntime + period < expectedStart and curRuntime + period < datetime.today():
		# 	curRuntime += period

		# if curRuntime + period > expectedStart and curRuntime + period < datetime.today():
		# 	curRuntime = expectedStart

		# while curRuntime + period < expectedStart + timedelta(days=1) and curRuntime + period < datetime.today():
		# 	curRuntime += period
		curRuntime = lastJobTime()

		missedLast = False
		print("Last scheduled run:", curRuntime)

		if lastTime < curRuntime:
			print("Missed last scheduled run!!!")
			missedLast = True

		if missedLast:
			print("Performing backup now")
			doBackup()
		else:
			print("On schedule; setting up next backup...")

		#return the next scheduled job runtime
		# if (curRuntime + period).time() > runat:
		# 	scheduledJobTime = (curRuntime + period).replace(hour=runat.hour)
		# else:
		# 	scheduledJobTime = curRuntime + period
		scheduledJobTime = nextJobTime(curRuntime)
		print("Scheduled job at:", scheduledJobTime)

def initializeGit():
	global gitPath
	#make dump folder
	gitPath = os.path.join(OUTPUT, config["database"])
	if not os.path.exists(gitPath):
		os.makedirs(gitPath)
	with cd(gitPath):
		subprocess.run(["git", "init"], stdout=subprocess.DEVNULL)
		#check if local username/email set
		try:
			print("Checking git user config ...", end=" ")
			subprocess.run(["grep", "\[user\]", ".git/config"], check=True, stdout=subprocess.DEVNULL)
			print("already set!")
		except subprocess.CalledProcessError:
			subprocess.run(["git", "config", "--local", "user.name", config["username"]])
			subprocess.run(["git", "config", "--local", "user.email", config["email"]])
			print("set name and email")

		try:
			print("Adding remote to", config["remote"], "...", end=" ")
			subprocess.run(["git", "remote", "add", "origin", config["remote"]], check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
			print("remote set!")
		except subprocess.CalledProcessError:
			print("remote already set")
	return (True, "Git initialized",)

def fetchLatest():
	#pull the latest commit
	with cd(gitPath):
		try:
			print("Fetching latest commit ...", end=" ")
			subprocess.run(["git", "fetch", "origin"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
			subprocess.run(["git", "checkout", config["branch"]], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
			print("done!")
			return (True, "Fetched latest commit",)
		except:
			return (False, "ERROR FETCHING LATEST COMMIT",)

def fetchCommit(commitId):
	#pull the specified commit
	with cd(gitPath):
		try:
			print("Fetching commit", commitId, end=" ")
			subprocess.run(["git", "checkout", commitId, "."], check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
			print("done!")
			return (True, "Fetched commit " + commitId,)
		except subprocess.CalledProcessError:
			print("Invalid commit ID; aborting")
			return (False, "ERROR FETCHING COMMIT " + commitId,)

def doBackup():
	backupLock.acquire()

	try:
		resp = initializeGit()
		if not resp[0]:
			return resp[1]
		resp = fetchLatest()
		if not resp[0]:
			return resp[1]

		print("Dumping ...", end=" ")
		subprocess.run(["mongodump", "--host", config["mongodb-ip"], "--port", config["mongodb-port"], "--db", config["database"], "--out", OUTPUT], check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
		print("OK")

		changed = True
		with cd(gitPath):
			subprocess.run(["git", "pull"], stdout=subprocess.DEVNULL)

			print("Adding to git ...", end=" ")
			subprocess.run(["git", "add", "-A"], check=True, stdout=subprocess.DEVNULL)
			print("OK")

			print("Git status:", end=" ")
			status = subprocess.run(["git", "status"], check=True, stdout=subprocess.PIPE)

			if b"Changes to be committed" in status.stdout:
				print("changes detected, so pushing")
				subprocess.run(["git", "commit", "-m", str(datetime.now())], check=True, stdout=subprocess.DEVNULL)
				subprocess.run(["git", "push", "-u", "origin", config["branch"]], check=True, stdout=subprocess.DEVNULL)
			elif b"No commits yet" in status.stdout:
				print("no commits yet, so pushing")
				subprocess.run(["git", "commit", "-m", str(datetime.now())], check=True, stdout=subprocess.DEVNULL)
				subprocess.run(["git", "push", "-u", "origin", config["branch"]], check=True, stdout=subprocess.DEVNULL)
			elif b"Your branch is up to date" in status.stdout:
				print("no changes detected, marking as complete")
				changed = False
			else:
				print("unknown status")
				print(status.stdout)
				print()
		#update last run
		with open(LAST, "w") as f:
			f.write(datetime.now().strftime("%Y-%m-%d %H:%M"))

		#return status so when called from client can respond
		if changed:
			return "Backup done"
		else:
			return "Backup performed, no changes"
	finally:
		backupLock.release()

def doRestore(commitId):
	resp = initializeGit()
	if not resp[0]:
		return resp[1]
	if commitId == "latest":
		resp = fetchLatest()
	else:
		resp = fetchCommit(commitId)
	if not resp[0]:
		return resp[1]

	with cd(gitPath):
		try:
			subprocess.run(["mongorestore", "--db", config["database"], "--drop", "."], check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
			subprocess.run(["git", "reset", "--hard"], check=True, stdout=subprocess.DEVNULL)
		except subprocess.CalledProcessError:
			print("Error restoring commit")
			return "Restore failed"
	#return status so when called from client can respond
	return "Restore done"

def doReload():
	global scheduledJobTime
	resp = configure()
	if not resp[0]:
		return resp[1]

	#the scheduler has no jobs; add new jobs and start it again
	#recompute the scheduledJobTime
	# expectedStart = datetime.combine(dt.date.today(), runat)
	# curRuntime = expectedStart - timedelta(days=1)
	# while curRuntime + period < expectedStart and curRuntime + period < datetime.today():
	# 	curRuntime += period
	# if curRuntime + period > expectedStart and curRuntime + period < datetime.today():
	# 		curRuntime = expectedStart
	# while curRuntime + period < expectedStart + timedelta(days=1) and curRuntime + period < datetime.today():
	# 	curRuntime += period
	curRuntime = lastJobTime()
	scheduledJobTime = nextJobTime(curRuntime)
	print("Scheduled job at:", scheduledJobTime)
	#add the job and compute the next
	computeNextJob()
	#notify the scheduler
	schedEvent.set()
	return "Reload OK"

def listenerManagerThread(stopFlag):
	while not stopFlag.is_set():
		with listener.accept() as conn:
			print()
			print("New client connection:", listener.last_accepted)
			msg = conn.recv()	#receives an array of string messages
			print("DEBUG MSG:", msg)
			if msg[0] == "shutdown":
				print("Shutting down...")
				conn.send(["Shutdown OK"])
				stopFlag.set()
				#cancel all backup events
				for event in scheduler.queue:
					scheduler.cancel(event)
				schedEvent.set()
				conn.close()
				listener.close()
			elif msg[0] == "backup":
				print("Performing backup on request")
				conn.send([doBackup()])
			elif msg[0] == "restore":
				print("Performing restore with commit:", msg[1])
				conn.send([doRestore(msg[1])])
			elif msg[0] == "reload":
				print("Reloading configuration")
				conn.send([doReload()])
			else:
				print("Invalid command")
				conn.send(["Invalid command"])
				conn.close()
			print()

def computeNextJob():
	global scheduledJobTime
	print("Job added for execution at:", scheduledJobTime)
	scheduler.enterabs(time.mktime(scheduledJobTime.timetuple()), 0, doBackupJob)
	# if (scheduledJobTime + period).time() > runat:
	# 	scheduledJobTime = (scheduledJobTime + period).replace(hour=runat.hour)
	# else:
	# 	scheduledJobTime += period
	scheduledJobTime = nextJobTime(scheduledJobTime)
	print("Next job scheduled for:", scheduledJobTime)

def doBackupJob():
	print("Executing backup job")
	doBackup()
	computeNextJob()


def schedulerThread(stopFlag):
	interrupted = False
	while not stopFlag.is_set():
		print(len(scheduler.queue))
		for e in scheduler.queue:
			print(e)
		delay = scheduler.run(False)
		print("delay:", delay)
		if delay is None:
			time.sleep(1)
			continue
		interrupted = schedEvent.wait(delay)
		if interrupted:
			print("interrupted!")
			print(len(scheduler.queue))
			for e in scheduler.queue:
				print(e)
			schedEvent.clear()
		print()

if __name__ == "__main__":
	if not configure()[0]:
		print("Bad configuration file")
		sys.exit()

	#check when last run; if missed then try immediately
	checkLastRun()
	#setup listener socket in another thread
	stopFlag = Event()
	listenerThread = Thread(target=listenerManagerThread, args=(stopFlag,))
	listenerThread.start()

	computeNextJob()
	schedThread = Thread(target=schedulerThread, args=(stopFlag,))
	schedThread.start()

	schedThread.join()

	listenerThread.join()
	print("Stopped")
