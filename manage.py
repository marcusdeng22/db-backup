import argparse
import sys

from multiprocessing.connection import Client

'''
supported modes:
backup: do a backup now
restore: restore with the latest data
restore <commit id>: restore with the commit data
reload: reload the current config
shutdown: stop the auto backup process
'''

parser = argparse.ArgumentParser(description="Manage the backup process.")
parser.add_argument("--backup", action="store_const", const=True, required=False, help="do a backup now")
parser.add_argument("--restore", action="store", nargs="?", const="latest", type=str, required=False, help="restore with the latest or specified commit", metavar="commit ID")
parser.add_argument("--reload", action="store_const", const=True, required=False, help="reload the configuration")
parser.add_argument("--shutdown", action="store_const", const=True, required=False, help="shutdown the backup process")

args = parser.parse_args()
cmd = {k: v for k, v in vars(args).items() if v is not None}

if len(cmd) == 0:
	parser.print_help()
	sys.exit()
else:
	#create the multiproc client
	try:
		client = Client(("localhost", 9999,), authkey=b"test")
		#select on the command
		if "backup" in cmd:
			print("doing backup")
			client.send(["backup"])
			#block for response
			print("Response:", client.recv()[0])
		elif "restore" in cmd:
			print("doing restore")
			if cmd["restore"] == "latest":
				print("restoring with latest")
			else:
				print("restoring with commit:", cmd["restore"])
			client.send(["restore", cmd["restore"]])
			print("Response:", client.recv()[0])
		elif "reload" in cmd:
			print("doing reload")
			client.send(["reload"])
			print("Response:", client.recv()[0])
		elif "shutdown" in cmd:
			print("shutting down")
			client.send(["shutdown"])
			print("Response:", client.recv()[0])
		else:
			parser.print_help()
			sys.exit()
		#close the client
		client.close()
	except:
		print("Backup service is not running; please start it")