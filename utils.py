from queue import Queue
import select
import socket
from threading import Thread
import time
import traceback

def run_thread(fn, args):
    try:
        my_thread = Thread(target=fn, args=args)
        my_thread.daemon = True
        my_thread.start()
        return my_thread
    except (KeyboardInterrupt, SystemExit):
        pass


