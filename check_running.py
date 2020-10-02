import subprocess
import os, time
from datetime import datetime

def main(process_name, restart_command):
    try:
        process = subprocess.Popen(['ps', 'aux'],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE)
        stdout, stderr = process.communicate()
        if str(stdout).find(process_name) == -1:
            os.system(restart_command)
            print("Restarted process {} at timestamp {}.".format(process_name, datetime.now().time()))
    except Exception as e:
        print(e)


if __name__=="__main__":
    while True:
        main("python3 Streaming.py","./execute_streaming.sh")
        time.sleep(120)
