# autorclone v2.0.0
#
# Author Telegram https://t.me/CodyDoby
# Inbox  codyd@qq.com
#
# can copy from
# - [x] publicly shared folder to Team Drive
# - [x] Team Drive to Team Drive
# - [ ] publicly shared folder to publicly shared folder (with write privilege)
# - [ ] Team Drive to publicly shared folder
#   `python3 .\rclone_sa_magic.py -s SourceID -d DestinationID -dp DestinationPathName -b 10`
#
# - [x] local to Team Drive
# - [ ] local to private folder
# - [ ] private folder to any (think service accounts cannot do anything about private folder)
#
from __future__ import print_function
import argparse
import glob
import json
import os, io
import platform
import subprocess
import sys
import time
import shutil
from signal import signal, SIGINT
from dotenv import load_dotenv

# import distutils.spawn # deprecated, will be removed in python3.12
# https://docs.python.org/3/library/distutils.html

load_dotenv()
# =================modify here=================
logfile = "log_rclone.txt"  # log file: tail -f log_rclone.txt
PID = 0

# parameters for this script
SIZE_GB_MAX = 650  # if one account has already copied 650GB, switch to next account
CNT_DEAD_RETRY = 100  # if there is no files be copied for 100 times, switch to next account
CNT_SA_EXIT = 4  # if continually switch account for 4 times stop script

# change it when u know what are u doing
# paramters for rclone.
# If TPSLIMITxTRANSFERS is too big, will cause 404 user rate limit error,
# especially for tasks with a lot of small files
TPSLIMIT = 5 # Default 5
TRANSFERS = 5 # Default 5

CLIENT_ID = os.environ.get('CLIENT_ID')
CLIENT_SECRET = os.environ.get('CLIENT_SECRET')
TOKEN = os.environ.get('TOKEN')
# =================modify here=================


# Returns True if the current operating system is Windows, False otherwise
def is_windows():
    return platform.system() == 'Windows'

# Signal handler function that kills a process with the specified PID
def handler(signal_received, frame):
    global PROCESS_ID

    # Construct the appropriate kill command based on the operating system
    if is_windows():
        kill_cmd = 'taskkill /PID {} /F'.format(PROCESS_ID)
    else:
        kill_cmd = "kill -9 {}".format(PROCESS_ID)

    try:
        # Print the current time and execute the kill command
        print("\n" + " " * 20 + " {}".format(time.strftime("%H:%M:%S")))
        subprocess.check_call(kill_cmd, shell=True)
    except:
        # Ignore any errors that occur while trying to kill the process
        pass

    # Exit the script with a status of 0 (success)
    sys.exit(0)

# Parses command-line arguments and returns an object containing the arguments
def parse_args():
    # Create an argument parser with a description of what the script does
    parser = argparse.ArgumentParser(description="Copy files from a source folder to a destination folder in Google Drive, using rclone.")

    # Add command-line arguments to the parser
    parser.add_argument('-s', '--source_id', type=str,
                        help='the ID of the source folder. This can be a Team Drive ID or a publicly shared folder ID.')
    parser.add_argument('-d', '--destination_id', type=str, required=True,
                        help='the ID of the destination folder. This can be a Team Drive ID or a publicly shared folder ID.')

    parser.add_argument('-sp', '--source_path', type=str, default="",
                        help='the folder path of the source folder. This can be a path in Google Drive or a local path.')
    parser.add_argument('-dp', '--destination_path', type=str, default="",
                        help='the folder path of the destination folder. This must be a path in Google Drive.')

    # If the source path contains special characters, you can specify its folder ID instead
    parser.add_argument('-spi', '--source_path_id', type=str, default="",
                        help='the folder ID (not the name) of the source folder. This should be a path in Google Drive.')

    parser.add_argument('-sa', '--service_account', type=str, default="accounts",
                        help='the folder path of JSON files for service accounts.')
    parser.add_argument('-cp', '--check_path', action="store_true",
                        help='check the source and destination paths.')

    parser.add_argument('-p', '--port', type=int, default=5572,
                        help='the port to run rclone remote control. Use a different port for each instance of rclone.')

    parser.add_argument('-b', '--begin_sa_id', type=int, default=1,
                        help='the beginning ID of the service account to use for the source folder.')
    parser.add_argument('-e', '--end_sa_id', type=int, default=600,
                        help='the ending ID of the service account to use for the destination folder.')

    parser.add_argument('-c', '--rclone_config_file', type=str,
                        help='the path of the rclone config file.')
    parser.add_argument('-test', '--test_only', action="store_true",
                        help='for testing purposes: make rclone print more information.')
    parser.add_argument('-t', '--dry_run', action="store_true",
                        help='for testing purposes: make rclone perform a dry run (no files are actually copied).')

    parser.add_argument('--disable_list_r', action="store_true",
                        help='for debugging purposes: do not use this.')

    parser.add_argument('--crypt', action="store_true",
                        help='for testing purposes: encrypt the destination folder.')

    parser.add_argument('--cache', action="store_true",
                        help="for testing purposes: cache the destination folder.")

    # Parse the command-line arguments and return the result
    args = parser.parse_args()
    return args

# This function generates a rclone configuration file based on the input arguments
def gen_rclone_cfg(args):
    # Find all .json files in the service account folder
    sa_files = glob.glob(os.path.join(args.service_account, '*.json'))
    # Output file path for the rclone configuration
    output_of_config_file = './rclone.conf'

    # If no json files found in the service account folder, exit the script
    if len(sa_files) == 0:
        sys.exit('No json files found in ./{}'.format(args.service_account))

    # Open the output file and write the configuration
    with open(output_of_config_file, 'w') as fp:
        # Loop over each json file found in the service account folder
        for i, filename in enumerate(sa_files):

            # Get the directory path of the current file
            dir_path = os.path.dirname(os.path.realpath(__file__))
            # Join the directory path and filename to get the full path
            filename = os.path.join(dir_path, filename)
            # Replace the os separator with forward slash
            filename = filename.replace(os.sep, '/')

            # For source
            if args.source_id:
                # For team drive only
                if len(args.source_id) == 33:
                    folder_or_team_drive_src = 'root_folder_id'
                elif len(args.source_id) == 19:
                    folder_or_team_drive_src = 'team_drive'
                else:
                    sys.exit('Wrong length of team_drive_id or publicly shared root_folder_id')
                
                # Write the text for the source configuration to the output file
                text_to_write = "[{}{:03d}]\n" \
                                "type = drive\n" \
                                "scope = drive\n" \
                                "token = {}\n" \
                                "client_id ={}\n" \
                                "client_secret = {}\n" \
                                "{} = {}\n".format('src', i + 1, TOKEN, CLIENT_ID, CLIENT_SECRET,folder_or_team_drive_src, args.source_id)

                # use path id instead path name
                if args.source_path_id:
                    # for team drive only
                    if len(args.source_id) == 19:
                        if len(args.source_path_id) == 33:
                            text_to_write += 'root_folder_id = {}\n'.format(args.source_path_id)
                        else:
                            sys.exit('Wrong length of source_path_id')
                    else:
                        sys.exit('For publicly shared folder please do not set -spi flag')

                text_to_write += "\n"

                try:
                    fp.write(text_to_write)
                except:
                    sys.exit("failed to write {} to {}".format(args.source_id, output_of_config_file))
            else:
                pass

            # For destination
            # Determine whether the destination is a root folder id or a team drive id
            if len(args.destination_id) == 33:
                folder_or_team_drive_dst = 'root_folder_id'
            elif len(args.destination_id) == 19:
                folder_or_team_drive_dst = 'team_drive'
            else:
                sys.exit('Wrong length of team_drive_id or publicly shared root_folder_id')

            # Create the text to write for the destination configuration
            try:
                fp.write('[{}{:03d}]\n'
                         'type = drive\n'
                         'scope = drive\n'
                         'client_id = {}\n'
                         'client_secret = {}\n'
                         'service_account_file = {}\n'
                         '{} = {}\n\n'.format('dst', i + 1, CLIENT_ID, CLIENT_SECRET, filename, folder_or_team_drive_dst, args.destination_id))
            except:
                sys.exit("failed to write {} to {}".format(args.destination_id, output_of_config_file))

            # For crypt destination
            if args.crypt:
                remote_name = '{}{:03d}'.format('dst', i + 1)
                try:
                    fp.write('[{}_crypt]\n'
                             'type = crypt\n'
                             'remote = {}:\n'
                             'filename_encryption = standard\n'
                             'password = hfSJiSRFrgyeQ_xNyx-rwOpsN2P2ZHZV\n'
                             'directory_name_encryption = true\n\n'.format(remote_name, remote_name))
                except:
                    sys.exit("failed to write {} to {}".format(args.destination_id, output_of_config_file))

            # For cache destination
            if args.cache:
                remote_name = '{}{:03d}'.format('dst', i + 1)
                try:
                    fp.write('[{}_cache]\n'
                             'type = cache\n'
                             'remote = {}:\n'
                             'chunk_total_size = 1G\n\n'.format(remote_name, remote_name))
                except:
                    sys.exit("failed to write {} to {}".format(args.destination_id, output_of_config_file))

    return output_of_config_file, i


def print_during(time_start):
    
    # get the current time
    time_stop = time.time()
    
    # calculate the elapsed time in hours, minutes, and seconds
    hours, rem = divmod((time_stop - time_start), 3600)
    minutes, sec = divmod(rem, 60)

    # print the elapsed time in the format "HH:MM:SS.ss"
    print("Elapsed Time: {:0>2}:{:0>2}:{:05.2f}".format(int(hours), int(minutes), sec))


def check_rclone_program():
    # Check if running on Windows and add .exe extension to rclone program name
    rclone_prog = 'rclone'
    if is_windows():
        rclone_prog += ".exe"

    # Find the path to rclone program
    rclone_path = shutil.which(rclone_prog)

    # If rclone program is not found, exit with an error message
    if rclone_path is None:
        sys.exit("Please install rclone firstly: https://rclone.org/downloads/")

    # Return the path to rclone program if found
    return rclone_path


def check_path(path):
    """run rclone command to check the size of the path
        using the configuration file 'rclone.conf'
        and disabling the ListR mode to speed up the operation
    """
    try:
        ret = subprocess.check_output('rclone --config {} --disable ListR size \"{}\"'.format('rclone.conf', path),
                                      shell=True)
        # if the command ran successfully, print the output
        print('It is okay:\n{}'.format(ret.decode('utf-8').replace('\0', '')))
    
    except subprocess.SubprocessError as error:
        # if the command failed, exit the program with the error message
        sys.exit(str(error))


def main():
    
    # Set signal handler for interrupt (SIGINT) signal
    signal(SIGINT, handler)

    # Check if rclone is installed
    ret = check_rclone_program()
    print("rclone is detected: {}".format(ret))
    
    # Parse command-line arguments
    args = parse_args()

    # Set the start and end IDs for the service accounts to be used on uploads.
    id = args.begin_sa_id
    end_id = args.end_sa_id

    # If no rclone config file is specified, generate one
    config_file = args.rclone_config_file
    if config_file is None:
        print('generating rclone config file.')
        config_file, end_id = gen_rclone_cfg(args)
        print('rclone config file generated.')
    else:
        return print('not supported yet.')
        pass
        # need parse labels from config files
    
    # Record the start time
    time_start = time.time()
    print("Start: {}".format(time.strftime("%H:%M:%S")))

    cnt_acc_error = 0
    while id <= end_id + 1:

        if id == end_id + 1:
            break
            # id = 1

    # Create a file named 'current_sa.txt' and write the current ID to it
        with io.open('current_sa.txt', 'w', encoding='utf-8') as fp:
            fp.write(str(id) + '\n')

    # Set the source and destination labels based on the current ID and whether encryption and caching are enabled
        src_label = "src" + "{0:03d}".format(id) + ":"
        dst_label = "dst" + "{0:03d}".format(id) + ":"
        if args.crypt:
            dst_label = "dst" + "{0:03d}_crypt".format(id) + ":"

        if args.cache:
            dst_label = "dst" + "{0:03d}_cache".format(id) + ":"
    
    # Set the full source path based on whether a source ID is specified or not
        src_full_path = src_label + args.source_path
        if args.source_id is None:
            src_full_path = args.source_path

    # Set the full destination path based on whether a destination ID is specified or not
        dst_full_path = dst_label + args.destination_path
        if args.destination_id is None:
            dst_full_path = args.destination_path

    # Print the source and destination paths if test mode is enabled
        if args.test_only:
            print('\nsrc full path\n', src_full_path)
            print('\ndst full path\n', dst_full_path, '\n')
    
    # Check the source and destination paths if path checking is enabled and this is the first SA ID
        if args.check_path and id == args.begin_sa_id:
            print("Please wait. Checking source path...")
            check_path(src_full_path)

            print("Please wait. Checking destination path...")
            check_path(dst_full_path)

        # =================cmd to run=================

        # Construct the rclone command to run
        rclone_cmd = "rclone copy --config {} ".format(config_file)
        if args.dry_run:
           rclone_cmd += "--dry-run "

        # ================= edit below if needed =================
        # edit here to add more flags for rclone command !
        rclone_cmd += "--fast-list --drive-server-side-across-configs --rc --rc-addr=\"localhost:{}\" --low-level-retries 1 -vv --ignore-existing --checkers 10 --progress ".format(args.port)
        rclone_cmd += "--tpslimit {} --transfers {} --drive-chunk-size 256M ".format(TPSLIMIT, TRANSFERS)
        if args.disable_list_r:
            rclone_cmd += "--disable ListR "
        rclone_cmd += "--drive-acknowledge-abuse --log-file={} \"{}\" \"{}\"".format(logfile, src_full_path,
                                                                                     dst_full_path)
        
    # Add an '&' to the end of the rclone command if the operating system is not Windows, otherwise add 'start /b'
        if not is_windows():
            rclone_cmd = rclone_cmd + " &"
        else:
            rclone_cmd = "start /b " + rclone_cmd
        # =================cmd to run=================
        # Print the rclone command

        print(rclone_cmd)

    # Attempt to run the rclone command in the shell
        try:
            subprocess.check_call(rclone_cmd, shell=True)

        # Print message and sleep for 10 seconds if successful
            print(">> Let us go {} {}".format(dst_label, time.strftime("%H:%M:%S")))
            time.sleep(5)

    # If there's an error, print the error message and return
        except subprocess.SubprocessError as error:
            return print("error: " + str(error))

    # Initialize some counters and flags
        cnt_error = 0
        cnt_dead_retry = 0
        size_bytes_done_before = 0
        cnt_acc_sucess = 0
        already_start = False

    # Try to get the PID of the rclone process running on the specified port
        try:
            response = subprocess.check_output('rclone rc --rc-addr="localhost:{}" core/pid'.format(args.port), shell=True)
        
        # Extract the PID from the response and convert it to an integer
            pid = json.loads(response.decode('utf-8').replace('\0', ''))['pid']
        
        # Print the PID if test_only flag is set
            if args.test_only: print('\npid is: {}\n'.format(pid))

        # Set the global variable PID to the extracted PID
            global PID
            PID = int(pid)

   # If there's an error, do nothing (i.e. continue with the script)
        except subprocess.SubprocessError as error:
            pass

    # A while loop to constantly check for the status of the rclone task
        while True:

        # Command to get rclone stats using rclone remote control
            rc_cmd = 'rclone rc --rc-addr="localhost:{}" core/stats'.format(format(args.port))
            
            try:
            # Get the response from running the command
                response = subprocess.check_output(rc_cmd, shell=True)
            # Increment counter for successful responses
                cnt_acc_sucess += 1
            # Reset error counter if there were multiple successful responses after a long waiting time
                cnt_error = 0
                
                """
                Reset error counter if there were multiple successful responses after a long waiting time
                if there is a long time waiting, this will be easily satisfied, so check if it is started using
                already_started flag
                """

                if already_start and cnt_acc_sucess >= 9:
                    cnt_acc_error = 0
                    cnt_acc_sucess = 0
                    if args.test_only: print(
                        "total 9 times success. the cnt_acc_error is reset to {}\n".format(cnt_acc_error))

            except subprocess.SubprocessError as error:
                # Continually increase error counter until a certain threshold
                # continually ...
                cnt_error = cnt_error + 1
                cnt_acc_error = cnt_acc_error + 1
                if cnt_error >= 3:
                    cnt_acc_sucess = 0
                
                    if args.test_only: print(
                        "total 3 times failure. the cnt_acc_sucess is reset to {}\n".format(cnt_acc_sucess))

                # If the error threshold is reached, assume the task is finished and break the loop
                    print('No rclone task detected (possibly done for this '
                          'account). ({}/3)'.format(int(cnt_acc_error / cnt_error)))
                    
                    # Regard continually exit as *all done*.
                    if cnt_acc_error >= 9:
                        print('All done (3/3).')
                        print_during(time_start)
                        return
                    break
                continue
            
        # Process the response and extract relevant data
            response_processed = response.decode('utf-8').replace('\0', '')
            response_processed_json = json.loads(response_processed)
            size_bytes_done = int(response_processed_json['bytes'])
            checks_done = int(response_processed_json['checks'])
            size_GB_done = int(size_bytes_done * 9.31322e-10)
            speed_now = float(int(response_processed_json['speed']) * 9.31322e-10 * 1024)

            # try:
            #     print(json.loads(response.decode('utf-8')))
            # except:
            #     print("have some encoding problem to print info")
            
        
        # Comment is removed as peer now the script prints all the progress stats automatically, but for tracing the code remains here.
            """
            if already_start:
                print("\n%s %dGB Done @ %fMB/s | checks: %d files" % (dst_label, size_GB_done, speed_now, checks_done), end="\r")
            else:
                print("\n%s reading source/destination | checks: %d files" % (dst_label, checks_done), end="\r")
            """
            # continually no ...
            if size_bytes_done - size_bytes_done_before == 0:

            # If there has been no increase in the amount of data transferred since the last check and the job has already started
                if already_start:

            # Increase the count of times there has been no increase in data transferred
                    cnt_dead_retry += 1

            # If the script is in test mode, print some debugging information
                    if args.test_only:
                        print('\nsize_bytes_done', size_bytes_done)
                        print('size_bytes_done_before', size_bytes_done_before)
                        print("No. No size increase after job started.")
       
            else:
            # If there has been an increase in the amount of data transferred, reset the count of times there has been no increase        
                cnt_dead_retry = 0

            # If the script is in test mode, print some debugging information
                if args.test_only: print("\nOk. I think the job has started")

            # Mark the job as having started
                already_start = True

        # Remember the amount of data transferred for the next check
            size_bytes_done_before = size_bytes_done

            # Stop by error (403, etc) info
            if size_GB_done >= SIZE_GB_MAX or cnt_dead_retry >= CNT_DEAD_RETRY:
   
             # If the amount of data transferred exceeds the maximum size or there have been too many consecutive checks with no increase in data transferred:
                if is_windows():
                    # kill_cmd = 'taskkill /IM "rclone.exe" /F'
                    kill_cmd = 'taskkill /PID {} /F'.format(PID)
                else:

                # If the script is running on Linux, use the 'kill' command to terminate the rclone process
                    kill_cmd = "kill -9 {}".format(PID)
                
                # Print the current time
                print("\n" + " " * 20 + " {}".format(time.strftime("%H:%M:%S")))
                try:
                    subprocess.check_call(kill_cmd, shell=True)

                # Run the kill command to terminate the rclone process
                    print('\n')
                except:
                
                # If the kill command fails, print an error message (if in test mode) and continue
                    if args.test_only: print("\nFailed to kill.")
                    pass

                # =================Finish it=================
                if cnt_dead_retry >= CNT_DEAD_RETRY:

                # If there have been too many consecutive checks with no increase in data transferred:
                    try:
                    # Increase the count of times the script has exited due to this reason
                        cnt_exit += 1
                    except:
                    # If cnt_exit hasn't been initialized yet, set it to 1
                        cnt_exit = 1
            
                    # If the script is in test mode, print some debugging information
                    if args.test_only: print(
                        "1 more time for long time waiting. the cnt_exit is added to {}\n".format(cnt_exit))
                else:
                    # clear cnt if there is one time
                    cnt_exit = 0
                    if args.test_only: print("1 time sucess. the cnt_exit is reset to {}\n".format(cnt_exit))

                # Regard continually exit as *all done*.
                if cnt_exit >= CNT_SA_EXIT:
                
                # If the script has exited due to this reason enough times, print the duration of the job and a completion message and exit
                    print_during(time_start)
                    # exit directly rather than switch to next account.
                    print('All Done.')
                    return
                # =================Finish it=================

                break

        # wait for 2 seconds before checking the job progress again
            time.sleep(4)

    # increment the id of the job being processed
        id = id + 1

    # print the time taken to complete the job
    print_during(time_start)


if __name__ == "__main__":
    main()
