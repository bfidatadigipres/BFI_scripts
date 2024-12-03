#!/usr/bin/env python3

'''
Parse `global.log` and report on files with outstanding
WARNING alerts issued for the given day.

2022
'''

# Python library imports
import os
import sys
import csv
import shutil
import datetime
csv.field_size_limit(10000000)

# Local imports
sys.path.append(os.environ['CODE'])
import utils

# Date variable for use in ordering error outputs
TODAY = datetime.date.today()
YEST = TODAY - datetime.timedelta(days=1)
YEST2 = TODAY - datetime.timedelta(days=2)
DATE_VAR = YEST.strftime('%Y-%m-%d')
DATE_VAR2 = YEST2.strftime('%Y-%m-%d')
LOGS = os.environ['LOG_PATH']
CONTROL_JSON = os.path.join(LOGS, 'downtime_control.json')
GLOBAL_LOG = os.path.join(LOGS, 'autoingest/global.log')
CURRENT_ERROR_FOLD = os.environ['CURRENT_ERRORS']
CURRENT_ERRORS = os.path.join(CURRENT_ERROR_FOLD, 'current_errors.csv')
CURRENT_ERRORS_NEW = os.path.join(CURRENT_ERROR_FOLD, 'current_errors_new.csv')


def main():
    '''
    For standalone use of log_parser
    not, launched from autoingest
    '''
    if not utils.check_control('autoingest'):
        sys.exit('Script run prevented by downtime_control.json. Script exiting.')
    create_current_errors_logs()


def create_current_errors_logs():
    '''
    Parse global.log entries
    '''
    data = {}
    with open(GLOBAL_LOG, 'r') as file:
        rows = csv.reader(file, delimiter='\n')
        for row in rows:
            row = row[0].split('\t')
            print(row)
            # Temp addition to reduce current_errors.csv
            if 'MD5 checksum does not yet exist for this file.' in str(row):
                continue
            try:
                timedate = row[0]
                local_p = row[2]
                remote_p = row[3]
                status = row[1]
                file_ = row[4]
                message = row[5]
            except (IndexError, KeyError):
                continue
            print(timedate, status, local_p, remote_p, file_, message)
            if '.tmp' in file_ or '.ini' in file_ or '.DS_Store' in file_:
                continue

            # Add items from today only that have WARNING status file still in path
            if timedate.startswith(DATE_VAR) and 'WARNING' in status:
                print(f"File exists in date range with 'WARNING', adding to dictionary: {file}")
                # Aggregate all messages for select files.
                if file_ in data:
                    data[file_][timedate] = (status, message, local_p, remote_p)
                else:
                    data[file_] = {timedate : (status, message, local_p, remote_p)}
            elif timedate.startswith(DATE_VAR2) and 'WARNING' in status:
                print(f"File exists in date range with 'WARNING', adding to dictionary: {file}")
                # Aggregate all messages for select files.
                if file_ in data:
                    data[file_][timedate] = (status, message, local_p, remote_p)
                else:
                    data[file_] = {timedate : (status, message, local_p, remote_p)}

    print(data)
    append_rows = []
    for file_ in data.items():
        # This section removes duplicates entries, writing just last entry to csv
        latest_timedate = sorted(data[file_[0]].keys())[-1]
        latest_message = data[file_[0]][latest_timedate]
        (status, message, local_p, remote_p) = latest_message

        # Remove non-files, like .tmp, .ini and .DS_Store
        if '.tmp' in file_ or '.ini' in file_ or '.DS_Store' in file_:
            pass
        else:
            print(f'* Adding {local_p} to error log')
            local_p2 = local_p.replace('/', ' | ')
            local_p2 = local_p2.lstrip(' | ')
            append_rows.append((latest_timedate[:16], local_p2, file_[0], message))

    if append_rows:
        append_rows.sort(reverse=True)
        print("* Creating CSV file current_errors.csv in current_errors folder...")
        with open(CURRENT_ERRORS, 'w') as of:
            writer = csv.writer(of)
            writer.writerow(['timedate', 'path', 'file', 'message'])
            for ar in append_rows:
                writer.writerow(ar)
    else:
        print('* No files still exist where status = WARNING, so nothing to add to error log this time...')
        with open(CURRENT_ERRORS_NEW, 'w+') as of:
            of.write('No files where status = WARNING and still_exists = True, so no error logs to report this time...')
        shutil.move(CURRENT_ERRORS_NEW, CURRENT_ERRORS)

    print('* Creating versions of error log in all in-scope autoingest NAS shares')
    if os.path.exists(os.environ['AUTOINGEST_EDITSHARE']):
        shutil.copy(CURRENT_ERRORS, os.path.join(os.environ['AUTOINGEST_EDITSHARE'], 'current_errors/current_errors.csv'))
    if os.path.exists(os.environ['AUTOINGEST_H22']):
        shutil.copy(CURRENT_ERRORS, os.path.join(os.environ['AUTOINGEST_H22'], 'current_errors/current_errors.csv'))
    if os.path.exists(os.environ['AUTOINGEST_QNAP01']):
        shutil.copy(CURRENT_ERRORS, os.path.join(os.environ['AUTOINGEST_QNAP01'], 'current_errors/current_errors.csv'))
    if os.path.exists(os.environ['AUTOINGEST_QNAP02']):
        shutil.copy(CURRENT_ERRORS, os.path.join(os.environ['AUTOINGEST_QNAP02'], 'current_errors/current_errors.csv'))
    if os.path.exists(os.environ['AUTOINGEST_QNAP03']):
        shutil.copy(CURRENT_ERRORS, os.path.join(os.environ['AUTOINGEST_QNAP03'], 'current_errors/current_errors.csv'))
    if os.path.exists(os.environ['AUTOINGEST_QNAP04']):
        shutil.copy(CURRENT_ERRORS, os.path.join(os.environ['AUTOINGEST_QNAP04'], 'current_errors/current_errors.csv'))
    if os.path.exists(os.environ['AUTOINGEST_QNAP05']):
        shutil.copy(CURRENT_ERRORS, os.path.join(os.environ['AUTOINGEST_QNAP05'], 'current_errors/current_errors.csv'))
    if os.path.exists(os.environ['AUTOINGEST_QNAP06']):
        shutil.copy(CURRENT_ERRORS, os.path.join(os.environ['AUTOINGEST_QNAP06'], 'current_errors/current_errors.csv'))
    if os.path.exists(os.environ['AUTOINGEST_QNAP07']):
        shutil.copy(CURRENT_ERRORS, os.path.join(os.environ['AUTOINGEST_QNAP07'], 'current_errors/current_errors.csv'))
    if os.path.exists(os.environ['AUTOINGEST_QNAP08']):
        shutil.copy(CURRENT_ERRORS, os.path.join(os.environ['AUTOINGEST_QNAP08'], 'current_errors/current_errors.csv'))
    if os.path.exists(os.environ['AUTOINGEST_QNAP09']):
        shutil.copy(CURRENT_ERRORS, os.path.join(os.environ['AUTOINGEST_QNAP09'], 'current_errors/current_errors.csv'))
    if os.path.exists(os.environ['AUTOINGEST_QNAP10']):
        shutil.copy(CURRENT_ERRORS, os.path.join(os.environ['AUTOINGEST_QNAP10'], 'current_errors/current_errors.csv'))
    if os.path.exists(os.environ['AUTOINGEST_QNAP11']):
        shutil.copy(CURRENT_ERRORS, os.path.join(os.environ['AUTOINGEST_QNAP11'], 'current_errors/current_errors.csv'))
    if os.path.exists(os.environ['AUTOINGEST_QNAP08_OSH']):
        shutil.copy(CURRENT_ERRORS, os.path.join(os.environ['AUTOINGEST_QNAP08_OSH'], 'current_errors/current_errors.csv'))
    if os.path.exists(os.environ['AUTOINGEST_QNAP_TEMP']):
        shutil.copy(CURRENT_ERRORS, os.path.join(os.environ['AUTOINGEST_QNAP_TEMP'], 'current_errors/current_errors.csv'))

if __name__ == '__main__':
    main()
