from google.oauth2.service_account import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from argparse import ArgumentParser
from os.path import exists
from json import loads
from glob import glob
import pickle

successful = []


def _is_success(id, resp, exception):
    global successful

    if exception is None:
        successful.append(resp['emailAddress'])


def masshare(drive_id=None, path='accounts', token='token.pickle', credentials='credentials.json'):
    global successful

    SCOPES = ["https://www.googleapis.com/auth/drive",
              "https://www.googleapis.com/auth/cloud-platform",
              "https://www.googleapis.com/auth/iam"]
    creds = None

    if exists(token):
        with open(token, 'rb') as t:
            creds = pickle.load(t)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                credentials, SCOPES)
            creds = flow.run_local_server(port=0)
        with open(token, 'wb') as t:
            pickle.dump(creds, t)

    drive = build("drive", "v3", credentials=creds)

    accounts_to_add = []

    print('Fetching emails')
    for i in glob('%s/*.json' % path):
        accounts_to_add.append(loads(open(i, 'r').read())['client_email'])

    while len(successful) < len(accounts_to_add):
        print('Preparing %d members' % (len(accounts_to_add) - len(successful)))
        batch = drive.new_batch_http_request(callback=_is_success)
        for i in accounts_to_add:
            if i not in successful:
                batch.add(drive.permissions().create(fileId=drive_id, fields='emailAddress', supportsAllDrives=True, body={
                    "role": "fileOrganizer",
                    "type": "user",
                    "emailAddress": i
                }))
        print('Adding')
        batch.execute()


if __name__ == '__main__':
    parse = ArgumentParser(description='A tool to add service accounts to a shared drive from a folder containing credential files.')
    parse.add_argument('--path', '-p', default='accounts', help='Specify an alternative path to the service accounts folder.')
    parse.add_argument('--token', default='token.pickle', help='Specify the pickle token file path.')
    parse.add_argument('--credentials', default='credentials.json', help='Specify the credentials file path.')
    parsereq = parse.add_argument_group('required arguments')
    parsereq.add_argument('--drive-id', '-d', help='The ID of the Shared Drive.', required=True)
    args = parse.parse_args()
    masshare(
        drive_id=args.drive_id,
        path=args.path,
        token=args.token,
        credentials=args.credentials
    )