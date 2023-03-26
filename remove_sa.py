from google.oauth2.service_account import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from argparse import ArgumentParser
from os.path import exists
import pickle

to_be_removed = []


def _is_success(id, resp, exception):
    global to_be_removed

    if exception is not None:
        exp = str(exception).split('?')[0].split('/')
        if exp[0].startswith('<HttpError 404'):
            pass
        else:
            to_be_removed.append(exp[-1])


def remove(drive_id=None, token='token.pickle', credentials='credentials.json', suffix=None, prefix=None, role=None):
    global to_be_removed

    SCOPES = ["https://www.googleapis.com/auth/drive"]
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

    valid_roles = ['owner', 'organizer', 'fileorganizer', 'writer', 'reader', 'commenter']
    valid_levels = ['owner', 'manager', 'content manager', 'contributor', 'viewer', 'commenter']
    if role:
        if role.lower() in valid_levels:
            role = valid_roles[valid_levels.index(role.lower())]
        elif role.lower() in valid_roles:
            role = role.lower()
        else:
            print('Invalid role.')
            exit(-1)

    print('Getting permissions')

    rp = drive.permissions().list(fileId=drive_id, pageSize=100,
                                  fields='nextPageToken,permissions(id,emailAddress,role)',
                                  supportsAllDrives=True).execute()
    cont = True
    all_perms = []
    while cont:
        all_perms += rp['permissions']
        if "nextPageToken" in rp:
            rp = drive.permissions().list(fileId=drive_id, pageSize=100,
                                          fields='nextPageToken,permissions(id,emailAddress,role)',
                                          supportsAllDrives=True, pageToken=rp["nextPageToken"]).execute()
        else:
            cont = False

    for i in all_perms:
        if prefix:
            if i['emailAddress'].split('@')[0].startswith(prefix):
                to_be_removed.append(i['id'])
        elif suffix:
            if i['emailAddress'].split('@')[0].endswith(suffix):
                to_be_removed.append(i['id'])
        elif role:
            if role == i['role'].lower():
                to_be_removed.append(i['id'])

    while len(to_be_removed) > 0:
        print('Removing %d members.' % len(to_be_removed))
        tbr = [to_be_removed[i:i + 100] for i in range(0, len(to_be_removed), 100)]
        to_be_removed = []
        for j in tbr:
            batch = drive.new_batch_http_request(callback=_is_success)
            for i in j:
                batch.add(drive.permissions().delete(fileId=drive_id, permissionId=i, supportsAllDrives=True))
            batch.execute()
    print('Users removed.')


if __name__ == '__main__':
    parse = ArgumentParser(description='A tool to remove users from a Shared Drive.')
    parse.add_argument('--token', default='token.pickle', help='Specify the pickle token file path.')
    parse.add_argument('--credentials', default='credentials.json', help='Specify the credentials file path.')
    oft = parse.add_mutually_exclusive_group(required=True)
    oft.add_argument('--prefix', help='Remove users that match a prefix.')
    oft.add_argument('--suffix', help='Remove users that match a suffix.')
    oft.add_argument('--role', help='Remove users based on permission roles.')
    parsereq = parse.add_argument_group('required arguments')
    parsereq.add_argument('--drive-id', '-d', help='The ID of the Shared Drive.', required=True)
    args = parse.parse_args()

    remove(
        drive_id=args.drive_id,
        token=args.token,
        credentials=args.credentials,
        prefix=args.prefix,
        suffix=args.suffix,
        role=args.role
    )