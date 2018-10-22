try:
    import ujson as json
except:
    import json

import os
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from itertools import chain


def get_sheet_client():
    # use creds to create a client to interact with the Google Drive API
    scope = ['https://spreadsheets.google.com/feeds']
    creds = ServiceAccountCredentials.from_json_keyfile_name('../gspread.json', scope)
    client = gspread.authorize(creds)
    return client
    

def get_white_keys(client):
    def get_es_names(hashes):
        return {i.get('Elastic search variable') for i in hashes}
    url = os.getenv('DATA_DICT_URL')
    doc = client.open_by_url(url)
    #the first page is not relevant, get the hashes from the next 6
    pages = [doc.get_worksheet(i).get_all_records() for i in range(1,7)]
    variables = [get_es_names(i) for i in pages]
    white_keys = set(chain(*variables))
    return white_keys


def truncate_unicode(s, length, encoding='utf-8'):
    """Truncate a string by byte limit"""
    encoded = s.encode(encoding)[:length]
    return encoded.decode(encoding, 'ignore')
            
    
def truncate_bytes(the_list, byte_limit=32766):
    """Truncate a list based on a total byte limit"""
    total_bytes = 0
    if len(the_list) == 1:
        return [truncate_unicode(i, byte_limit) for i in the_list]
    for i, item in enumerate(the_list):
        if isinstance(item, str):
            total_bytes += len(item.encode('utf-8'))
        if total_bytes > byte_limit:
            return the_list[:i-1] # max limit reached, return what came before
    return the_list # doesn't reach max limit? just give the list back


def json_lines_bk_iter(fle, white_keys):
    for line in fle:
        if isinstance(line, str):
            j = json.loads(line)
        else:
            j = json.loads(line.decode('utf-8'))
        # preserve only keys from the whitelist
        wanted = { k: v for k, v in j.items() if k in white_keys }
        for key, val in wanted.items():
                fb = "".join(val)
                if isinstance(fb, str):
                    fb = len(fb.encode('utf-8'))
                    if(fb > 32766):
                        # print("{}:{} - {}".format(i, key, fb))
                        wanted[key] = truncate_bytes(val)
                        pass
        yield wanted


def dump_lines(lines, file_path):
    for line in lines:
        if line == {}:
            continue
        with open(file_path, 'a+') as f:
            f.write(json.dumps(line) + '\n')
            

if __name__=='__main__':
    client = get_sheet_client()
    print('[Getting white list keys]')
    white_keys = get_white_keys(client)
    print(white_keys)

    fp = '/Users/jgriffithshawking/dtajson/metadata-report01-01.json'
    lines = json_lines_bk_iter(open(fp), white_keys)
    print('[Dumping filtered file..]')
    file_path = '/Users/jgriffithshawking/dtajson/refilter-10-10.json'
    dump_lines(lines, file_path)
    print('[Done]')
