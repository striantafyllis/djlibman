
import time
import logging
import configparser
import pkce
from urllib.parse import urlencode, urlparse, parse_qs
import webbrowser
import requests
import json


import pandas as pd
import numpy as np

from general_utils import *
import cache

logger = logging.getLogger(__name__)

class SoundcloudInterface:
    def __init__(self, config):
        self._client_id = config['client_id']
        if self._client_id.startswith('$'):
            self._client_id = os.environ[self._client_id[1:]]

        client_secret_loc = config['client_secret']

        if client_secret_loc.startswith('$'):
            self._client_secret = os.environ[client_secret_loc[1:]]
        else:
            with open(client_secret_loc) as client_secret_file:
                self._client_secret = client_secret_file.read().strip()

        self._redirect_uri = config['redirect_uri']
        self._cached_token_file = config['cached_token_file']

        self._access_token = None
        self._refresh_token = None
        self._access_token_expires_at = None

        self._cache = cache.Cache()

        return

    def _ensure_access_token(self):
        if self._access_token is None:
            if os.path.exists(self._cached_token_file):
                try:
                    self._read_access_token_file()
                except Exception as e:
                    print(f'Reading Soundcloud access token file failed: {str(e)}')
                    self._authorization_workflow()

        if self._access_token_expires_at is not None and \
            pd.Timestamp.now() < self._access_token_expires_at:
            return

        if self._refresh_token is not None:
            try:
                self._refresh_token_workflow()
            except Exception as e:
                print(f'Refreshing Soundcloud access token failed: {str(e)}')
                self._authorization_workflow()

        return

    def _read_access_token_file(self):
        with open(self._cached_token_file) as token_fh:
            obj = json.load(token_fh)
            self._access_token = obj['access_token']
            self._refresh_token = obj['refresh_token']
            self._access_token_expires_at = pd.Timestamp(obj['expires_at'])

    def _write_access_token_file(self):
        with open(self._cached_token_file, 'w') as token_file:
            json.dump(
                obj={
                    'access_token': self._access_token,
                    'refresh_token': self._refresh_token,
                    'expires_at': str(self._access_token_expires_at)
                },
                fp= token_file,
                indent=2)
        return

    def _authorization_workflow(self):
        state = random_string(10)

        code_verifier = pkce.generate_code_verifier(length=128)
        code_challenge = pkce.get_code_challenge(code_verifier)

        options = {
            'client_id': self._client_id,
            'redirect_uri': self._redirect_uri,
            'response_type': 'code',
            'code_challenge': code_challenge,
            'code_challenge_method': 'S256',
            'state': state
        }

        authorize_url = f'https://secure.soundcloud.com/authorize?{urlencode(options)}'

        print(f'Authorize URL: {authorize_url}')

        webbrowser.open(authorize_url)

        sys.stdout.write('Paste redirect URL here: > ')
        sys.stdout.flush()

        redirect_url = sys.stdin.readline()
        redirect_url = redirect_url.strip()

        parse_result = urlparse(redirect_url)
        response_options = parse_qs(parse_result.query)

        print('Response options: ')
        for key, value in response_options.items():
            print(f'{key}: {value[0]}')

        response_state = response_options['state'][0]
        response_code = response_options['code'][0]

        if response_state != state:
            raise Exception(f"Mismatch in state part of response; send '{state}', received '{response_state}'")


        post_headers = {
            'accept': 'application/json; charset=utf-8',
            'Content-Type': 'application/x-www-form-urlencoded'
        }

        post_data = {
            'grant_type': 'authorization_code',
            'client_id': self._client_id,
            'client_secret': self._client_secret,
            'redirect_uri': self._redirect_uri,
            'code_verifier': code_verifier,
            'code': response_code
        }

        post_response = requests.post(
            url='https://secure.soundcloud.com/oauth/token',
            data=post_data,
            headers=post_headers
        )

        if post_response.status_code != 200:
            raise Exception(f'SoundCloud authorization flow failed; '
                            f'code {post_response.status_code} text {post_response.text}')

        post_response_data = post_response.json()

        self._access_token = post_response_data['access_token']
        self._refresh_token = post_response_data['refresh_token']
        expires_in = post_response_data['expires_in']
        self._access_token_expires_at = pd.Timestamp.now() + pd.Timedelta(seconds=expires_in-10)

        self._write_access_token_file()
        return

    def _refresh_token_workflow(self):
        assert self._refresh_token is not None

        post_headers = {
            'accept': 'application/json; charset=utf-8',
            'Content-Type': 'application/x-www-form-urlencoded'
        }

        post_data = {
            'grant_type': 'refresh_token',
            'client_id': self._client_id,
            'client_secret': self._client_secret,
            'redirect_uri': self._redirect_uri,
            'refresh_token': self._refresh_token
        }

        post_response = requests.post(
            url='https://secure.soundcloud.com/oauth/token',
            data=post_data,
            headers=post_headers
        )

        if post_response.status_code != 200:
            raise Exception(f'SoundCloud refresh token flow failed; '
                            f'code {post_response.status_code} text {post_response.text}')

        post_response_data = post_response.json()

        self._access_token = post_response_data['access_token']
        self._refresh_token = post_response_data['refresh_token']
        expires_in = post_response_data['expires_in']
        self._access_token_expires_at = pd.Timestamp.now() + pd.Timedelta(seconds=expires_in-10)

        self._write_access_token_file()
        return

    def current_user(self):
        self._ensure_access_token()

        headers = {
            'accept': 'application/json; charset=utf-8',
            'Authorization': f'OAuth {self._access_token}'
        }

        response = requests.get(
            url='https://api.soundcloud.com/me',
            headers=headers
        )

        if response.status_code != 200:
            raise Exception(f'SoundCloud API GET failed; code {response.status_code} '
                            f'text {response.text}')

        return response.json()


def main():
    config_file = './config'
    config = configparser.ConfigParser()
    config.read(config_file)

    soundcloud_section = config['soundcloud']

    scl = SoundcloudInterface(soundcloud_section)

    cu = scl.current_user()

    print('Current user info:')
    json.dump(cu, sys.stdout, indent=4)
    sys.stdout.write('\n')
    sys.stdout.flush()

    return


if __name__ == '__main__':
    main()
    sys.exit(0)
