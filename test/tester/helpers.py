from datetime import datetime, timezone
from functools import partial
from base64 import b64encode
import requests
import random
import uuid
import time

from tester.context import get_service_url

WRITE_KEY = "not-actually-secret"
WRITE_KEY_B64 = b64encode(("%s:" % WRITE_KEY).encode('utf-8')).decode('ascii')
NOT_WRITE_KEY = "this-is-not-the-write-key"
NOT_WRITE_KEY_B64 = b64encode(("%s:" % NOT_WRITE_KEY).encode('utf-8')).decode('ascii')


def wait():
    time.sleep(0.5)


def run_and_wait(fn, *args, **kwargs):
    headers = kwargs.get("headers", {})
    if 'disable_auth' not in kwargs and 'Authorization' not in headers:
        headers['Authorization'] = 'Basic %s' % WRITE_KEY_B64
        kwargs['headers'] = headers
    elif 'disable_auth' in kwargs:
        del kwargs['disable_auth']

    ret = fn(*args, **kwargs)
    wait()
    return ret


class Stilgar:
    sourceConfig = partial(run_and_wait, requests.get, get_service_url("stilgar", "sourceConfig"))
    page = partial(run_and_wait, requests.post, get_service_url("stilgar", "v1", "page"))
    group = partial(run_and_wait, requests.post, get_service_url("stilgar", "v1", "group"))
    alias = partial(run_and_wait, requests.post, get_service_url("stilgar", "v1", "alias"))
    screen = partial(run_and_wait, requests.post, get_service_url("stilgar", "v1", "screen"))
    track = partial(run_and_wait, requests.post, get_service_url("stilgar", "v1", "track"))
    identify = partial(run_and_wait, requests.post, get_service_url("stilgar", "v1", "identify"))
    batch = partial(run_and_wait, requests.post, get_service_url("stilgar", "v1", "batch"))


class Events:
    @staticmethod
    def random_str():
        return str(uuid.uuid4()).replace('-', '')

    @staticmethod
    def random_event_type():
        return "custom_event_%s" % hex(random.getrandbits(64))[2:]

    @staticmethod
    def context():
        return {
            "app": {
                "name": "RudderLabs JavaScript SDK",
                "namespace": "com.rudderlabs.javascript",
                "version": "2.26.0"
            },
            "library": {
                "name": "RudderLabs JavaScript SDK",
                "version": "2.26.0"
            },
            "userAgent": "Mozilla/5.0 (X11; Linux x86_64; rv:109.0) Gecko/20100101 Firefox/110.0",
            "os": {
                "name": "",
                "version": Events.random_str(),
            },
            "locale": "en-US",
            "screen": {
                "density": 1,
                "width": 1920,
                "height": 1080,
                "innerWidth": 1920,
                "innerHeight": 576
            },
            "campaign": {}
        }

    @staticmethod
    def common_fields():
        dt = datetime.now(tz=timezone.utc).isoformat()
        return {
            "anonymousId": Events.random_str(),
            "context": Events.context(),
            "channel": "web",
            "messageId": Events.random_str(),
            "originalTimestamp": dt,
            "sentAt": dt,
            "integrations": {
                "All": True
            },
        }

    @staticmethod
    def alias():
        return {**Events.common_fields(), **{
            "type": "alias",
            "previousId": Events.random_str(),
        }}

    @staticmethod
    def group():
        return {**Events.common_fields(), **{
            "type": "group",
            "traits": {
                "group_awesomeness": 5
            },
            "groupId": Events.random_str(),
        }}

    @staticmethod
    def page():
        path = Events.random_str()
        url = "https://stilg.ar/" + path
        return {**Events.common_fields(), **{
            "type": "page",
            "properties": {
                "page_awesomeness": 2,
                "path": "/" + path,
                "referrer": "$direct",
                "referring_domain": "",
                "search": "",
                "title": "Rudderstack JS SDK",
                "url": url,
                "tab_url": url,
                "initial_referrer": "$direct",
                "initial_referring_domain": ""
            },
            "name": Events.random_str(),
            "category": Events.random_str(),
        }}

    @staticmethod
    def screen():
        return {**Events.common_fields(), **{
            "type": "screen",
            "properties": {
                "screen_awesomeness": 2,
            },
            "name": Events.random_str(),
            "category": Events.random_str(),
        }}

    @staticmethod
    def track():
        return {**Events.common_fields(), **{
            "type": "track",
            "event": "custom_test_event",
            "properties": {
                "custom_prop1": 2,
                "custom_prop2": "wow",
            },
        }}

    @staticmethod
    def identify(user_id, traits={}):
        identify_payload = {**Events.common_fields(), **{
            "type": "identify",
            "userId": user_id,
        }}
        identify_payload['context']['traits'] = traits
        return identify_payload
