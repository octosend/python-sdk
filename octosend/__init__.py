import json
import urllib2
import base64

RMTA_API_URL = "https://api.octosend.com/api/3.0"


class API(object):

    def __init__(self, url = RMTA_API_URL, token = None):
        self.url = url
        self.token = token

    def call(self, method, params = None):
        url = self.url + '/' + method
        headers = {}
        headers['Content-Type'] = 'application/json'
        if self.token:
            headers['X-RMTA-API-Key'] = self.token

        if params is None:
            req = urllib2.Request(url, None, headers)
        else:
            req = urllib2.Request(url, json.dumps(params), headers)
        try:
            f = urllib2.urlopen(req)
            body = f.read()
            return json.loads(body)
        except urllib2.HTTPError, exc:
            print exc.code
            print exc.headers
            print exc.read()
            raise

    def authenticate(self, user, password):
        res = self.call('authenticate', { 'username': user, 'password': password })
        self.token = res['api-key']
        return res

    def client(self):
        return Client(self)

    def domain_by_data(self, data):
        return Domain(self, data)

    def domain_by_name(self, name):
        data = self.call('domain/' + name)
        return self.domain_by_data(data)

    def spooler_by_data(self, data):
        return Spooler(self, data)

    def spooler_by_token(self, token):
        data = self.call('spooler/' + token)
        return self.spooler_by_data(data)


class Client(object):

    def __init__(self, api):
        self.api = api

    def spoolers(self):
        return SpoolersFilter(self.api)

    def domains(self):
        return DomainsFilter(self.api)

    def domain(self, name):
        return self.api.domain_by_name(name)

    def spooler(self, token):
        return self.api.spooler_by_token(token)

    def statistics(self, period, groupBy = 'global'):
        return self.api.call('statistics/global', { 'period': period,
                                                    'groupBy': groupBy })

    def timeline(self, type = 'marketing'):
        return self.api.call('timeline/global', { 'type': type })

class Filter(object):

    url_count = None
    url_fetch = None

    def __init__(self, api):
        self.api = api
        self.filters = {}

    def param(self, key, value):
        self.filters[key] = value

    def param_array(self, key, value):
        if key not in self.filters:
            self.filters[key] = []
        self.filters[key].append(value)

    def count(self):
        return self.api.call(self.url_count, self.filters)

    def fetch(self, offset = 0, limit = 100, reverse = False):
        params = { 'offset': offset,
                   'limit': limit,
                   'reverse': reverse }
        params.update(self.filters)
        return [ self.factory(data) for data in self.api.call(self.url_fetch, params) ]

    def iterate(self, offset = 0, count = None, reverse = False, batch_size = 50):

        if count is not None:
            left = count

        while True:
            if count is not None:
                limit = min(left, batch_size)
            else:
                limit = batch_size

            if limit == 0:
                return

            rows = self.fetch(offset, limit, reverse)
            for entry in rows:
                yield entry

            if len(rows) != limit:
                return

            offset += len(rows)
            if count is not None:
                left -= len(rows)

    def factory(self, data):
        return data


class Item(object):

    def __init__(self, api, data):
        self.api = api
        self.data = data


class DomainsFilter(Filter):

    url_count = 'domains/count'
    url_fetch = 'domains/fetch'

    def factory(self, data):
        return self.api.domain_by_data(data)


class SpoolersFilter(Filter):

    url_count = 'spoolers/count'
    url_fetch = 'spoolers/fetch'

    def factory(self, data):
        return self.api.spooler_by_data(data)

    def domain(self, domain):
        self.param_array('domains', domain)

    def state(self, state):
        self.param_array('states', state)


class EventsFilter(Filter):

    def factory(self, data):
        return data

    def event(self, event):
        self.param('event', event)


class Domain(Item):

    def name(self):
        return self.data['name']

    def spoolers(self):
        f = SpoolersFilter(self.api)
        f.domain(self.name())
        return f

    def create_spooler(self, type):
        data = self.api.call('spoolers/create', { 'domain': self.name(),
                                                  'type': type })
        return self.api.spooler_by_data(data)

    def statistics(self, period, groupBy = 'global'):
        return self.api.call('statistics/domain/' + self.name(), { 'period': period,
                                                                   'groupBy': groupBy })

    def timeline(self, type = 'marketing'):
        return self.api.call('timeline/domain/' + self.name(), { 'type': type })

    def draft_addresses(self, addresses = None):
        url = 'domain/' + self.name() + '/draft-addresses'
        if addresses is None:
            return self.api.call(url)
        else:
            return self.api.call(url, { 'addresses': addresses })

class Spooler(Item):

    def _url(self, path = ''):
        return 'spooler/' + self.token() + path

    def token(self):
        return self.data['token']
    def type(self):
        return self.data['type']
    def domain(self):
        return self.api.domain_by_name(self.data['domain'])

    def name(self, name = None):
        if name is not None:
            self.data = self.api.call(self._url('/name'), { 'name': name })
        return self.data['name']

    def start(self, timestamp = None):
        if timestamp is not None:
            self.data = self.api.call(self._url('/start'), { 'start': timestamp })
        return self.data['start']

    def tags(self, tags = None):
        if tags is not None:
            self.data = self.api.call(self._url('/tags'), { 'tags': tags })
        return self.data['tags']

    def ready(self):
        self.api.call(self._url('/ready'), {})
    def finish(self):
        self.api.call(self._url('/finish'), {})
    def cancel(self):
        self.api.call(self._url('/cancel'), {})

    def statistics(self, groupBy = 'global'):
        return self.api.call('statistics/spooler/' + self.token(), { 'groupBy': groupBy })

    def timeline(self):
        return self.api.call('timeline/spooler/' + self.token(), {})

    def message(self, new = False):
        if not new:
            data = self.api.call(self._url('/message'))
        else:
            data = {}
        return SpoolerMessage(self, data)

    def batch(self):
        return SpoolerBatch(self)

    def mail(self, email):
        return SpoolerMail(self, email)

    def events(self, event):
        fltr = EventsFilter(self.api)
        fltr.url_count = 'events/spooler/%s/count' % self.token()
        fltr.url_fetch = 'events/spooler/%s/fetch' % self.token()
        fltr.event(event)
        return fltr


class Message(object):

    def __init__(self, spooler, data):
        self.spooler = spooler
        self.api = spooler.api
        self.data = data

    def _property(self, key, value):
        if value is not None:
            self.data[key] = value
        return self.data[key]

    def _property_add(self, key, value):
        if value is not None:
            if key not in self.data:
                self.data[key] = []
            self.data[key].append(value)
        return self.data.get(key)

    def subject(self, subject = None):
        return self._property('subject', subject)
    def sender(self, sender = None):
        return self._property('sender', sender)
    def recipient(self, recipient = None):
        return self._property('recipient', recipient)
    def headers(self, headers = None):
        return self._property('headers', headers)
    def variables(self, variables = None):
        return self._property('variables', variables)
    def parts(self, parts = None):
        return self._property('parts', parts)
    def attachments(self, attachments = None):
        return self._property('attachments', attahments)

    def part(self, type, content):
        resource_id = self.api.call(self.spooler._url(self.url_part), { 'type': type,
                                                                        'content': content })
        self._property_add('parts', resource_id)

    def attachment(self, type, content, filename = None):
        params = { 'type': type,
                   'content': base64.b64encode(content) }
        if filename is not None:
            params['filename'] = filename
        resource_id = self.api.call(self.spooler._url(self.url_attachment), params)
        self._property_add('attachments', resource_id)

    def unsubscribe(self, type, content):
        resource_id = self.api.call(self.spooler._url(self.url_unsubscribe), { 'type': type,
                                                                               'content': content })
        self._property('unsubscribe', resource_id)


class SpoolerMessage(Message):

    url_part = '/resources/part'
    url_attachment = '/resources/attachment'
    url_unsubscribe = '/resources/unsubscribe'

    def reset(self):
        self.data.clear()

    def reset_parts(self):
        if 'parts' in self.data:
            del self.data['parts']

    def reset_attachments(self):
        if 'attachments' in self.data:
            del self.data['attachments']

    def save(self):
        self.data = self.api.call(self.spooler._url('/message'), self.data)


class MailMessage(Message):

    url_part = '/mails/resources/part'
    url_attachment = '/mails/resources/attachment'
    url_unsubscribe = '/mails/resources/unsubscribe'


class SpoolerBatch(object):

    def __init__(self, spooler):
        self.spooler = spooler
        self.mails = []

    def mail(self, email):
        m = SpoolerMail(self.spooler, email)
        self.mails.append(m)
        return m

    def spool(self):
        return self.spooler.api.call(self.spooler._url('/spool'), {
                'mails': [ mail._spool_data() for mail in self.mails ]
                })

    def draft(self):
        return self.spooler.api.call(self.spooler._url('/draft'), {
                'mails': [ mail._spool_data() for mail in self.mails ]
                })

    def preview(self):
        return self.spooler.api.call(self.spooler._url('/preview'), {
                'mails': [ mail._spool_data() for mail in self.mails ]
                })


class SpoolerMail(object):

    def __init__(self, spooler, email):
        self.spooler = spooler
        self.api = spooler.api
        self.email = email
        self.data = {}

    def _spool_data(self):
        r = {}
        r["email"] = self.email
        r.update(self.data)
        return r

    def message(self):
        return MailMessage(self.spooler, self.data)

    def spool(self):
        return self.spooler.api.call(self.spooler._url('/spool'), {
                'mails': [ self._spool_data() ]
                })

    def draft(self):
        return self.spooler.api.call(self.spooler._url('/draft'), {
                'mails': [ self._spool_data() ]
                })

    def preview(self):
        return self.spooler.api.call(self.spooler._url('/preview'), {
                'mails': [ self._spool_data() ]
                })
