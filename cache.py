
import time
import logging

logger = logging.getLogger(__name__)

class Cache:
    def __init__(self):
        self._cache = {}
        return

    def look_up(self, *keys):
        entry = self._cache.get(keys)

        if entry is None:
            logger.debug('Miss: %s', keys)
            return None

        if time.time() > entry['timestamp'] + entry['ttl']:
            logger.debug('Expired entry: %s', keys)
            del self._cache[keys]
            return None

        logger.debug('Hit: %s', keys)
        return entry['value']

    def store(self, value, ttl, *keys):
        self._cache[keys] = {
            'value': value,
            'timestamp': time.time(),
            'ttl': ttl
        }
        return

    def invalidate(self, *keys):
        if keys in self._cache:
            del self._cache[keys]
        return

    def look_up_or_get(self, func, ttl, *keys):
        value = self.look_up(*keys)

        if value is not None:
            return value

        value = func()

        self.store(value, ttl, *keys)

        return value
