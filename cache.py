
import time
import logging

logger = logging.getLogger(__name__)

class Cache:
    def __init__(self):
        self._cache = {}
        return

    def look_up(self, *keys):
        key = tuple(keys)

        entry = self._cache.get(key)

        if entry is None:
            logger.debug('Miss: %s', key)
            return None

        if time.time() > entry['timestamp'] + entry['ttl']:
            logger.debug('Expired entry: %s', key)
            del self._cache[key]
            return None

        logger.debug('Hit: %s', key)
        return entry['value']

    def store(self, value, ttl, *keys):
        self._cache[tuple(keys)] = {
            'value': value,
            'timestamp': time.time(),
            'ttl': ttl
        }
        return

    def invalidate(self, *keys):
        del self._cache[tuple(keys)]
        return

    def look_up_or_get(self, func, ttl, *keys):
        value = self.look_up(*keys)

        if value is not None:
            return value

        value = func()

        self.store(func, ttl)

        return value
