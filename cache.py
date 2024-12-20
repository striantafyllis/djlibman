
import time


class Cache:
    def __init__(self):
        self._cache = {}
        return

    def look_up(self, *keys):
        entry = self._cache.get(tuple(keys))

        if entry is None:
            return None

        if time.time() > entry['timestamp'] + entry['ttl']:
            del self._cache[tuple(keys)]
            return None

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
