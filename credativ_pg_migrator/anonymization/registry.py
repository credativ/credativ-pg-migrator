class Registry:
    def __init__(self):
        self._methods = {}

    def register(self, name: str):
        def decorator(func):
            self._methods[name] = func
            return func
        return decorator

    def get(self, name: str):
        return self._methods.get(name)

    def is_registered(self, name: str):
        return name in self._methods

anonymization_registry = Registry()
