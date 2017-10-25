from brightside.handler import Handler, Command
from brightside.messaging import BrightsideMessage, BrightsideMessageStore


class FakeMessageStore(BrightsideMessageStore):
    def __init__(self):
        self._message_was_added = None
        self._messages = []

    @property
    def message_was_added(self):
        return self._message_was_added

    def add(self, message: BrightsideMessage):
        self._messages.append(message)
        self._message_was_added = True

    def get_message(self, key):
        for msg in self._messages:
            if msg.id == key:
                return msg
        return None


class HelloWorldCommandHandler(Handler):
    def handle(self, request):
        print("Hello {}".format(request.addressee))


class HelloWorldCommand(Command):
    def __init__(self, addressee=None):
        super().__init__()
        self._addressee = addressee

    @property
    def addressee(self):
        return self._addressee
