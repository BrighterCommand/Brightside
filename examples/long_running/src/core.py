import logging
import time

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


class LongRunningCommand(Command):
    def __init__(self, sleep_for=30):
        super().__init__()
        self.sleep_for = sleep_for


class LongRunningCommandHandler(Handler):
    def __init__(self):
        super().__init__()
        self._logger = logging.getLogger(__name__)

    def handle(self, request: LongRunningCommand):
        self._logger.debug("Received Long Running Command - will sleep for {} seconds".format(request.sleep_for))
        time.sleep(request.sleep_for)
        self._logger.debug("Ended Long Running Command - woken up message pump")



