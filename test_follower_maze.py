from unittest import TestCase
from unittest.mock import MagicMock
from asyncio.test_utils import TestLoop

from follower_maze import Client, EventStream


class AsyncMock(MagicMock):
    async def __call__(self, *args, **kwargs):
        return super(AsyncMock, self).__call__(*args, **kwargs)



class EventStreamTestCase(TestCase):
    @classmethod
    def setUpClass(cls):
        cls.event_loop = TestLoop()
        cls.event_stream = EventStream()
        cls.event_loop.run_until_complete(cls.event_stream.register_client('1', None, None))
        cls.event_loop.run_until_complete(cls.event_stream.register_client('2', None, None))
        cls.event_loop.run_until_complete(cls.event_stream.register_client('3', None, None))
        cls.event_stream.clients['1'].write = AsyncMock()
        cls.event_stream.clients['2'].write = AsyncMock()
        cls.event_stream.clients['3'].write = AsyncMock()

    def test_client_registration(self):
        assert '1' in self.event_stream.clients
        assert '2' in self.event_stream.clients
        assert '3' in self.event_stream.clients

    def test_follow(self):
        follow_event = '1|F|2|1\n'
        self.event_loop.run_until_complete(self.event_stream.handle_event(follow_event))

        self.event_stream.clients['1'].write.assert_called_with(follow_event)
        assert '2' in self.event_stream.clients['1'].followers

    def test_status_update(self):
        follow_event = '1|F|2|1\n'
        self.event_loop.run_until_complete(self.event_stream.handle_event(follow_event))
        status_update_event = '2|S|1\n'
        self.event_loop.run_until_complete(self.event_stream.handle_event(status_update_event))

        self.event_stream.clients['1'].write.assert_called_with(follow_event)
        assert '2' in self.event_stream.clients['1'].followers
        self.event_stream.clients['2'].write.assert_called_with(status_update_event)

    def test_broadcast(self):
        broadcast_event = '1|B\n'
        self.event_loop.run_until_complete(self.event_stream.handle_event(broadcast_event))

        self.event_stream.clients['1'].write.assert_called_with(broadcast_event)
        self.event_stream.clients['2'].write.assert_called_with(broadcast_event)


    def test_private_message(self):
        follow_event = '1|F|2|1\n'
        self.event_loop.run_until_complete(self.event_stream.handle_event(follow_event))
        status_update_event = '2|S|1\n'
        self.event_loop.run_until_complete(self.event_stream.handle_event(status_update_event))

        assert '2' in self.event_stream.clients['1'].followers
        self.event_stream.clients['1'].write.assert_called_with(follow_event)
        self.event_stream.clients['2'].write.assert_called_with(status_update_event)

