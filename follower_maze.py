#!/usr/bin/env python3.5
# coding=utf-8
"""
This module contains everything for Follower Maze.

There are two top-level classes:

Client: manages sending messages to clients and stores the list of their
        followers.

EventStream: stores all clients we know about and stores events until we
             receive the one we're expecting.

We support one environment variable:

logLevel: This defaults to INFO, but can be changed to DEBUG to show debug logs.

We've made a few assumptions:

- Might receive follows for a client that doesn't exist yet, and a client might
  disconnect before the end of a stream.

We use one third party library for this module:

uvloop: It uses the libuv C library for the event loop instead of the default
        Python event loop since it's 2-4x faster

We use one third party library for our tests:

pytest: To easily run the tests using 'py.test test_follower_maze.py -v'
"""
import asyncio
import logging
from asyncio import StreamReader, StreamWriter
from os import getenv

try:
    import uvloop
except ImportError:
    uvloop = None


class Client(object):
    """Client model for each client we know about from events or connections.

    Attributes:
        id: ID client gave us after connecting, or from an event for a client
            that doesn't exist yet
        reader: StreamReader from the connected client if we have one, else None
        writer: StreamWriter from the connected client if we have one, else None
        followers: set of followers
    """

    def __init__(self, client_id, reader=None, writer=None):
        self.id = client_id
        self.reader = reader
        self.writer = writer
        self.followers = set()

    async def write(self, message):
        """Try to send message to client.

        We might not need to support disconnected clients or the case where we
        receive followers before knowing about the client.
        """
        logging.debug('Sending a message to %s', self.id)
        if self.writer is None:
            logging.debug('Tried to send a message but no client connected yet for %s', self.id)
            return
        try:
            self.writer.write(message.encode())
        except RuntimeError as exc:
            if 'handler is closed' in exc.args[0]:
                logging.debug('Tried to send a message but handler is probably closed for %s', self.id)
                return
        try:
            await self.writer.drain()
        except ConnectionResetError:
            logging.debug('Tried to send a message but connection lost for %s', self.id)
            return
        logging.debug('Sent %s to %s', message, self.id)


class EventStream(object):
    """EventStream model for handling events and storing clients.

    We stores a dictionary of all clients so we can easily look up clients by
    their ID.

    We store all events that we haven't processed yet and delete them from the
    dictionary just before we process them.

    Attributes:
        clients: dictionary of connected Client instances
        expected_sequence_num: integer for the event we're currently waiting for
        events: dictionary of events we haven't been able to process yet
    """

    def __init__(self):
        self.clients = {}
        self.expected_sequence_num = 1
        self.events = {}

    async def register_client(self, client_id, reader, writer):
        """Create client and store it in self.clients."""
        logging.debug('Registered client %r', client_id)
        if client_id not in self.clients:
            self.clients[client_id] = Client(client_id, reader, writer)
        else:
            self.clients[client_id].reader = reader
            self.clients[client_id].writer = writer

    async def message_client(self, event, to_id):
        """Send event to to_id's client."""
        if to_id not in self.clients:
            logging.debug('Tried to send a message but no client connected yet for %s', to_id)
            return
        await self.clients[to_id].write(event)

    async def follow(self, event, from_id, to_id):
        """Add from_id to to_id client's followers and notify to_id's client."""
        logging.debug('%s followed %s', from_id, to_id)
        self.clients.setdefault(to_id, Client(to_id))
        client = self.clients[to_id]
        client.followers.add(from_id)
        await self.message_client(event, to_id)

    async def unfollow(self, from_id, to_id):
        """Remove from_id client from to_id client's followers and don't notify anyone."""
        logging.debug('%s unfollowed %s', from_id, to_id)
        self.clients.setdefault(to_id, Client(to_id))
        client = self.clients[to_id]
        client.followers.remove(from_id)

    async def broadcast(self, event):
        """Forward event to all clients."""
        logging.debug('Broadcast event received')
        await asyncio.gather(*[
            self.message_client(event, client.id)
            for client in self.clients.values()
        ], return_exceptions=True)

    async def private_message(self, event, from_id, to_id):
        """Forward event only to to_id."""
        logging.debug('%s private messaged %s', from_id, to_id)
        await self.message_client(event, to_id)

    async def status_update(self, event, from_id):
        """Forward event to current followers of the from_id."""
        logging.debug('%s updated their status', from_id)
        self.clients.setdefault(from_id, Client(from_id))
        await asyncio.gather(*[
            self.message_client(event, client_id)
            for client_id in self.clients[from_id].followers
        ], return_exceptions=True)

    async def handle_event(self, event):
        """Route event to whichever function handles depending on type of event.

        We add a newline to the event because when reading from the event source
        connections's stream we use 'readline' which reads until the newline
        but doesn't include the newline in the actual event's string.
        """
        event += '\n'
        command = event.strip().split('|')
        logging.debug('Processing %s', event.strip())
        payload_type = command[1]
        if payload_type == 'F':
            from_id, to_id = command[2:]
            await self.follow(event, from_id, to_id)
        elif payload_type == 'U':
            from_id, to_id = command[2:]
            await self.unfollow(from_id, to_id)
        elif payload_type == 'B':
            await self.broadcast(event)
        elif payload_type == 'P':
            from_id, to_id = command[2:]
            await self.private_message(event, from_id, to_id)
        else:  # S
            from_id = command[2]
            await self.status_update(event, from_id)

    async def handle_event_batch(self, event):
        """Add event to self.events and then process all events we can.

        We store events until we receive the next expected event, stored in
        self.expected_sequence_num, and then pop it from the dictionary when
        processing it.
        """
        sequence_num, _, _ = event.partition('|')
        self.events[int(sequence_num)] = event.strip()
        while self.expected_sequence_num in self.events:
            logging.debug('Processing event %s', self.expected_sequence_num)
            await self.handle_event(self.events.pop(self.expected_sequence_num))
            self.expected_sequence_num += 1


async def handle_event_source(reader: StreamReader, writer: StreamWriter):
    """Add event to event_stream and then process all events we can.

    "View" for event source connections that reads each event and adds it to
    the event stream.
    """
    addr = writer.get_extra_info('peername')
    while True:
        data = await reader.readline()
        if not data:
            writer.close()
            return
        event = data.decode()

        logging.debug('Received %r from %r', event, addr)
        await event_stream.handle_event_batch(event)


async def handle_client(reader: StreamReader, writer: StreamWriter):
    """Handler for client connections.

    "View" for client connections that registers the client and stores the
    client's reader and writer in event_stream.clients under it's ID.
    """
    addr = writer.get_extra_info('peername')
    while True:
        data = await reader.readline()
        if not data:
            writer.close()
            return
        message = data.decode()
        logging.debug('Received %r from %r', message, addr)
        await event_stream.register_client(message.strip(), reader, writer)


event_stream = EventStream()


def main():
    logging.basicConfig(level=getattr(logging, getenv('logLevel', 'INFO').upper()))

    if uvloop is None:
        logging.info("uvloop not installed: using default event loop")
    else:
        asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

    loop = asyncio.get_event_loop()

    event_source_server_coroutine = asyncio.start_server(handle_event_source, '127.0.0.1', 9090, loop=loop)
    client_server_coroutine = asyncio.start_server(handle_client, '127.0.0.1', 9099, loop=loop)
    event_source_server = loop.run_until_complete(event_source_server_coroutine)
    client_server = loop.run_until_complete(client_server_coroutine)

    logging.info('Event source server serving on %s:%s', *event_source_server.sockets[0].getsockname())
    logging.info('Client server serving on %s:%s', *client_server.sockets[0].getsockname())

    try:
        loop.run_forever()
    except KeyboardInterrupt:
        pass

    event_source_server.close()
    client_server.close()
    loop.run_until_complete(event_source_server.wait_closed())
    loop.run_until_complete(client_server.wait_closed())
    loop.close()
    logging.info('Handled %s events', event_stream.expected_sequence_num - 1)


if __name__ == '__main__':
    main()
