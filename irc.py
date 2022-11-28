from message_wrapper import receive_messages, delete_messages
from queue_wrapper import get_queue
import asyncio
import websockets
import json
import logging

logger = logging.getLogger("websockets")
logger.setLevel(logging.DEBUG)
logger.addHandler(logging.StreamHandler())
oauth = ""
userId = "virgil246"
channel_list = []
loop = asyncio.get_event_loop()


async def start_connection():
    uri = "wss://irc-ws.chat.twitch.tv:443"
    async for websocket in websockets.connect(uri):
        try:
            await websocket.send(f"PASS {oauth}")
            await websocket.send(f"NICK {userId}")
            for channel in channel_list:
                await websocket.send(f"JOIN #{channel}")
            await handler(websocket)
        except websockets.ConnectionClosed:
            continue


async def consumer_handler(websocket):
    async for message in websocket:
        await consumer(message)


async def consumer(message):
    print("message", message)


async def producer_handler(websocket):
    while True:
        msg_list = await producer()
        coro = [send_message(websocket, msg) for msg in msg_list]
        await asyncio.gather(*coro)


async def producer():

    queue = get_queue("DonateNoti")

    batch_size = 10
    received_messages = await loop.run_in_executor(
        None, receive_messages, queue, batch_size, 2
    )
    message_list = []
    for message in received_messages:
        attr, body = unpack_message(message)
        donate_msg = json.loads(body)
        message_list.append(donate_msg)
    if received_messages:
        await loop.run_in_executor(None, delete_messages, queue, received_messages)
    return message_list


async def handler(websocket):
    await asyncio.gather(
        consumer_handler(websocket),
        producer_handler(websocket),
    )


def unpack_message(msg):
    return (msg.message_attributes, msg.body)


async def send_message(websocket, donate_msg):
    await websocket.send(
        f'PRIVMSG #{donate_msg["twitch_id"]} :{donate_msg["name"]}丟了{donate_msg["amount"]}說: {donate_msg["msg"]}'
    )


if __name__ == "__main__":
    loop.run_until_complete(start_connection())
