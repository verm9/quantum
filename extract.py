import abc
import asyncio
import datetime
import json
import logging
import os
from asyncio import sleep
from enum import Enum
from string import Template
from typing import Optional

import aiohttp
import websockets
from binance import AsyncClient
from kucoin.asyncio import KucoinSocketManager
from kucoin.client import Client

from repo import PriceRecord, BasePriceRepo

logger = logging.getLogger(__name__)
fileHandler = logging.StreamHandler()
os.makedirs('logs', exist_ok=True)
consoleHandler = logging.FileHandler(f'logs/{__name__ }.log')
formatter = logging.Formatter(
        '%(asctime)s %(name)-12s %(levelname)-8s %(message)s')
fileHandler.setFormatter(formatter)
consoleHandler.setFormatter(formatter)
logger.addHandler(fileHandler)
logger.addHandler(consoleHandler)
logger.setLevel(logging.INFO)


class ChannelStatus(Enum):
    CREATED = 1
    SUBSCRIBING = 2
    SUBSCRIBED = 3
    WORKING = 4
    ERROR = 16


class Channel:
    def __init__(self, connection, symbol: str, status: Optional[ChannelStatus]):
        self.connection = connection
        self.symbol = symbol
        self.status = status


class BitfinexChannel(Channel):
    def __init__(self, connection, symbol: str, status: Optional[ChannelStatus], channel_id: Optional[str]):
        super().__init__(connection, symbol, status)
        self.channel_id = channel_id

    def __str__(self):
        return f'BitfinexChannel(channel_id={self.channel_id}, symbol={self.symbol}, status={self.status})'


class BaseExtractor(abc.ABC):
    def __init__(self, repo: BasePriceRepo):
        self.repo = repo
        logger.info(f'initialized {self.__class__.__name__} with repo {self.repo.__class__.__name__}')

    @abc.abstractmethod
    async def run(self):
        pass

    async def save(self, price_record):
        await self.repo.insert(price_record)


class BitfinexExtractor(BaseExtractor):
    """
    Bitfinex websocket channel: https://docs.bitfinex.com/docs/ws-general
    Uses v2.
    """
    def __init__(self, repo):
        super().__init__(repo)
        self.websocket_resource_url = 'wss://api-pub.bitfinex.com/ws/2'
        self.pairs_resource_url = 'https://api-pub.bitfinex.com/v2/conf/pub:list:pair:exchange'
        self.subscribes_per_websocket = 30
        self.channels = []
        self.websockets = []

    async def get_pairs(self):
        logger.info('getting available symbols')
        async with aiohttp.ClientSession() as session:
            async with session.get(self.pairs_resource_url) as resp:
                pairs = await resp.json()
                logger.info(f'got {len(pairs[0])} symbols')
                return pairs[0]  # Pairs are nested inside.

    async def subscribe(self, channel, websocket):
        subscribe_template = Template('{"event": "subscribe", "channel": "ticker", "symbol": "t$pair"}')
        message = subscribe_template.substitute(pair=channel.symbol)
        logger.debug(f'subscribing with msg={message}')
        await websocket.send(message)

    def get_channel(self, *, channel_id=None, symbol=None) -> Optional[BitfinexChannel]:
        if symbol:
            for channel in self.channels:
                if channel.symbol == symbol:
                    return channel
        elif channel_id:
            for channel in self.channels:
                if channel.channel_id == channel_id:
                    return channel
        return None

    async def _listen(self, websocket):
        logger.info(f'started listening on websocket {websocket}')
        async for message in websocket:
            await self.process_message(message)

    async def listen(self):
        # Start listening to each websocket.
        for websocket in self.websockets:
            asyncio.create_task(self._listen(websocket),
                                name='websocket listening: ' + str(websocket))

    async def process_price_message(self, message):
        """
        Message structure:
        [CHANNEL_ID,
          [BID, BID_SIZE, ASK, ASK_SIZE, DAILY_CHANGE, DAILY_CHANGE_RELATIVE, LAST_PRICE, VOLUME, HIGH, LOW]
        ]
        Example:
        [17559,
          [4.0681, 29735.6070077, 4.077, 28107.59973336, -0.3189, -0.0728, 4.0606, 35598.65661632, 4.3795,
           4.0606]
        ]
        """
        channel = self.get_channel(channel_id=message[0])
        if not channel:
            raise Exception("No channel found for an upcoming message!")
        if ':' in channel.symbol:
            base, quote = channel.symbol.split(':')
        else:
            base, quote = channel.symbol[:3], channel.symbol[3:]
        r = PriceRecord(exchange='Bitfinex',
                        symbol=channel.symbol,
                        base=base,
                        quote=quote,
                        last_price=message[1][8])
        await self.save(r)

    async def process_message(self, message: str):
        logger.debug(f'processing message from bitfinex {message}')
        message = json.loads(message)
        try:
            if isinstance(message, list):
                if isinstance(message[0], int):
                    if message[1] == 'hb':
                        return  # Ignore heartbeat events.
                    # Assume it is a Ticker event with price where message[0] is a channel ID.
                    await self.process_price_message(message)
            elif isinstance(message, dict):
                if message['event'] == 'subscribed':
                    channel = self.get_channel(symbol=message['pair'])  # pair is without 't' in the beginning
                    if message['pair'] != channel.symbol:
                        raise Exception(f'subscription failed for an unknown symbol mismatch for '
                                        f'{message["pair"]} != {channel.symbol}')
                    channel.status = ChannelStatus.SUBSCRIBED
                    channel.channel_id = message['chanId']
                    logger.info(f'subscribed to {channel.symbol} with id {channel.channel_id}')
        except Exception as e:
            logger.exception(e)

    async def run(self):
        logger.info(f'{self.__class__.__name__} extractor has been run')
        pairs = await self.get_pairs()
        for pair in pairs:
            channel = BitfinexChannel(None, pair, ChannelStatus.CREATED, None)
            self.channels.append(channel)

        # All websocket connections have a limit of 30 subscriptions to public market data feed channels.
        # So split all the pairs we have into chunks of 30 and create a websocket for each.
        n = self.subscribes_per_websocket
        chunks_of_channels = [self.channels[i:i + n] for i in range(0, len(self.channels), n)]

        # Create Websockets that will attached to each symbol.
        logger.info(f'creating {len(chunks_of_channels)}  connection to process all the symbols')
        tasks = []
        for chunk_of_channels in chunks_of_channels:
            websocket = await websockets.connect(self.websocket_resource_url)
            self.websockets.append(websocket)
            for channel in chunk_of_channels:
                channel.connection = websocket
                channel.status = ChannelStatus.SUBSCRIBING
                task = asyncio.create_task(self.subscribe(channel, websocket),
                                           name=self.__class__.__name__ + channel.symbol)
                tasks.append(task)

        await self.listen()


class KucoinExtractor(BaseExtractor):
    """
    Uses python-kucoin library and websockets.
    """
    async def run(self):
        async def handle_evt(message):
            logger.debug(f'processing message from kucoin {message}')
            try:
                if message['topic'] != '/market/ticker:all':
                    return
                base, quote = message['subject'].split('-')
                record = PriceRecord(exchange='Kucoin',
                                     symbol=message['subject'],
                                     base=base,
                                     quote=quote,
                                     last_price=message['data']['price'],
                                     time_received=datetime.datetime.fromtimestamp(message['data']['time'] / 1000)  # ms
                                     )
                await self.repo.insert(record)
            except Exception as e:
                logger.exception(f'bad message from kucoin: {e}')

        loop = asyncio.get_event_loop()
        client = Client('', '', '')
        ksm = await KucoinSocketManager.create(loop, client, handle_evt)

        logger.info(f'subscribed to all symbols on Kucoin')
        await ksm.subscribe('/market/ticker:all')


class BinanceExtractor(BaseExtractor):
    """
    Uses python-binance library with REST API calls.
    """
    @staticmethod
    def get_symbol_mapping(exchange_info: dict) -> dict:
        result = {}
        for symbol in exchange_info['symbols']:
            s = symbol['symbol']
            base = symbol['baseAsset']
            quote = symbol['quoteAsset']
            result[s] = (base, quote)
        return result

    async def run(self):
        client = await AsyncClient.create()
        exchange_info = await client.get_exchange_info()
        symbols_mapping = self.get_symbol_mapping(exchange_info)
        logger.info(f'got exchange info from Binance: {len(symbols_mapping)} symbols exist')

        while True:
            tickers = await client.get_all_tickers()
            logger.debug(f'Binance {tickers=}')
            start = datetime.datetime.now()
            for symbol in tickers:
                base, quote = symbols_mapping[symbol['symbol']]
                record = PriceRecord(exchange='Binance',
                                     symbol=symbol['symbol'],
                                     base=base,
                                     quote=quote,
                                     last_price=symbol['price'],
                                     )
                await self.repo.insert(record)

            await sleep(1)
