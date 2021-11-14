import asyncio
import datetime
from asyncio import sleep

from extract import BitfinexExtractor, KucoinExtractor, BinanceExtractor
from repo import ClickhousePriceRepo, TestPriceRepo


async def main():
    repo = ClickhousePriceRepo()
    extractors = [BitfinexExtractor(repo), KucoinExtractor(repo), BinanceExtractor(repo)]

    for extractor in extractors:
        asyncio.create_task(extractor.run(), name=extractor.__class__.__name__)

    while True:
        await sleep(200)
        data = await repo.get_average_prices_on_date('ETH', datetime.datetime.now() - datetime.timedelta(minutes=2))
        for l in data:
            print(f'{l[0]:6} {l[1]}')


if __name__ == '__main__':
    asyncio.run(main())
