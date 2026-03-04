import asyncio
from src.scraper.state_legislation.bahia import BahiaLegislaScraper


async def main():
    # Only testing 1994 to see if retries kick in
    scraper = BahiaLegislaScraper(year_start=1994, year_end=1994, verbose=True)
    await scraper.scrape()
    await scraper.cleanup()


if __name__ == "__main__":
    asyncio.run(main())
