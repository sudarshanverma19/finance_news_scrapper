# Define here the models for your spider middleware
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/spider-middleware.html

import os
from scrapy import Spider, crawler, signals
from scrapy.exceptions import IgnoreRequest
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()
from .database import NewsDatabase


class NewsScraperDownloaderMiddleware:
    output_urls = []

    @classmethod
    def from_crawler(cls, crawler: crawler):
        s = cls()
        crawler.signals.connect(s.spider_opened, signal=signals.spider_opened)
        return s

    def spider_opened(self, spider: Spider):
        spider.logger.info("Spider opened: %s" % spider.name)

        # load already parsed urls from database
        if spider.settings.getbool("SKIP_OUTPUT_URLS"):
            try:
                db = NewsDatabase()
                self.output_urls = db.get_unique_urls(spider_name=spider.name)
                db.close()
                
                spider.logger.info(
                    "Already scraped %s URLs for spider: %s"
                    % (len(self.output_urls), spider.name)
                )
            except Exception as e:
                spider.logger.warning(f"Could not load existing URLs: {e}")
                self.output_urls = []

    def process_request(self, request, spider: Spider):
        # ignore urls which are already processed
        if request.url in self.output_urls:
            spider.logger.info("Ignoring Request (already in output): %s", request.url)
            raise IgnoreRequest

        return None
