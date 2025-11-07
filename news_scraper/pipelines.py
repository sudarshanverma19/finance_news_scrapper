"""
Scrapy pipelines for processing scraped items.
"""
from .database import NewsDatabase


class SQLitePipeline:
    """Pipeline to save scraped articles to SQLite database."""
    
    def __init__(self):
        self.db = None
        
    def open_spider(self, spider):
        """Initialize database connection when spider opens."""
        self.db = NewsDatabase()
        spider.logger.info(f"SQLite database initialized at: {self.db.db_path}")
        
    def close_spider(self, spider):
        """Close database connection when spider closes."""
        if self.db:
            self.db.close()
            spider.logger.info("SQLite database connection closed")
    
    def process_item(self, item, spider):
        """Process each scraped item and save to database."""
        article_dict = dict(item)
        inserted = self.db.insert_article(article_dict, spider.name)
        
        if inserted:
            spider.logger.debug(f"Inserted article: {article_dict.get('url')}")
        else:
            spider.logger.debug(f"Skipped duplicate article: {article_dict.get('url')}")
        
        return item
