"""
SQLite database module for storing scraped news articles.
"""
import sqlite3
import csv
from datetime import datetime
from pathlib import Path
from typing import List, Optional, Dict, Any


class NewsDatabase:
    def __init__(self, db_path: str = "news_scraper.db"):
        """Initialize database connection and create tables if needed."""
        self.db_path = db_path
        self.conn = None
        self.init_db()

    def init_db(self):
        """Create database tables and indices if they don't exist."""
        self.conn = sqlite3.connect(self.db_path, check_same_thread=False)
        cursor = self.conn.cursor()
        
        # Create articles table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS articles (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                url TEXT UNIQUE NOT NULL,
                title TEXT,
                description TEXT,
                author TEXT,
                article_text TEXT,
                date_published TEXT,
                date_modified TEXT,
                scrapy_scraped_at TEXT,
                scrapy_parsed_at TEXT,
                paywall TEXT,
                spider_name TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Create indices for faster queries
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_spider_name ON articles(spider_name)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_date_published ON articles(date_published)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_date_modified ON articles(date_modified)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_created_at ON articles(created_at)")
        
        self.conn.commit()

    def insert_article(self, article: Dict[str, Any], spider_name: str) -> bool:
        """Insert a single article into the database."""
        try:
            cursor = self.conn.cursor()
            cursor.execute("""
                INSERT OR IGNORE INTO articles 
                (url, title, description, author, article_text, date_published, 
                 date_modified, scrapy_scraped_at, scrapy_parsed_at, paywall, spider_name)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                article.get('url'),
                article.get('title'),
                article.get('description'),
                article.get('author'),
                article.get('article_text'),
                article.get('date_published'),
                article.get('date_modified'),
                article.get('scrapy_scraped_at'),
                article.get('scrapy_parsed_at'),
                article.get('paywall'),
                spider_name
            ))
            self.conn.commit()
            return cursor.rowcount > 0
        except Exception as e:
            print(f"Error inserting article: {e}")
            return False

    def get_articles(
        self, 
        spider_name: Optional[str] = None,
        search_query: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        limit: int = 100,
        offset: int = 0
    ) -> List[Dict[str, Any]]:
        """Get articles with optional filtering."""
        query = "SELECT * FROM articles WHERE 1=1"
        params = []
        
        if spider_name:
            query += " AND spider_name = ?"
            params.append(spider_name)
        
        if search_query:
            query += " AND (title LIKE ? OR description LIKE ? OR article_text LIKE ?)"
            search_param = f"%{search_query}%"
            params.extend([search_param, search_param, search_param])
        
        if start_date:
            query += " AND date_published >= ?"
            params.append(start_date)
        
        if end_date:
            query += " AND date_published <= ?"
            params.append(end_date)
        
        query += " ORDER BY date_published DESC LIMIT ? OFFSET ?"
        params.extend([limit, offset])
        
        cursor = self.conn.cursor()
        cursor.execute(query, params)
        
        columns = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
        return [dict(zip(columns, row)) for row in rows]

    def get_article_count(
        self,
        spider_name: Optional[str] = None,
        search_query: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None
    ) -> int:
        """Get count of articles matching filters."""
        query = "SELECT COUNT(*) FROM articles WHERE 1=1"
        params = []
        
        if spider_name:
            query += " AND spider_name = ?"
            params.append(spider_name)
        
        if search_query:
            query += " AND (title LIKE ? OR description LIKE ? OR article_text LIKE ?)"
            search_param = f"%{search_query}%"
            params.extend([search_param, search_param, search_param])
        
        if start_date:
            query += " AND date_published >= ?"
            params.append(start_date)
        
        if end_date:
            query += " AND date_published <= ?"
            params.append(end_date)
        
        cursor = self.conn.cursor()
        cursor.execute(query, params)
        return cursor.fetchone()[0]

    def get_statistics(self) -> Dict[str, Any]:
        """Get statistics about scraped articles."""
        cursor = self.conn.cursor()
        
        # Total articles
        cursor.execute("SELECT COUNT(*) FROM articles")
        total = cursor.fetchone()[0]
        
        # Articles per spider
        cursor.execute("""
            SELECT spider_name, COUNT(*) as count 
            FROM articles 
            GROUP BY spider_name 
            ORDER BY count DESC
        """)
        by_spider = cursor.fetchall()
        
        # Articles per day (last 30 days)
        cursor.execute("""
            SELECT DATE(date_published) as date, COUNT(*) as count
            FROM articles
            WHERE date_published IS NOT NULL
            AND date_published >= date('now', '-30 days')
            GROUP BY DATE(date_published)
            ORDER BY date DESC
        """)
        by_day = cursor.fetchall()
        
        # Latest scrape time
        cursor.execute("SELECT MAX(created_at) FROM articles")
        latest = cursor.fetchone()[0]
        
        return {
            'total_articles': total,
            'by_spider': by_spider,
            'by_day': by_day,
            'latest_scrape': latest
        }

    def get_unique_urls(self, spider_name: Optional[str] = None) -> List[str]:
        """Get list of already scraped URLs for deduplication."""
        query = "SELECT url FROM articles"
        params = []
        
        if spider_name:
            query += " WHERE spider_name = ?"
            params.append(spider_name)
        
        cursor = self.conn.cursor()
        cursor.execute(query, params)
        return [row[0] for row in cursor.fetchall()]

    def export_to_csv(
        self,
        filepath: str,
        spider_name: Optional[str] = None,
        search_query: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None
    ):
        """Export filtered articles to CSV."""
        articles = self.get_articles(
            spider_name=spider_name,
            search_query=search_query,
            start_date=start_date,
            end_date=end_date,
            limit=1000000  # Get all matching articles
        )
        
        if articles:
            with open(filepath, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=articles[0].keys())
                writer.writeheader()
                writer.writerows(articles)
        
        return len(articles)

    def close(self):
        """Close database connection."""
        if self.conn:
            self.conn.close()
