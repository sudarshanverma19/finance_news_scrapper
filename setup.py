from setuptools import find_packages, setup

setup(
    name="news_scraper",
    description="Scrapes indian market news from popular online indian news outlets",
    version="0.2.0",
    packages=find_packages(),
    install_requires=[
        "scrapy",
        "pandas",
        "fake-useragent",
        "python-dotenv",
        "streamlit",
        "plotly",
        "sqlalchemy",
    ],
    extras_require={
        "test": [
            "pytest",
            "syrupy",
        ],
    },
    url="https://github.com/desiquant/news_scraper",
    python_requires=">=3.10",
)
