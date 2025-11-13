"""
Streamlit Dashboard for News Scraper
A clean interface to view, search, and manage scraped news articles.
"""
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
from pathlib import Path
import sys
import subprocess
import threading
import time
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent))

from news_scraper.database import NewsDatabase
from news_scraper.spiders import (
    BusinessStandardSpider,
    BusinessTodaySpider,
    EconomicTimesSpider,
    FinancialExpressSpider,
    FirstPostSpider,
    FreePressJournalSpider,
    IndianExpressSpider,
    MoneyControlSpider,
    NDTVProfitSpider,
    News18Spider,
    OutlookIndiaSpider,
    TheHinduBusinessLineSpider,
    TheHinduSpider,
    ZeeNewsSpider,
    CnbcTv18Spider
)
from news_scraper import gemini_client

# Page configuration
st.set_page_config(
    page_title="News Scraper Dashboard",
    page_icon="📰",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for better styling
st.markdown("""
    <style>
    .main {
        padding-top: 2rem;
    }
    .stAlert {
        margin-top: 1rem;
    }
    .article-card {
        padding: 1.5rem;
        border-radius: 0.5rem;
        border: 1px solid #e0e0e0;
        margin-bottom: 1rem;
        background-color: #ffffff;
    }
    .article-title {
        font-size: 1.2rem;
        font-weight: 600;
        color: #1f2937;
        margin-bottom: 0.5rem;
    }
    .article-meta {
        font-size: 0.875rem;
        color: #6b7280;
        margin-bottom: 0.5rem;
    }
    .article-description {
        font-size: 1rem;
        color: #374151;
        margin-bottom: 0.5rem;
    }
    </style>
""", unsafe_allow_html=True)

# Initialize database
@st.cache_resource
def get_database():
    return NewsDatabase()

db = get_database()

# Available spiders
SPIDERS = {
    "All Sources": None,
    "Business Standard": BusinessStandardSpider,
    "Business Today": BusinessTodaySpider,
    "Economic Times": EconomicTimesSpider,
    "Financial Express": FinancialExpressSpider,
    "First Post": FirstPostSpider,
    "Free Press Journal": FreePressJournalSpider,
    "Indian Express": IndianExpressSpider,
    "Money Control": MoneyControlSpider,
    "NDTV Profit": NDTVProfitSpider,
    "News18": News18Spider,
    "Outlook India": OutlookIndiaSpider,
    "The Hindu": TheHinduSpider,
    "The Hindu Business Line": TheHinduBusinessLineSpider,
    "Zee News": ZeeNewsSpider,
    "CNBC TV18": CnbcTv18Spider,
}

# Initialize session state
if 'scraping_status' not in st.session_state:
    st.session_state.scraping_status = {}
if 'scraping_active' not in st.session_state:
    st.session_state.scraping_active = False

def run_spider_command(spider_name: str):
    """Run a spider using scrapy crawl command."""
    try:
        # Run scrapy crawl command
        cmd = f"scrapy crawl {spider_name}"
        process = subprocess.Popen(
            cmd,
            shell=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            cwd=str(Path(__file__).parent)
        )
        
        stdout, stderr = process.communicate()
        
        return {
            'spider': spider_name,
            'success': process.returncode == 0,
            'stdout': stdout,
            'stderr': stderr
        }
            
    except Exception as e:
        return {
            'spider': spider_name,
            'success': False,
            'error': str(e)
        }

def start_scraping(spider_names: list):
    """Start scraping - runs directly without background thread."""
    with st.spinner(f"Scraping {len(spider_names)} source(s)..."):
        results = []
        for spider_name in spider_names:
            st.info(f"🕷️ Running {spider_name}...")
            result = run_spider_command(spider_name)
            results.append(result)
            
            if result['success']:
                st.success(f"✅ {spider_name} completed successfully!")
            else:
                error_msg = result.get('error', result.get('stderr', 'Unknown error'))
                st.error(f"❌ {spider_name} failed: {error_msg[:200]}")
        
        return results

# Sidebar
with st.sidebar:
    st.title("📰 News Scraper")
    st.markdown("---")
    
    # Navigation
    page = st.radio(
        "Navigation",
        ["⚙️ Scraper Control", "📰 Articles", "🔍 Search"],  # Removed Dashboard and Statistics temporarily
        label_visibility="collapsed"
    )
    
    st.markdown("---")
    st.caption("Made with ❤️ using Streamlit")

# Main content based on selected page
if page == "⚙️ Scraper Control":
    st.title("⚙️ Scraper Control")
    st.markdown("Manually trigger scraping from specific news sources.")
    
    col1, col2 = st.columns([2, 1])
    
    with col1:
        selected_sources = st.multiselect(
            "Select News Sources to Scrape",
            options=[name for name, spider in SPIDERS.items() if spider is not None],
            default=[]
        )
    
    with col2:
        st.write("")
        st.write("")
        scrape_button = st.button("🚀 Start Scraping", type="primary", use_container_width=True)
    
    if scrape_button:
        if not selected_sources:
            st.error("Please select at least one news source!")
        else:
            # Convert display names to spider names
            spider_names = []
            for source in selected_sources:
                spider_class = SPIDERS[source]
                if spider_class:
                    spider_names.append(spider_class.name)
            
            # Run scraping
            results = start_scraping(spider_names)
            
            # Show summary
            successful = sum(1 for r in results if r['success'])
            st.info(f"✅ {successful}/{len(results)} sources scraped successfully!")
    
    # Quick scrape all button
    st.markdown("---")
    st.markdown("### ⚡ Bulk Scraping")
    st.warning("⚠️ Scraping all sources may take several minutes")
    
    if st.button("🌐 Scrape All 15 Sources", type="secondary", use_container_width=True):
        all_spider_names = [spider.name for name, spider in SPIDERS.items() if spider is not None]
        results = start_scraping(all_spider_names)
        successful = sum(1 for r in results if r['success'])
        st.info(f"✅ {successful}/{len(results)} sources scraped successfully!")

# TEMPORARILY DISABLED - Dashboard page (uncomment to restore)
# elif page == "📊 Dashboard":
#     st.title("📊 Dashboard Overview")
#     
#     # Get statistics
#     stats = db.get_statistics()
#     
#     # Top metrics
#     col1, col2, col3, col4 = st.columns(4)
#     
#     with col1:
#         st.metric("Total Articles", f"{stats['total_articles']:,}")
#     
#     with col2:
#         if stats['by_spider']:
#             st.metric("Active Sources", len(stats['by_spider']))
#         else:
#             st.metric("Active Sources", 0)
#     
#     with col3:
#         if stats['by_day']:
#             today_count = stats['by_day'][0][1] if stats['by_day'] else 0
#             st.metric("Today's Articles", today_count)
#         else:
#             st.metric("Today's Articles", 0)
#     
#     with col4:
#         if stats['latest_scrape']:
#             latest = datetime.fromisoformat(stats['latest_scrape'])
#             st.metric("Last Updated", latest.strftime("%H:%M"))
#         else:
#             st.metric("Last Updated", "Never")
#     
#     st.markdown("---")
#     
#     # Charts
#     col1, col2 = st.columns(2)
#     
#     with col1:
#         st.subheader("Articles by Source")
#         if stats['by_spider']:
#             df_spider = pd.DataFrame(stats['by_spider'], columns=['Source', 'Count'])
#             fig = px.bar(
#                 df_spider,
#                 x='Count',
#                 y='Source',
#                 orientation='h',
#                 color='Count',
#                 color_continuous_scale='blues'
#             )
#             fig.update_layout(height=400, showlegend=False)
#             st.plotly_chart(fig, use_container_width=True)
#         else:
#             st.info("No data available. Start scraping to see statistics!")
#     
#     with col2:
#         st.subheader("Articles Over Time (Last 30 Days)")
#         if stats['by_day']:
#             df_day = pd.DataFrame(stats['by_day'], columns=['Date', 'Count'])
#             df_day['Date'] = pd.to_datetime(df_day['Date'])
#             fig = px.line(
#                 df_day,
#                 x='Date',
#                 y='Count',
#                 markers=True
#             )
#             fig.update_layout(height=400)
#             st.plotly_chart(fig, use_container_width=True)
#         else:
#             st.info("No data available. Start scraping to see trends!")

if page == "📰 Articles":
    st.title("📰 Recent Articles")
    
    # Filters
    col1, col2, col3 = st.columns([2, 2, 1])
    
    with col1:
        source_filter = st.selectbox("Filter by Source", list(SPIDERS.keys()))
    
    with col2:
        date_range = st.date_input(
            "Date Range",
            value=(datetime.now() - timedelta(days=7), datetime.now()),
            max_value=datetime.now()
        )
    
    with col3:
        per_page = st.selectbox("Articles per page", [10, 25, 50, 100], index=1)
    
    # Pagination
    if 'article_page' not in st.session_state:
        st.session_state.article_page = 0
    
    # Get spider name for filter
    spider_name = None
    if source_filter != "All Sources":
        spider_class = SPIDERS[source_filter]
        spider_name = spider_class.name if spider_class else None
    
    # Get articles
    start_date = date_range[0].isoformat() if len(date_range) > 0 else None
    end_date = date_range[1].isoformat() if len(date_range) > 1 else None
    
    articles = db.get_articles(
        spider_name=spider_name,
        start_date=start_date,
        end_date=end_date,
        limit=per_page,
        offset=st.session_state.article_page * per_page
    )
    
    total_count = db.get_article_count(
        spider_name=spider_name,
        start_date=start_date,
        end_date=end_date
    )
    
    # Display articles
    if len(articles) > 0:
        st.markdown(f"Showing {len(articles)} of {total_count} articles")
        
        for article in articles:
            # Parse date if exists
            date_str = 'No Date'
            if article.get('date_published'):
                try:
                    from datetime import datetime as dt
                    date_obj = dt.fromisoformat(str(article['date_published']).replace('Z', '+00:00'))
                    date_str = date_obj.strftime('%B %d, %Y')
                except:
                    date_str = str(article['date_published'])
            
            with st.container():
                st.markdown(f"""
                <div class="article-card">
                    <div class="article-title">{article.get('title') or 'No Title'}</div>
                    <div class="article-meta">
                        📰 {article.get('spider_name')} | 
                        📅 {date_str} | 
                        ✍️ {article.get('author') or 'Unknown Author'}
                    </div>
                    <div class="article-description">{article.get('description') or ''}</div>
                </div>
                """, unsafe_allow_html=True)
                
                # Center-aligned buttons
                col1, col2, col3 = st.columns([1, 2, 1])
                with col2:
                    if st.button(f"🔗 Open Article", key=f"link_{article['id']}", use_container_width=True):
                        st.markdown(f"[Open in browser]({article['url']})")
                
                # Read More expander (full width, centered)
                with st.expander("📖 Read Full Article", expanded=False):
                    st.markdown(f"**Full Text:**")
                    st.write(article.get('article_text') or "No article text available")
                
                st.markdown("---")
        
        # Pagination controls
        col1, col2, col3 = st.columns([1, 2, 1])
        with col1:
            if st.session_state.article_page > 0:
                if st.button("⬅️ Previous"):
                    st.session_state.article_page -= 1
                    st.rerun()
        
        with col2:
            total_pages = (total_count + per_page - 1) // per_page
            st.markdown(f"<center>Page {st.session_state.article_page + 1} of {total_pages}</center>", unsafe_allow_html=True)
        
        with col3:
            if (st.session_state.article_page + 1) * per_page < total_count:
                if st.button("Next ➡️"):
                    st.session_state.article_page += 1
                    st.rerun()
    else:
        st.info("No articles found. Try adjusting your filters or start scraping!")

elif page == "🔍 Search":
    st.title("🔍 Search Articles")
    
    # Search bar
    search_query = st.text_input("🔎 Search articles by title, description, or content", "")
    
    col1, col2 = st.columns([3, 1])
    with col1:
        source_filter = st.selectbox("Filter by Source", list(SPIDERS.keys()), key="search_source")
    with col2:
        search_limit = st.selectbox("Max Results", [25, 50, 100, 200], index=1)
    
    if search_query:
        spider_name = None
        if source_filter != "All Sources":
            spider_class = SPIDERS[source_filter]
            spider_name = spider_class.name if spider_class else None
        
        results = db.get_articles(
            spider_name=spider_name,
            search_query=search_query,
            limit=search_limit
        )
        
        st.markdown(f"### Found {len(results)} results")
        
        if len(results) > 0:
            # Action buttons
            col1, col2 = st.columns(2)
            
            with col1:
                # Export button
                if st.button("📥 Export Results to CSV"):
                    export_path = f"search_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
                    count = db.export_to_csv(
                        export_path,
                        spider_name=spider_name,
                        search_query=search_query
                    )
                    st.success(f"Exported {count} results to {export_path}")
            
            with col2:
                # Summarize button - only show if Gemini is configured
                gemini_available = False
                try:
                    # Try to get API key from environment or Streamlit secrets
                    import os
                    api_key = os.getenv("GEMINI_API_KEY") or st.secrets.get("GEMINI_API_KEY", None)
                    if api_key:
                        os.environ["GEMINI_API_KEY"] = api_key
                        gemini_available = True
                except:
                    pass
                
                if gemini_available and st.button("✨ Summarize Results with AI"):
                    with st.spinner(f"🤖 Analyzing up to 8 articles about '{search_query}'... This may take 1-2 minutes due to API rate limits."):
                        try:
                            # Show a note about article content and rate limits
                            st.info("💡 **Note:** Free tier has 10 requests/minute limit. Processing up to 8 articles with delays to avoid quota errors. For faster results, upgrade your Gemini API plan.")
                            
                            summary = gemini_client.summarize_multiple_articles(
                                results,
                                topic=search_query,
                                max_articles=8  # Reduced from 20 to stay within free tier limits
                            )
                            
                            st.success("✅ Summary generated!")
                            st.markdown("---")
                            st.markdown(f"### 📊 AI Summary: {search_query}")
                            st.markdown(summary)
                            st.markdown("---")
                            
                        except Exception as e:
                            st.error(f"❌ Summarization failed: {str(e)}")
                            if "quota" in str(e).lower() or "429" in str(e):
                                st.warning("⏳ **Rate Limit Hit**: Please wait 60 seconds and try again. Free tier allows 10 requests per minute.")
                                st.info("💡 **Tips to avoid rate limits:**\n- Search for more specific topics (fewer articles)\n- Wait a minute between summarizations\n- Consider upgrading your Gemini API plan for higher limits")
                            else:
                                st.info("💡 Make sure your GEMINI_API_KEY is set correctly in environment variables or Streamlit secrets.")
                                st.info("💡 Also check that the scraped articles have full text content (not just titles).")

            
            st.markdown("---")
            
            for article in results:
                # Parse date if exists
                date_str = 'No Date'
                if article.get('date_published'):
                    try:
                        from datetime import datetime as dt
                        date_obj = dt.fromisoformat(str(article['date_published']).replace('Z', '+00:00'))
                        date_str = date_obj.strftime('%B %d, %Y')
                    except:
                        date_str = str(article['date_published'])
                
                with st.container():
                    st.markdown(f"""
                    <div class="article-card">
                        <div class="article-title">{article.get('title') or 'No Title'}</div>
                        <div class="article-meta">
                            📰 {article.get('spider_name')} | 
                            📅 {date_str}
                        </div>
                        <div class="article-description">{article.get('description') or ''}</div>
                    </div>
                    """, unsafe_allow_html=True)
                    
                    # Center-aligned link button
                    col1, col2, col3 = st.columns([1, 2, 1])
                    with col2:
                        st.markdown(f"[🔗 Open Article]({article['url']})")
                    
                    # Read More expander (full width, centered)
                    with st.expander("📖 Read Full Article", expanded=False):
                        st.write(article.get('article_text') or "No content available")
                    
                    st.markdown("---")
        else:
            st.warning("No articles found matching your search.")
    else:
        st.info("Enter a search query to find articles.")

# TEMPORARILY DISABLED - Statistics page (uncomment to restore)
# elif page == "📈 Statistics":
#     st.title("📈 Detailed Statistics")
#     
#     stats = db.get_statistics()
#     
#     # Source breakdown
#     st.subheader("Article Distribution by Source")
#     if stats['by_spider']:
#         df_spider = pd.DataFrame(stats['by_spider'], columns=['Source', 'Count'])
#         
#         col1, col2 = st.columns(2)
#         
#         with col1:
#             fig = px.pie(
#                 df_spider,
#                 values='Count',
#                 names='Source',
#                 title='Article Count by Source'
#             )
#             st.plotly_chart(fig, use_container_width=True)
#         
#         with col2:
#             st.dataframe(
#                 df_spider.sort_values('Count', ascending=False),
#                 hide_index=True,
#                 use_container_width=True
#             )
#     else:
#         st.info("No data available yet.")
#     
#     st.markdown("---")
#     
#     # Trend analysis
#     st.subheader("Article Trends Over Time")
#     if stats['by_day']:
#         df_day = pd.DataFrame(stats['by_day'], columns=['Date', 'Count'])
#         df_day['Date'] = pd.to_datetime(df_day['Date'])
#         
#         fig = go.Figure()
#         fig.add_trace(go.Scatter(
#             x=df_day['Date'],
#             y=df_day['Count'],
#             mode='lines+markers',
#             name='Articles',
#             fill='tozeroy'
#         ))
#         fig.update_layout(
#             title='Daily Article Count (Last 30 Days)',
#             xaxis_title='Date',
#             yaxis_title='Number of Articles',
#             height=400
#         )
#         st.plotly_chart(fig, use_container_width=True)
#         
#         # Summary stats
#         col1, col2, col3 = st.columns(3)
#         with col1:
#             st.metric("Total (30 days)", df_day['Count'].sum())
#         with col2:
#             st.metric("Daily Average", f"{df_day['Count'].mean():.1f}")
#         with col3:
#             st.metric("Peak Day", df_day['Count'].max())
#     else:
#         st.info("No trend data available yet.")

# Footer
st.markdown("---")
st.markdown("""
<div style='text-align: center; color: #6b7280; font-size: 0.875rem;'>
    News Scraper Dashboard | Built with Streamlit 🎈
</div>
""", unsafe_allow_html=True)
