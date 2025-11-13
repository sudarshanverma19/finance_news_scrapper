"""
Gemini API client for summarizing news articles.
Uses chunked map-reduce approach to handle large texts.
"""
import os
import time
from typing import List, Optional
import google.generativeai as genai

# Configuration from environment variables
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
GEMINI_MODEL = os.getenv("GEMINI_MODEL", "gemini-2.5-flash")  # Default to 2.5 flash for speed/cost

# Configure the API
if GEMINI_API_KEY:
    genai.configure(api_key=GEMINI_API_KEY)


def _chunk_text_by_paragraph(text: str, approx_chars_per_chunk: int = 15000) -> List[str]:
    """
    Split text into manageable chunks by paragraphs.
    Keeps paragraphs together to maintain context.
    """
    if not text:
        return []
    
    paragraphs = [p.strip() for p in text.split("\n\n") if p.strip()]
    if not paragraphs:
        paragraphs = [text]
    
    chunks = []
    current_chunk = ""
    
    for paragraph in paragraphs:
        if len(current_chunk) + len(paragraph) + 2 <= approx_chars_per_chunk:
            current_chunk = (current_chunk + "\n\n" + paragraph).strip()
        else:
            if current_chunk:
                chunks.append(current_chunk)
            current_chunk = paragraph
    
    if current_chunk:
        chunks.append(current_chunk)
    
    return chunks


def _call_gemini(prompt: str, temperature: float = 0.2, max_tokens: int = 500) -> str:
    """
    Call Gemini API with the given prompt.
    Includes retry logic for rate limits (429 errors).
    """
    if not GEMINI_API_KEY:
        raise RuntimeError("GEMINI_API_KEY not set in environment or st.secrets")
    
    max_retries = 3
    retry_delay = 12  # Free tier: 10 requests per minute = 6 seconds between requests minimum
    
    for attempt in range(max_retries):
        try:
            model = genai.GenerativeModel(GEMINI_MODEL)
            
            generation_config = genai.types.GenerationConfig(
                temperature=temperature,
                max_output_tokens=max_tokens,
            )
            
            response = model.generate_content(
                prompt,
                generation_config=generation_config
            )
            
            # Add delay to avoid rate limits (free tier: 10 req/min)
            time.sleep(6.5)  # ~6-7 seconds between requests
            
            # Safely extract text from response
            try:
                if response.text:
                    return response.text.strip()
            except ValueError:
                # Handle blocked responses (safety filters, etc.)
                pass
            
            # Check if response was blocked
            if hasattr(response, 'prompt_feedback') and response.prompt_feedback:
                raise RuntimeError(f"Content blocked by safety filters: {response.prompt_feedback}")
            
            # Try to get text from candidates
            if response.candidates and len(response.candidates) > 0:
                candidate = response.candidates[0]
                if hasattr(candidate, 'content') and candidate.content.parts:
                    return candidate.content.parts[0].text.strip()
                elif hasattr(candidate, 'finish_reason'):
                    finish_reasons = {
                        0: "FINISH_REASON_UNSPECIFIED",
                        1: "STOP (completed successfully)",
                        2: "MAX_TOKENS (response too long)",
                        3: "SAFETY (blocked by safety filters)",
                        4: "RECITATION (blocked due to recitation)",
                        5: "OTHER"
                    }
                    reason = finish_reasons.get(candidate.finish_reason, "UNKNOWN")
                    raise RuntimeError(f"Response generation stopped: {reason}")
            
            raise RuntimeError("No valid response text returned from Gemini API")
            
        except Exception as e:
            error_msg = str(e)
            
            # Check if it's a rate limit error (429)
            if "429" in error_msg or "quota" in error_msg.lower():
                if attempt < max_retries - 1:
                    wait_time = retry_delay * (attempt + 1)
                    print(f"Rate limit hit, waiting {wait_time} seconds before retry {attempt + 2}/{max_retries}...")
                    time.sleep(wait_time)
                    continue
                else:
                    raise RuntimeError(f"Rate limit exceeded after {max_retries} retries. Please wait a minute and try again with fewer articles.")
            else:
                # Other error, raise immediately
                raise RuntimeError(f"Gemini API call failed: {error_msg}")
    
    raise RuntimeError("Failed after maximum retries")


def summarize_article(article_text: str, title: str = "", bullet_points: bool = True) -> str:
    """
    Summarize a single article using chunked approach if needed.
    
    Args:
        article_text: The full text of the article
        title: Optional article title for context
        bullet_points: If True, return bullet point summary
    
    Returns:
        Summary text
    """
    if not article_text:
        return "No content to summarize."
    
    # If article is short enough, summarize directly
    if len(article_text) <= 15000:
        format_instruction = "as bullet points" if bullet_points else "in a concise paragraph"
        prompt = f"""Summarize the following article {format_instruction}. Focus on key facts, events, and conclusions.

Title: {title}

Article:
{article_text}

Summary:"""
        return _call_gemini(prompt, temperature=0.2, max_tokens=400)
    
    # For longer articles, use map-reduce approach
    chunks = _chunk_text_by_paragraph(article_text, approx_chars_per_chunk=15000)
    
    # Step 1: Summarize each chunk
    chunk_summaries = []
    for i, chunk in enumerate(chunks, 1):
        prompt = f"""Summarize this excerpt from an article into 3-4 concise sentences. Focus on key points only.

Excerpt {i}/{len(chunks)}:
{chunk}

Summary:"""
        summary = _call_gemini(prompt, temperature=0.2, max_tokens=200)
        chunk_summaries.append(summary)
    
    # Step 2: Combine chunk summaries into final summary
    combined = "\n\n".join(chunk_summaries)
    format_instruction = "as bullet points" if bullet_points else "in a concise paragraph"
    
    final_prompt = f"""Combine and refine the following summaries into a single comprehensive summary {format_instruction}.

Title: {title}

Summaries from different parts of the article:
{combined}

Final comprehensive summary:"""
    
    return _call_gemini(final_prompt, temperature=0.2, max_tokens=500)


def summarize_multiple_articles(articles: List[dict], topic: str = "", max_articles: int = 20) -> str:
    """
    Summarize multiple articles about a specific topic.
    SIMPLIFIED VERSION: Combines all article titles/descriptions into one prompt to avoid rate limits.
    
    Args:
        articles: List of article dicts with 'title', 'description', 'article_text', 'url'
        topic: The search topic/query for context
        max_articles: Maximum number of articles to process (default: 20)
    
    Returns:
        Bullet-point summary
    """
    if not articles:
        return "No articles to summarize."
    
    # Limit articles to avoid token limits
    articles_to_process = articles[:min(max_articles, 15)]
    
    # Build a combined text with all article info
    article_texts = []
    for i, article in enumerate(articles_to_process, 1):
        title = article.get('title', 'Untitled')
        description = article.get('description', '')
        text = article.get('article_text', '')
        source = article.get('spider_name', 'Unknown')
        
        # Use title + description + first 500 chars of article text
        content = f"{title}. {description}"
        if text and len(text) > 100:
            content += f" {text[:500]}"
        
        article_texts.append(f"{i}. [{source}] {content}")
    
    combined_text = "\n\n".join(article_texts)
    
    topic_context = f" about '{topic}'" if topic else ""
    
    # Single API call to summarize everything
    prompt = f"""You are analyzing {len(articles_to_process)} financial news articles{topic_context}.

Articles:
{combined_text}

Create a comprehensive bullet-point summary (5-8 points) that:
• Identifies the main themes and key facts across all articles
• Highlights important trends, figures, or events
• Notes any different perspectives
• Provides actionable insights if applicable

Format as clear, concise bullet points:"""
    
    try:
        summary = _call_gemini(prompt, temperature=0.3, max_tokens=600)
        return summary
    except Exception as e:
        error_msg = str(e)
        return f"❌ Could not generate summary: {error_msg}\n\n**Article Titles:**\n" + "\n".join([f"• {a.get('title', 'Untitled')}" for a in articles_to_process])


def is_configured() -> bool:
    """Check if Gemini API is properly configured."""
    return GEMINI_API_KEY is not None and len(GEMINI_API_KEY) > 0
