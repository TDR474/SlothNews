from datetime import datetime, timedelta
import pandas as pd
import gdelt
import requests
from bs4 import BeautifulSoup
from langdetect import detect
import sys


def get_event_articles(url, article_date, window=7):
    """Gets related articles based on events' IDs.
    Input article_date in format 'YYYY-MM-DD'."""
    gd = gdelt.gdelt(version=2)

    article_date = datetime.strptime(article_date, '%Y-%m-%d')
    start_date = article_date - timedelta(days=window)
    end_date = min(article_date + timedelta(days=window), datetime.now())

    all_mentions = gd.Search([start_date.strftime('%Y %b %d'), end_date.strftime('%Y %b %d')],
                         table='mentions',
                         output='pd',
                         coverage=False)

    input_mentions = all_mentions[all_mentions['MentionIdentifier'] == url]

    if input_mentions.empty:
        return None

    event_ids = input_mentions['GLOBALEVENTID'].unique()

    related_mentions = all_mentions[all_mentions['GLOBALEVENTID'].isin(event_ids)]

    related_mentions = related_mentions.drop_duplicates(subset='MentionIdentifier')

    results_list = related_mentions.to_dict('records')

    for result in results_list:
        result['ArticleContent'] = None
        result['ArticleLanguage'] = None
        result['SOURCEURL'] = result.pop('MentionIdentifier', None)

    return results_list


# TODO: move limit to get_event_articles
# TODO: utilize max_len within scraping if possible
def scrape_detect_lang(articles, limit=50, max_len=10000):
    """Scrapes the articles' content and determines language."""
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                      'AppleWebKit/537.36 (KHTML, like Gecko) '
                      'Chrome/91.0.4472.124 Safari/537.36'}
    for i, article in enumerate(articles):
        if i >= limit:
            break
        try:
            response = requests.get(article['SOURCEURL'], headers=headers,
                                    timeout=10)
            response.raise_for_status()

            soup = BeautifulSoup(response.content, 'html.parser')
            paragraphs = soup.find_all('p')
            content = " ".join([p.get_text() for p in paragraphs])

            if len(content) > max_len:
                content = content[:max_len] + '...'

            article['ArticleContent'] = content

            if content:
                article['ArticleLanguage'] = detect(content)

        except Exception as e:
            print(f"Error scraping {article['SOURCEURL']}: {str(e)}",
                  file=sys.stderr)


def preprocess_articles(url, article_date, window=7, limit=50, max_len=10000):
    """Get preprocessed articles for the given url.
    window=7 is the time window (radius).
    limit=50 is the limit for number of articles.
    max_len=10000 is the limit for length of each article before truncating."""
    articles = get_event_articles(url, article_date, window)

    if not articles:
        return []

    scrape_detect_lang(articles, limit, max_len)

    return list({article['ArticleContent']: article for article in articles if
                 article['ArticleContent']}.values())
