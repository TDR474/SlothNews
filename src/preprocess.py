from datetime import datetime, timedelta
from google.cloud import bigquery
import requests
from bs4 import BeautifulSoup
from langdetect import detect
import sys


def get_gdelt_event_id(url):
    """Gets the GLOBALEVENTID for the given URL if it exists in GDELT."""
    client = bigquery.Client()

    query = f"""
    SELECT DISTINCT GLOBALEVENTID, SQLDATE
    FROM `gdelt-bq.gdeltv2.events`
    WHERE SOURCEURL = '{url}'
    LIMIT 10
    """

    job = client.query(query)
    results = list(job)
    if not results:
        return None, None
    event_ids = [row['GLOBALEVENTID'] for row in results]
    article_date = results[0]['SQLDATE']
    return event_ids, article_date


def get_event_articles(url, window=7):
    """Gets articles in the same event as the URL within the specified
    time window."""
    client = bigquery.Client()

    event_ids, article_date = get_gdelt_event_id(url)
    if not event_ids:
        return None

    date = datetime.strptime(str(article_date), '%Y%m%d')
    start_date = date - timedelta(days=window)
    end_date = min(date + timedelta(days=window), datetime.now())

    ids_string = ", ".join(map(str, event_ids))
    # TODO: ensure the CAST on date works as expected
    article_query = f"""
    SELECT
        GLOBALEVENTID,
        SOURCEURL,
        SQLDATE,
        Actor1Name,
        Actor1CountryCode,
        Actor2Name,
        Actor2CountryCode,
        ActionGeo_Type,
        ActionGeo_FullName,
        ActionGeo_CountryCode,
        CAST(AvgTone AS FLOAT64) AS AvgTone,
        CAST(GoldsteinScale AS FLOAT64) AS GoldsteinScale,
        NumArticles
    FROM `gdelt-bq.gdeltv2.events`
    WHERE GLOBALEVENTID IN ({ids_string}) AND SOURCEURL IS NOT NULL
    AND CAST(SQLDATE AS STRING) BETWEEN '{start_date.strftime('%Y%m%d')}' AND '{end_date.strftime('%Y%m%d')}'
    """

    article_job = client.query(article_query)
    results = [dict(row) for row in article_job]

    for result in results:
        result['ArticleContent'] = None
        result['ArticleLanguage'] = None

    return results


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


def preprocess_articles(url, window=7, limit=50, max_len=10000):
    """Get preprocessed articles for the given url.
    window=7 is the time window (radius).
    limit=50 is the limit for number of articles.
    max_len=10000 is the limit for length of each article before truncating."""
    articles = get_event_articles(url, window)

    if not articles:
        return []

    scrape_detect_lang(articles, limit, max_len)

    return [article for article in articles if article['ArticleContent']]

