import asyncio
import hashlib
import os
import time

from datetime import datetime
from goose3 import Goose

from async_requests import extract_html
from parsers import find_rss_path, parse_rss_html
from sources import sources
from sqlalchemy import create_engine, text
from utils import invert_protocol, queries_in_text, remove_protocol, is_valid_url, remove_html_tags

engine = create_engine(
    f"postgresql+psycopg2://{os.environ['DB_USER']}:{os.environ['DB_PASSWORD']}"
    f"@{os.environ['DB_HOST']}:{os.environ['DB_PORT']}/{os.environ['DB_NAME']}"
)

g = Goose()


if __name__ == "__main__":
    loop = asyncio.get_event_loop()

    rows = engine.execute('SELECT id, text AS name FROM posts_query')
    queries = [dict(row) for row in rows]

    rows = engine.execute(
        """
        SELECT posts_rssfeed.id AS rss_id, posts_rssfeed.rss_feed, posts_rssfeed.updated_at, 
        posts_source.id AS source_id
        FROM posts_rssfeed 
        JOIN posts_source ON posts_rssfeed.source_id = posts_source.id 
        WHERE posts_source.allow_parsing OR posts_source.allow_parsing IS NULL
        """
    )
    rss_paths = []
    for row in rows:
        converted_row = dict(row)
        rss_paths.append({
            'url': converted_row['rss_feed'],
            'pass_values': {
                'updated_at': converted_row['updated_at'],
                'rss_id': converted_row['rss_id'],
                'source_id': converted_row['source_id']
            }
        })

    rss_parsers = []

    rss_responses = loop.run_until_complete(extract_html(rss_paths))
    for response in rss_responses:
        if response['status'] != 200:
            print(response['message'])
            print("Somethin is wrong", response['url'])
            continue

        parser = parse_rss_html(response['html'], response['url'])
        if parser['status'] == 'success':
            rss_parsers.append({
                **parser['parser'], 
                **response['headers'], 
                'updated_at': response['updated_at'],
                'rss_id': response['rss_id'],
                'source_id': response['source_id']
            })
            print(f'Successfully parsed rss path `{response["url"]}`.')
        else:
            print(parser['message'], response['url'])

    for parser in rss_parsers:
        etag = parser.get('ETag', '')
        last_modified = parser.get('Last-Modified', '')
        updated = parser['feed'].get('updated', '')

        if etag or last_modified or updated:
            updated_at = etag + last_modified + updated
            hashed_updated_at = hashlib.md5(updated_at.encode())
            updated_at = hashed_updated_at.hexdigest()
        else:
            updated_at = None

        if parser['updated_at'] == updated_at:
            print("Nothing has changed, skipping...")
            continue

        engine.execute(text(
            """
            UPDATE posts_rssfeed SET updated_at=:updated_at WHERE id=:rss_id
            """
        ), {'updated_at': updated_at, 'rss_id': parser['rss_id']})

        internal_entries = []
        raw_external_entries = []
        for entry in parser['entries']:
            if entry.get('link'):
                link = entry['link']
            else:
                if entry.get('id') and is_valid_url(entry['id']):
                    link = entry['id']
                else:
                    print("Unfortunately this news doesn\'t have a link.")
                    print(entry)
                    continue

            content = None
            if entry.get('summary'):
                text_to_search_in = entry['summary']
            elif entry.get('content') and entry['content'][0]['value']:
                content = remove_html_tags(entry['content'][0]['value'])
                text_to_search_in = content
            else:
                text_to_search_in = ''

            # existing_query = queries_in_text(f'{entry["title"]} {text_to_search_in}', queries)
            # if not existing_query:
            #     continue

            existing_query = queries[0]

            main_data = {
                'link': link,
                'published_at': entry.get('published') or entry.get('updated'),
                'query_id': existing_query['id'],
                'source_id': parser['source_id']
            }

            if content:
                internal_entries.append({
                    'title': entry['title'],
                    'description': content,
                    **main_data
                })
            else:
                raw_external_entries.append({
                    'url': link,
                    'pass_values': main_data
                })

        external_entries = []
        inserted_sources = []

        external_entries_responses = loop.run_until_complete(extract_html(raw_external_entries))
        for response in external_entries_responses:
            if response['status'] != 200:
                print(f'[GooseError] {response["message"]} | URL: {response["url"]}')
                continue

            article = g.extract(raw_html=response['html'])
            external_entries.append({
                'query_id': response['query_id'],
                'link': response['link'],
                'published_at': response['published_at'],
                'description': article.cleaned_text,
                'title': article.title,
                'source_id': response['source_id']
            })

        articles = []
        processed_entries = [*internal_entries, *external_entries]
        for entry in processed_entries:
            # if (
            #     entry['source_url'] not in existing_sources and \
            #     entry['source_url'] not in inserted_sources
            # ):
            #     raw_source_id = engine.execute(text(
            #         """
            #         INSERT INTO posts_source(title, url, allow_parsing, is_spam)
            #         VALUES(:title, :url, :allow_parsing, :is_spam) 
            #         ON CONFLICT (url) DO NOTHING
            #         RETURNING id
            #         """
            #     ), {
            #         'title': remove_protocol(response['source_url']),
            #         'url': response['source_url'],
            #         'allow_parsing': True,
            #         'is_spam': False
            #     })
            #     source_id = [dict(row) for row in raw_source_id][0]['id']

            #     inserted_sources.append(response['source_url'])
            # else:
            #     raw_source_id = engine.execute(text(
            #         """
            #         SELECT id FROM posts_source WHERE url=:url
            #         """
            #     ), {'url': response['source_url']})

            #     source_id = [dict(row) for row in raw_source_id][0]['id']

            articles.append({
                'created': datetime.now(),
                'data_source': 'general_source',
                'query_id': entry['query_id'],
                'source_id': entry['source_id'],
                'url': entry['link'],
                'posted': entry['published_at'],
                'text': entry['description'],
                'title': entry['title'],
                'language': 'en',
                'sentiment': False,
                'editor_choice': False
            })

        if articles:
            engine.execute(text(
                """
                INSERT INTO posts_post(created, data_source, query_id, source_id, 
                url, posted, text, title, language, sentiment, editor_choice) 
                VALUES(:created, :data_source, :query_id, :source_id, :url, :posted, 
                :text, :title, :language, :sentiment, :editor_choice) 
                ON CONFLICT (url) DO 
                UPDATE SET query_id=:query_id, url=:url, text=:text, title=:title
                """
            ), articles)
            
        # engine.execute(text('UPDATE sources SET updated_at=:updated_at WHERE rss_path=:rss_path'), {'updated_at': updated_at, 'rss_path': parser['rss_path']})