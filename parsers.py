from bs4 import BeautifulSoup
import feedparser
import re
import urllib
import xml

from utils import is_valid_url


def find_rss_path(html, url):
    soup = BeautifulSoup(html, 'html.parser')
    
    raw_rss_paths = soup.find_all('link', {'type': re.compile('rss|atom')})
    if not raw_rss_paths:
        raw_rss_paths = soup.find_all('a', {'href': re.compile('feed|rss')})

    rss_paths = []
    invalid_rss_path = re.compile('(feedback)|((.pdf|.xlsx|.xls|.doc|.docx)$)', re.IGNORECASE)

    for path in raw_rss_paths:
        if invalid_rss_path.search(path['href']) is None:
            rss_paths.append(path['href'])

    if rss_paths:
        href = rss_paths[0].replace(' ',  '%20')

        if is_valid_url(href):
            full_path = href
        else:
            if href[0] != '/':
                full_path = f'{url}/{href}'
            else:
                full_path = url + href

        return full_path

    return None


def parse_rss_html(html, url):
    parser = feedparser.parse(html)
    if parser['bozo'] == 0:
        return {
            'status': 'success',
            'parser': parser
        }
    
    elif type(parser['bozo_exception']) == xml.sax._exceptions.SAXParseException:
        return {
            'status': 'parsing_error',
            'message': '[InvalidRssFormat]',
            'url': url
        }
    
    elif type(parser['bozo_exception']) == urllib.error.URLError:
        return {
            'status': 'not_found',
            'message': 'Found RSS path but it\'s unreachable. Try using the other protocol.',
            'url': url
        }
    
    elif type(parser['bozo_exception']) == feedparser.NonXMLContentType:
        return {
            'status': 'parsing_error',
            'message': '[NonXMLContentType]',
            'url': url
        }

    elif type(parser['bozo_exception']) == feedparser.CharacterEncodingOverride:
        return {
            'status': 'parsing_error',
            'message': '[CharacterEncodingOverride]',
            'url': url
        }

    else:
        print('BAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAD')
        print(parser, url, type(parser['bozo_exception']))
        return {
            'status': 'parsing_error',
            'message': 'Bad Idea'
        }