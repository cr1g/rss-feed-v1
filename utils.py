import inflect
import re

engine = inflect.engine()


def invert_protocol(url):
    if 'https' in url:
        url.replace('https', 'http')
    else:
        url.replace('http', 'https')

    return url


def is_valid_url(url):
    validation = re.compile(
        # http:// or https://
        r'^(?:http|ftp)s?://'
        #domain...
        r'(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+(?:[A-Z]{2,6}\.?|[A-Z0-9-]{2,}\.?)|'
        #localhost...
        r'localhost|'
        # ...or ip
        r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})'
        # optional port
        r'(?::\d+)?'
        r'(?:/?|[/?]\S+)$'
        , re.IGNORECASE
    )

    if validation.match(url):
        return True
    
    return False


def queries_in_text(text, queries):
    for query in queries:
        splitted_query = query['name'].split(' ')
        existing = []
        for q in splitted_query:
            qp = engine.plural_noun(q).lower()
            if q.lower() in text.lower() or qp in text.lower():
                existing.append(q)

        if existing == splitted_query:
            return query
    
    return None


def remove_protocol(url):
    return re.sub('^(https|http)://', '', url)


def remove_html_tags(text):
    return re.sub(r'(<.*?>)', '', text, flags=re.DOTALL)