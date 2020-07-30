import aiohttp
import asyncio


async def fetch(session, url, pass_values):
    try:
        async with session.get(url) as response:
            html = await response.content.read()
            headers = response.headers
            status = 200
    except aiohttp.ClientConnectorError:
        status = 404
        message = f'Couldn\'t connect to `{url}`.'
    except (aiohttp.ServerTimeoutError, asyncio.exceptions.TimeoutError):
        status = 408
        message = f'`{url}` takes too long to respond.'
    except aiohttp.TooManyRedirects:
        status = ''
        message = f'`{url}`, too many redirects.'
    except aiohttp.ClientPayloadError:
        status = 400
        message = f'Can not decode content-encoding: gzip for `{url}` .'
    except aiohttp.ServerDisconnectedError:
        print(url)
        quit()

    if status == 200:
        return {
            'url': url,
            'status': status,
            'headers': headers,
            'html': html,
            **pass_values
        }

    return {
        'url': url,
        'status': status,
        'message': message
    }


async def extract_html(urls):
    async with aiohttp.ClientSession(
        read_timeout=30,
        conn_timeout=30,
        headers={
            'Accept-Encoding': 'gzip',
            'Keep-Alive': 'connection',
            'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.87 Safari/537.36'
        }
    ) as session:

        tasks = []
        for url in urls:
            tasks.append(fetch(session, url['url'], url['pass_values']))
        result = await asyncio.gather(*tasks)
    
        return result