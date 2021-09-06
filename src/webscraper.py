import settings
import logging
import aiohttp
import asyncio
# import reprlib
from operator import itemgetter
from bs4 import BeautifulSoup
import re

# Logging
logger = logging.getLogger('client.webscraper')


def chunks(l, n):
    """Yield successive n-sized chunks from l."""
    for i in range(0, len(l), n):
        yield l[i:i + n]


def parseSprinterData(data):
    # Parse JSON list of data in pages
    data_paged = list(chunks(data, 2))

    return data_paged


async def isValidIsin(isin, allow_redirects=False):
    # Checks if a sprinter is valid
    # Checks wether it's an actual ISIN. Starts with NL OR NLING and has 7 numbers
    try:
        # https://regex101.com/r/U88u9y/1
        isin = re.search(r"(?i)((nl)[0-9, A-Z]{10})", isin).group(0)
    except AttributeError:
        return False

    url = 'https://www.ingsprinters.nl/zoeken?q=' + isin

    async with aiohttp.ClientSession() as session:
        async with session.get(url, allow_redirects=allow_redirects) as response:
            status = response.status

    if status == 302:  # Redirect url found
        return True

    return False



async def fetchURL(session, url, requested_format, allow_redirects=False):
    # Fetch URL asynchronous
    async with session.get(url, allow_redirects=allow_redirects) as response:
        if response.status == 200 or response.status == 302:
            if requested_format == "json":
                return await response.json()
            return await response.text()
        return None


async def getSprinterDataHTML(isin_list):
    tasks = []
    results = []
    results_unavailable = []

    # Asynchronically get HTML pages
    async with aiohttp.ClientSession() as session:
        for value in isin_list:
            url = 'https://www.ingsprinters.nl/zoeken?q=' + value
            tasks.append(fetchURL(session, url, "html", allow_redirects=True))
        htmls = await asyncio.gather(*tasks)

    # Asynchronically scrape data
    async def iterations(index, value):
        temp_unavailable = {}
        soup = BeautifulSoup(value, 'lxml')
        try:
            name = []
            for span_tag in soup.find_all('span', itemprop='name'):
                name.append(span_tag.text.strip())
            sprinter_name = name[-1]

            if "Beëindigd" in name:
                temp_unavailable["Isin"] = isin_list[index]
                temp_unavailable["Ended"] = 1
                results_unavailable.append(temp_unavailable)
                return
        except (IndexError, TypeError, KeyError, AttributeError) as e:
            temp_unavailable["Isin"] = isin_list[index]
            temp_unavailable["Ended"] = 1
            results_unavailable.append(temp_unavailable)
            return

        # Scraping data Not in the correct format yet
        chart = []
        for h3_tag in soup.find_all('h3', class_='meta__heading no-margin'):
            chart.append(h3_tag.text.strip())

        data = []
        for span_tag in soup.find_all('span', class_=re.compile('^meta__value meta__value--l*')):
            data.append(span_tag.text.strip())

        # https://regex101.com/r/DygTpD/1
        sprinter_type = soup.find('h1', class_="text-body").text.strip()
        sprinter_type = re.search(r"(\S+)\s\S*[0-9]", sprinter_type).group(1)

        temp_dict = {}
        temp_dict["Title"] = sprinter_name
        temp_dict["Isin"] = isin_list[index]
        temp_dict["Type"] = sprinter_type
        temp_dict["Ended"] = 0

        for x in range(len(chart)):
            if x == 2:
                temp_dict[chart[x]] = data[x].replace(" %", "")
            if x == 5:
                # Either create a string as '50,12 -0,4'
                temp_dict[chart[x].replace("*", "")] = [data[x], data[6]]
                # Or create two entries in dict (better)
                temp_dict[chart[x].replace("*", "") + "_1"] = data[x]
                temp_dict[chart[x].replace("*", "") + "_2"] = data[6]
            else:
                temp_dict[chart[x]] = data[x]

        results.append(temp_dict)
        return

    coros = [iterations(index, value) for index, value in enumerate(htmls)]
    await asyncio.gather(*coros)

    return results, results_unavailable
