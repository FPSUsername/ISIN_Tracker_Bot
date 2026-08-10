import settings
import logging
import aiohttp
import asyncio
# import reprlib
from aiohttp.helpers import URL
from operator import itemgetter
from bs4 import BeautifulSoup
import re

# Logging
logger = logging.getLogger('client.webscraper')


def chunks(l, n):
    """Yield successive n-sized chunks from l."""
    for i in range(0, len(l), n):
        yield l[i:i + n]


def parseProductData(data):
    # Parse JSON list of data in pages
    data_paged = list(chunks(data, 2))

    return data_paged


async def isValidIsin(isin, allow_redirects=False):
    # Checks if a product is valid
    # Checks wether it's an actual ISIN. Starts with DE, NL OR NLING and has 7 numbers
    try:
        # https://regex101.com/r/xxPxLe/1
        isin = re.search(r"(?i)((nl|de)[0-9, A-Z]{10})", isin).group(0)
    except AttributeError:
        return False

    url = 'https://www.ingmarkets.nl/producten/' + isin

    async with aiohttp.ClientSession() as session:
        async with session.get(url, allow_redirects=allow_redirects) as response:
            status = response.status

    if status != 404:  # 200 or 302
        return True

    return False


async def fetchURL(session, url, requested_format, allow_redirects=False):

    # --- ING Markets disclaimer bypass (real fix) ---
    if "ingmarkets.nl/producten" in url:
        await session.post(
            "https://www.ingmarkets.nl/disclaimer",
            data={"redirect": url}
        )

    try:
        async with session.get(url, allow_redirects=True) as response:
            if requested_format in ("text", "html"):
                return await response.text()
            elif requested_format == "json":
                return await response.json()
            elif requested_format == "bytes":
                return await response.read()
            else:
                raise ValueError(f"Unknown requested_format: {requested_format}")

    except Exception as e:
        print(f"Error fetching URL {url}: {e}")
        return None

async def getProductDataHTML(isin_list):
    tasks = []
    results = []
    results_unavailable = []

    # Asynchronically get HTML pages
    async with aiohttp.ClientSession() as session:

        # --- ING Markets disclaimer bypass (correct placement) ---
        session.cookie_jar.update_cookies(
            {"disclaimer": "true"},
            response_url=URL("https://www.ingmarkets.nl/")
        )

        # --- Schedule parallel fetches ---
        for value in isin_list:
            url = f"https://www.ingmarkets.nl/producten/{value}"
            tasks.append(fetchURL(session, url, "html", allow_redirects=True))

        htmls = await asyncio.gather(*tasks)

    # Asynchronically scrape data
    async def iterations(index, value):
        temp_unavailable = {}
        soup = BeautifulSoup(value, 'lxml')
        try:
            name = []
            # Find name
            for h1_tag in soup.find_all('h1'):
                name.append(h1_tag.get_text(strip=True))
            product_name = name[-1]
            # Unknown if this still works
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

        dt = soup.find('dt', string=lambda txt: txt and 'Onderliggende' in txt)
        if dt:
            dd = dt.find_next_sibling('dd')
            product_name = dd.get_text(strip=True) if dd else None

        dt = soup.find('dt', string=lambda txt: txt and 'Onderliggende' in txt)
        if dt:
            dd = dt.find_next_sibling('dd')
            a = dd.find('a') if dd else None
            market_url = a['href'] if a and a.has_attr('href') else None


        dt = soup.find('dt', string=lambda txt: txt and 'Positie' in txt)
        if dt:
            dd = dt.find_next_sibling('dd')
            product_type = dd.get_text(strip=True) if dd else None

        perf_data = extract_performance_block(soup)
        temp_dict = {}
        temp_dict["Title"] = product_name
        temp_dict["Market"] = market_url
        temp_dict["Isin"] = isin_list[index]
        temp_dict["Bid"] = perf_data.get("Bid")
        temp_dict["Ask"] = perf_data.get("Ask")
        temp_dict["Day"] = perf_data.get("Day")
        temp_dict["Lever"] = perf_data.get("Lever")
        temp_dict["Stoploss"] = perf_data.get("Stoploss")
        temp_dict["Stoploss_dist"] = perf_data.get("Stoploss_distance")
        temp_dict["Reference"] = perf_data.get("Reference")
        temp_dict["Type"] = product_type
        temp_dict["Ended"] = 0

        results.append(temp_dict)

    coros = [iterations(index, value) for index, value in enumerate(htmls)]
    await asyncio.gather(*coros)

    return results, results_unavailable

def extract_performance_block(soup):
    perf = soup.find("iwp-product-performance")
    if not perf:
        logger.debug("[extract_performance_block] No <iwp-product-performance> found")
        return {}

    def get_attr(name):
        value = perf.get(name)
        logger.debug(f"[extract_performance_block] {name} = {value}")
        return value

    # Raw values
    raw_day = get_attr("performance")
    raw_dist = get_attr("distancetostoploss")
    raw_bid = get_attr("bid")
    raw_ask = get_attr("ask")
    raw_lever = get_attr("leverage")
    raw_stoploss = get_attr("stoplosslevel")
    raw_reference = get_attr("underlyingbid")

    # Convert fractional → percentage and format with 2 decimals
    def fmt_percent(raw):
        if raw is None:
            return None
        try:
            return f"{float(raw) * 100:.2f}"
        except ValueError:
            return None

    # Format plain numeric values with 2 decimals
    def fmt_number(raw):
        if raw is None:
            return None
        try:
            return f"{float(raw):.2f}"
        except ValueError:
            return None

    return {
        "Bid": fmt_number(raw_bid),
        "Ask": fmt_number(raw_ask),
        "Day": fmt_percent(raw_day),                     # % 1 Dag
        "Lever": fmt_number(raw_lever),                  # Hefboom
        "Stoploss": fmt_number(raw_stoploss),            # Stop-loss niveau
        "Stoploss_distance": fmt_percent(raw_dist),      # Afstand tot stop loss-niveau
        "Reference": fmt_number(raw_reference),          # Referentiekoers*
    }
