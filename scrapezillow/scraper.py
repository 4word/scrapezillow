import re
try:
    from httplib import OK
    from urlparse import urljoin, unquote
except ImportError:  # Python3
    from http.client import OK
    from urllib.parse import urljoin, unquote

from bs4 import BeautifulSoup, Comment
import requests
from logging import getLogger
from decimal import Decimal
try:  # Try to import a smarter datetime
    from django.utils import timezone as datetime
except ImportError:
    from datetime import datetime
from datetime import timedelta

from scrapezillow import constants

logger = getLogger(__name__)


class NullResultException(Exception):
    pass


def _check_for_null_result(result):
    if not result:
        raise NullResultException(
            "We were unable to parse crucial facts for this home. Perhaps this is "
            "not a valid listing or the html changed and we are unable to use the "
            "scraper. If the latter, file a bug at https://github.com/hahnicity/scrapezillow/issues"
        )


def _get_sale_info(soup):
    sale_info = {"price": None, "status": None, "zestimate": None, "rent_zestimate": None}
    value_wrapper = soup.find("div", id=constants.HOME_VALUE)
    summary_rows = value_wrapper.find_all(class_=re.compile("home-summary-row"))
    for row in summary_rows:
        pricing_re = "(Foreclosure Estimate|Below Zestimate|Rent Zestimate|Zestimate|Sold on|Sold|Price cut)(?:\xae)?:?[\n ]+-?\$?([\d,\/\w]+)"
        pricing = re.findall(pricing_re, row.text)
        status = re.findall("(For Sale|Auction|Make Me Move|For Rent|Pre-Foreclosure|Off Market)", row.text)
        if pricing:
            property_ = pricing[0][0].strip().replace(" ", "_").lower()
            sale_info[str(property_)] = pricing[0][1]
        elif status:
            sale_info["status"] = status[0]
        elif re.search("\$?[\d,]+", row.text):
            sale_info["price"] = re.findall(r"\$?([\d,]+)", row.text)[0]
    zestimates_values = soup.findAll("div",{"class":"zest-value"})
    zestimates_titles = soup.findAll("div",{"class":"zest-title"})
    for i,zest in enumerate(zestimates_titles): # loop through titles to make sure we have the right div for RENT zestimate
        value = zest.findAll(text=True)
        if 'rent' in str(value):
            sale_info["rent_zestimate"] = re.findall(r"\$?([\d,]+)", zestimates_values[i](text=True)[0])[0]
    return sale_info


def _get_property_summary(soup):
    def parse_property(regex, property_):
        try:
            results[property_] = re.findall(regex, prop_summary)[0]
        except IndexError:
            results[property_] = None

    prop_summary = soup.find("div", class_=constants.PROP_SUMMARY_CLASS)
    _check_for_null_result(prop_summary)
    prop_summary = prop_summary.text
    results = {}
    parse_property(r"([\d\.]+) beds?", "bedrooms")
    parse_property(r"([\d\.]+) baths?", "bathrooms")
    parse_property(r"([\d,\.]+) sqft", "sqft")
    parse_property(r"((?:[A-Z]\w+ ?){1,}), [A-Z]{2}", "city")
    parse_property(r"(?:[A-Z]\w+ ?){1,}, ([A-Z]{2})", "state")
    parse_property(r"[A-Z]{2} (\d{5}-?(?:\d{4})?)", "zipcode")
    return results


def _get_description(soup):
    description = soup.find("div", class_=constants.DESCRIPTION)
    _check_for_null_result(description)
    return description.text


def _get_photos(soup):
    images = soup.select("ol.photos img")
    if not images:
        return []

    photos = [i.get("href", i.get("src", None)) for i in images]
    return filter(None, photos)


def _get_fact_list(soup):
    groups = soup.find_all("ul", constants.FACT_GROUPING)
    facts = []
    for group in groups:
        facts.extend(group.find_all('li'))
    return facts


def _parse_facts(facts):
    parsed_facts = {}
    for fact in facts:
        if fact.text in constants.HOME_TYPES:
            parsed_facts["home_type"] = fact.text
        elif "Built in" in fact.text:
            parsed_facts["year"] = re.findall(r"Built in (\d+)", fact.text)[0]
        elif "days on Zillow" in fact.text:
            parsed_facts["days_on_zillow"] = re.findall(r"(\d+) days", fact.text)[0]
        elif len(fact.text.split(":")) == 1:
            if not "extras" in parsed_facts:
                parsed_facts["extras"] = []
            parsed_facts["extras"].append(fact.text)
        elif "Posted" in fact.text:
            string = re.sub("( #|# )", "", fact.text)
            split = string.split(":")
            days = split[1].strip().split(" ")[0]
            posted_date = datetime.now() - timedelta(days=int(days))
            parsed_facts['posted'] = posted_date
        else:
            string = re.sub("( #|# )", "", fact.text)
            split = string.split(":")
            # Translate facts types to vars_with_underscores and convert unicode to string
            parsed_facts[str(split[0].strip().replace(" ", "_").lower())] = split[1].strip()
    return parsed_facts


def get_raw_html(url, timeout, request_class):
    response = request_class.get(url, timeout=timeout)
    if response.status_code != OK:
        raise Exception("You received a {} error. Your content {}".format(
            response.status_code, response.content
        ))
    elif response.url == constants.ZILLOW_HOMES_URL:
        raise Exception(
            "You were redirected to {} perhaps this is because your original url {} was "
            "unable to be found".format(constants.ZILLOW_HOMES_URL, url)
        )
    else:
        return response.content


def validate_scraper_input(url, zpid):
    if url and zpid:
        raise ValueError("You cannot specify both a url and a zpid. Choode one or the other")
    elif not url and not zpid:
        raise ValueError("You must specify either a zpid or a url of the home to scrape")
    if url and "homes" not in url:
        raise ValueError(
            "This program only supports gathering data for homes. Please Specify your url as "
            "http://zillow.com/homes/<zpid>_zpid/(index)/"
        )
    return url or urljoin(constants.ZILLOW_URL, "homes/{}_zpid/(index)/".format(zpid))


def _get_location_data(soup):
    """
    Gets coordinates from hidden comment (zillow doesnt like showing coords, probably to prevent scrapers)

    :param soup: BS4 object with zillow listing data
    :return: tuple with (latitude, longitude)
    """
    location_data_div = soup.find('div', {'class': 'homeMarkerData'})
    if location_data_div:
        comments = location_data_div.find(text=lambda text: isinstance(text, Comment))
        parsed_comments = comments.string.strip('[').strip(']').split(',')
        try:
            lat = Decimal(Decimal(parsed_comments[-2]) / Decimal(1e6))
            lon_str = ''.join([a for a in parsed_comments[-1] if '\\' not in a])
            lon = Decimal(Decimal(lon_str) / Decimal(1e6))
        except Exception:
            lat = None
            lon = None
    else:
        lat = None
        lon = None

    location_data_elem = soup.find(id="hdp-map-coordinates")
    if not lat:
        lat = location_data_elem.attrs['data-latitude']
    if not lon:
        lon = location_data_elem.attrs['data-longitude']
    try:
        addr_url = location_data_elem.attrs['data-direction']
        addr_url = unquote(addr_url)
        addr_substr = addr_url[addr_url.index('addr=')+5:]
        addr_substr = addr_substr[:addr_substr.index('&')]
        addr_substr = addr_substr.replace('+', ' ').replace(' ,', ', ')
        if addr_substr[-5] == '-':
            addr_substr = addr_substr[:-5]
    except Exception:
        addr_substr = None

    return {'latitude': lat, 'longitude': lon, 'address': addr_substr}


def _get_ajax_url(soup, label):
    pattern = r"(\/AjaxRender.htm\?encparams=[\w\-_~=]+&rwebid=\d+&rhost=\d)\",customEvent:\"CollapsibleModule:expandSection\",jsModule:\"{}".format(label)
    url = re.search(pattern, soup.text)
    _check_for_null_result(url)
    ajax_url = "http://www.zillow.com" + url.group(1)
    return ajax_url


def _get_table_body(ajax_url, request_timeout, request_class=requests):
    html = get_raw_html(ajax_url, request_timeout, request_class)
    pattern = r' { "html": "(.*)" }'
    html = re.search(pattern, str(html)).group(1)
    html = re.sub(r'\\"', r'"', html)  # Correct escaped quotes
    html = re.sub(r'\\/', r'/', html)  # Correct escaped forward slashes
    soup = BeautifulSoup(html, 'html.parser')
    table = soup.find('table')
    if not table:  # It doesn't have a price/tax history
        raise ValueError("There is no table history for url {}".format(ajax_url))
    table_body = table.find('tbody')
    return table_body


def _get_price_history(ajax_url, request_timeout, request_class=requests):
    table_body = _get_table_body(ajax_url, request_timeout, request_class)
    data = []

    rows = table_body.find_all('tr')
    for row in rows:
        cols = row.find_all('td')
        cols = [ele for ele in cols]
        date = cols[0].get_text()
        event = cols[1].get_text()
        try:
            price_span = cols[2].find('span')
        except IndexError:
            price_span = None
        if not price_span:
            price = None
        else:
            price = price_span.get_text()

        data.append([date, event, price])
    return data


def _get_tax_history(ajax_url, request_timeout, request_class=requests):
    data = []
    try:
        table_body = _get_table_body(ajax_url, request_timeout, request_class)
    except ValueError:
        return data

    rows = table_body.find_all('tr')
    for row in rows:
        cols = row.find_all('td')
        cols = [ele for ele in cols]
        date = cols[0].get_text()
        tax = cols[1].contents[0]
        assessment = cols[3].get_text()

        data.append([date, tax, assessment])
    return data


def populate_price_and_tax_histories(soup, results, request_timeout, request_class=requests):
    try:
        history_url = _get_ajax_url(soup, "z-hdp-price-history")
        results["price_history"] = _get_price_history(history_url, request_timeout, request_class)
    except NullResultException as e:
        print('Unable to get price history.  Perhaps this is not a valid listing or the html changed and we are unable '
              'to use the scraper. If the latter, file a bug at https://github.com/hahnicity/scrapezillow/issues')
        logger.exception(e)
    try:
        tax_url = _get_ajax_url(soup, "z-expando-table")
        results["tax_history"] = _get_tax_history(tax_url, request_timeout, request_class)
    except NullResultException as e:
        print('Unable to get tax history.  Perhaps this is not a valid listing or the html changed and we are unable '
              'to use the scraper. If the latter, file a bug at https://github.com/hahnicity/scrapezillow/issues')
        logger.exception(e)


def scrape_url(url=None, zpid=None, request_timeout=10, request_class=requests, get_price_and_tax_info=True):
    """
    Scrape a specific zillow home. Takes either a url or a zpid. If both/neither are
    specified this function will throw an error.

    :param url: Optional string URL to crawl    (XOR with zpid)
    :param zpid: Optional zillow ID to use      (XOR with url)
    :param request_timeout: Optional the timeout param to pass to the request class
    :param request_class: Optional the request class to use. Defaults to requests. Class must have get method.

    :return results: Dict object with information about zillow listing.
    """
    url = validate_scraper_input(url, zpid)
    soup = BeautifulSoup(get_raw_html(url, request_timeout, request_class), 'html.parser')
    results = _get_property_summary(soup)
    facts = _parse_facts(_get_fact_list(soup))
    results.update(**facts)
    results.update(**_get_sale_info(soup))
    results["description"] = _get_description(soup)
    results["photos"] = _get_photos(soup)
    results["location_data"] = _get_location_data(soup)
    if get_price_and_tax_info:
        populate_price_and_tax_histories(soup, results, request_timeout, request_class)
    return results
