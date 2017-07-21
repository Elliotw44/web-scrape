__author__ = 'eweil'
import requests
import bs4
import csv
from multiprocessing import Pool
from multiprocessing.dummy import Pool as ThreadPool


def IsListingPage(page):
    title_text = page.find_all('title')[0].get_text()
    mls_num_pos = title_text.find("MLS#")
    return mls_num_pos > 0


def GetMLSNumber(listing_page):
    for child in listing_page.find_all('dl'):
        for grand_child in child.children:
            try:
                if grand_child.get_text().find("MLS ID:") != -1:
                    str = grand_child.get_text()
                    num_start = str.find(':') + 1
                    return str[num_start:].strip()
            except AttributeError:
                continue
                
                
def ParseInfoFromListingPage(listing_page, url):
    title_text = listing_page.find_all('title')[0].get_text()
    pipe_one = title_text.find('|')
    address = title_text[0:pipe_one-1]
    mls_num = GetMLSNumber(listing_page)
    return {'address': address, 'mls': mls_num, 'url': url}


def PageURlAddOn(pages_parsed):
    if pages_parsed == 0:
        return ""
    else:
        return str(pages_parsed+1) + "-pg"


def HandleURL(url):
    try:
        child_link = requests.get(url)
    except IOError:
        return
    child_soup = bs4.BeautifulSoup(child_link.content, 'html.parser')
    if not IsListingPage(child_soup):
        return
    else:
        return ParseInfoFromListingPage(child_soup, url)


def main():
    number_of_pages = int(raw_input("please enter the number of pages to parse: "))
    pages_parsed = 0
    base_url = "http://jamesonsothebys.com"
    base_page_url = 'http://jamesonsothebys.com/eng/sales/chicago-il-usa/'
    urls = []
    #Find all the URLs we want to parse
    while pages_parsed < number_of_pages:
        page = requests.get(base_page_url + PageURlAddOn(pages_parsed))
        soup = bs4.BeautifulSoup(page.content, 'html.parser')
        for link in soup.find_all('a'):
            if 'href' in link.attrs and link.attrs['href'].find('/eng/sales/detail/') == 0:
                url = (base_url + (link.attrs['href']))
                if url not in urls:
                    urls.append(base_url + link.attrs['href'])
        pages_parsed += 1
    #Parse all the URLs using ThreadPool
    pool = ThreadPool(13)
    records = pool.map(HandleURL, urls)
    pool.close()
    pool.join()
    #Write parsed information to CSV file
    with open('url_data.csv', 'wb') as csvfile:
        field_names = ["mls", "address", "url"]
        datawriter = csv.DictWriter(csvfile, fieldnames=field_names)
        datawriter.writeheader()
        for record in records:
            datawriter.writerow({"mls": record['mls'], "address": record['address'], "url": record['url']})

if __name__ == '__main__':
    main()
