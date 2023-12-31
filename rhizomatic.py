#!/usr/bin/env python3
# RHIZOMATIC - a tool to enumerate editors of Wikipedia pages of interest to identify related pages that have been edit-bombed
# primary output format inteded to be ingested into data crunching helper apps
# initial wikipedia target: https://en.wikipedia.org/wiki/A_Troublesome_Inheritance
# TARGET OUTPUT FORMAT: USERNAME, PAGE, TIME
__version__ = '20231108'

from argparse import ArgumentParser
from requests import get, head
from requests.exceptions import RequestException
from dateutil import parser
import sys
from lxml import html
import re
import logging
import csv

# Configure the logger
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
)

# Create a logger object for this script
logger = logging.getLogger('rhizomatic')


def bombout(msg):
    logger.error(msg)
    exit(1)

def out(msg=''):
    print(msg)

def get_page_history(wiki_url, offset:str='', limit:int=500):
    # Use re.search to find the last section of the url
    pattern = r'.*/([^/]+)+'
    page_match = re.search(pattern, wiki_url)
    if page_match:
        title = page_match.group(1)
    else:
        logger.error("No title found")
        raise ValueError("No title found")
      
    history_url = f'https://en.wikipedia.org/w/index.php?title={title}&action=history&offset={offset}&limit={limit}'
    all_entries = []
   
    
    try:
        # if there are {limit} number of page entries, then there are entries remaining
        # keep fethcing until we've gotten all the entries
        while True:
            logger.info(f"fetching history for page {title}, {limit} entries starting at {offset}")
            logger.info(f"Sending request to {history_url}")
            response = get(history_url)
            document = html.document_fromstring(response.content)

            editors = document.xpath('//ul[contains(@class,"mw-contributions-list")]//bdi/text()')
            edit_times = document.xpath('//ul[@class="mw-contributions-list"]//a[contains(@class,"mw-changeslist-date")]/text()')
            page_entries = [(e,t,title) for e,t in zip(editors, edit_times)]
            all_entries = all_entries + page_entries
            if len(page_entries) < limit:
                break
            else:
                last_page_date = parser.parse(edit_times[-1])
                offset = int(last_page_date.strftime('%Y%m%d%H%M%S'))
                history_url = f'https://en.wikipedia.org/w/index.php?title={title}&action=history&offset={offset}&limit={limit}'
    except RequestException as e:
        logger.error(e.strerror)
        logger.error('error fetching page, restart at last offset')
    finally:
        return all_entries


def get_user_edit_history(username, offset:str='', limit:int=500):
    base_url = 'https://en.wikipedia.org/w/index.php'
    contributions_url = 'Special:Contributions'
    wiki_url = f'{base_url}?title={contributions_url}&target={username}&limit={limit}&offset={offset}'
    title_from_url_pattern = r'.*/([^/]+)+'
    
    all_entries = []
    try:
        # if there are {limit} number of page entries, then there are entries remaining
        # keep fethcing until we've gotten all the entries
        while True:
            logger.info(f"fetching history for {username}, {limit} entries starting at {offset}")
            logger.info(f"Sending request to {wiki_url}")
            response = get(wiki_url)        
            response.raise_for_status()
            document = html.document_fromstring(response.content)
            edit_times = document.xpath('//a[contains(@class,"mw-changeslist-date")]/text()')
            edit_page_titles = [re.search(title_from_url_pattern, url).group(1)
                              for url in 
                              document.xpath('//a[contains(@class,"mw-contributions-title")]/@href')
                              ]
            # extract the edit entries as a list of tuples and add them to bulk list
            page_entries = [(username, time, title) 
                            for time,title in 
                            zip(edit_times, edit_page_titles)
                            ]
            all_entries = all_entries + page_entries
            if len(page_entries) < limit:
                break
            else:
                last_page_date = parser.parse(edit_times[-1])
                offset = int(last_page_date.strftime('%Y%m%d%H%M%S'))
                wiki_url = f'{base_url}?title={contributions_url}&target={username}&limit={limit}&offset={offset}'
            
    except RequestException as e:
        logger.error('error fetching page, restart at last offset')
        logger.error(e)
    finally:
        return all_entries


def write_tuples_to_csv(editing_history, output_file):
    # Define the CSV headers
    headers = ["editor", "timestamp", "page"]
    with open(output_file, mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(headers)
        writer.writerows(editing_history)


def main():
    parser = ArgumentParser()
    parser.add_argument('-u', '--user', help='user to query')
    parser.add_argument('-p', '--page', help='full url of page to enumerate the editors from')
    parser.add_argument('-o', '--output', help='output file (default: <url|user>.csv)')
 
    args = parser.parse_args()
    
    if args.output:
        output_filename = args.output

    if not (args.page or args.user):
        print('Must specify either an Editor or Page\n')
        parser.print_help()
        sys.exit(1)
    elif args.user and args.page:
        print('Must specify a user OR a Page, not both!')
        parser.print_help()
        sys.exit(1)
    elif args.page:
        if not args.output:
            try:
                title = re.search(r'.*/([^/]+)+', args.page).group(1)
                output_filename = f'wiki_page_{title}_history.csv'
            except:
                logger.error("Could not extract title from url")
                sys.exit(1)
        edit_history_tuples = get_page_history(args.page)
    elif args.user:
        if not args.output:
            output_filename = f'wiki_page_{args.user}_history.csv'
        edit_history_tuples = get_user_edit_history(args.user)
    else:
        bombout('How did you even get here?')
    
    logger.info(f'fetched {len(edit_history_tuples)} entries')
    logger.info(f'writing to {output_filename}')
    write_tuples_to_csv(edit_history_tuples, output_filename)
    

if __name__ == "__main__":
    main()
