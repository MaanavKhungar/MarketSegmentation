import gzip
import json
import os
import sys
import requests
import warcio
from warcio.archiveiterator import ArchiveIterator
from collections import defaultdict
from bs4 import BeautifulSoup


from tqdm import tqdm


# Our process at the moment is (example forbes.com) :
# We go to index.commoncrawl.org and enter the url we want to search for -> this returns a json file with the scrapped data and the location of the files which include data about the website for example forbes
# Then we filter for the filenames and download and extract them -> WARC files

# TODO What is missing
# Look at what we need from the WARC files -> and save the text/title for sentiment analysis
# At the moment it is streamlined for one website (forbes) -> we need to change it so it can be used for multiple websites and automate it so we dont need to manually enter the websites


# Things to look into
# https://github.com/commoncrawl/cc-index-server
# https://github.com/webrecorder/pywb/wiki/CDX-Server-API#api-reference

# Good to know
# cc-index.paths.gz -> basically a list of all the files that are available
# cdx-00000.gz -> the index file for the first file in cc-index.paths.gz

########################################################################################################################
# Load the JSON file containing the links
def get_links_from_json(json_file_path):
    try:
        with open(json_file_path, 'r') as json_file:
            data = json.load(json_file)
            if 'links' in data:
                websites = [link['url'] for link in data['links']]
                for website in websites:
                    print(website)  # TODO: change depending on the filter requirements for commoncrawl.org data
            else:
                print("No 'links' key found in the JSON data.")
    except FileNotFoundError:
        print(f"The JSON file '{json_file_path}' was not found.")
    except json.JSONDecodeError as e:
        print(f"Error decoding JSON: {e}")


# This is used to show the progress of the download
def bar_custom(current, total, width=80):
    progress_message = "Downloading: %d%% [%d / %d]" % (current / total * 100, current, total)
    sys.stdout.write("\r" + progress_message)
    sys.stdout.flush()


# we can use this method to get the info on where the files are located that we need to download
def download_json_from_index_website():

    output_path = 'data/json_data/CC-MAIN-2023-23-index_forbes.json'
    if not os.path.exists(output_path):
        # wget.download(
        #     "https://index.commoncrawl.org/CC-MAIN-2023-23-index?url=https%3A%2F%2Fwww.forbes.com%2F*&output=json",
        #     out=output_path, bar=bar_custom)
        req_down(
            "https://index.commoncrawl.org/CC-MAIN-2023-23-index?url=https%3A%2F%2Fwww.forbes.com%2Fsites%2Fa*&output=json",
            out=output_path)
    else:
        print(f"{output_path} already exists -> skipping download")


# takes the downloaded json file from download_json_from_index_websites of a website and filters out the needed filenames
# data looks like this: 0,146,201,84)/en/projects/aleksandr-solzhenitsyn-conference 20230610172419 {"url": "https://84.201.146.0/en/projects/aleksandr-solzhenitsyn-conference", "mime": "text/html", "mime-detected": "text/html", "status": "200", "digest": "2TV6YNTSXUMKAV55PIQVFDFWCEBTJCZ4", "length": "11146", "offset": "99489863", "filename": "crawl-data/CC-MAIN-2023-23/segments/1685224657735.85/warc/CC-MAIN-20230610164417-20230610194417-00125.warc.gz", "charset": "UTF-8", "languages": "eng"}
def filter_through_json_for_filenames() -> [str]:
    output_list = []
    for line in open('data/json_data/CC-MAIN-2023-23-index_forbes.json', 'r'):
        json_line = json.loads(line)
        if json_line['filename'] not in output_list:
            output_list.append(json_line['filename'])
    return output_list


# gets a list of filenames as input and downloads & extracts the files
def download_and_extract_files(ls_file_paths: [str]) -> None:
    limit = 0
    # ls_file_paths.reverse()
    for file_path in ls_file_paths:
        file_name = file_path[-51:]
        # if file_name != "CC-MAIN-20230609164851-20230609194851-00737.warc.gz":
        #     continue
        # path = wget.download(
        #     url="https://data.commoncrawl.org/" + file_path,
        #     out=f'data/zipped_data/{file_name}', bar=bar_custom)
        path = req_down(
            url="https://data.commoncrawl.org/" + file_path,
            out=f'data/zipped_data/{file_name}')
        print(path)

        with gzip.open(path, 'rb') as f:
            with open(f'data/unzipped_data/{file_name[:48]}', 'wb') as fi:
                for line in tqdm(f, desc="Extracting file"):
                    fi.write(line)
        
        limit = limit + 1
        if limit > 15:
            print("done!")
            return

def req_down(url, out):
    with open(out, 'wb') as f:
        response = requests.get(url)
        f.write(response.content)
        return out


if __name__ == '__main__':
    # list so we dont need to download the json file every time
    example_list = [
        'crawl-data/CC-MAIN-2023-23/segments/1685224652184.68/warc/CC-MAIN-20230605221713-20230606011713-00686.warc.gz',
        'crawl-data/CC-MAIN-2023-23/segments/1685224654031.92/warc/CC-MAIN-20230608003500-20230608033500-00227.warc.gz',
        'crawl-data/CC-MAIN-2023-23/segments/1685224644309.7/warc/CC-MAIN-20230528150639-20230528180639-00284.warc.gz',
        'crawl-data/CC-MAIN-2023-23/segments/1685224652184.68/warc/CC-MAIN-20230605221713-20230606011713-00223.warc.gz',
        'crawl-data/CC-MAIN-2023-23/segments/1685224657720.82/warc/CC-MAIN-20230610131939-20230610161939-00223.warc.gz',
        'crawl-data/CC-MAIN-2023-23/segments/1685224643462.13/warc/CC-MAIN-20230528015553-20230528045553-00569.warc.gz',
        'crawl-data/CC-MAIN-2023-23/segments/1685224648858.14/warc/CC-MAIN-20230602204755-20230602234755-00304.warc.gz',
        'crawl-data/CC-MAIN-2023-23/segments/1685224644915.48/warc/CC-MAIN-20230530000715-20230530030715-00711.warc.gz',
        'crawl-data/CC-MAIN-2023-23/segments/1685224648635.78/warc/CC-MAIN-20230602104352-20230602134352-00284.warc.gz',
        'crawl-data/CC-MAIN-2023-23/segments/1685224649302.35/warc/CC-MAIN-20230603165228-20230603195228-00235.warc.gz',
        'crawl-data/CC-MAIN-2023-23/segments/1685224649293.44/warc/CC-MAIN-20230603133129-20230603163129-00205.warc.gz']

    download_json_from_index_website()
    ls_filenames = filter_through_json_for_filenames()
    download_and_extract_files(ls_filenames)
    # f = warc.open("data/zipped_data/CC-MAIN-20230527223515-20230528013515-00005.warc.gz")
    # for record in f:
    #     print(record.payload.read())