import warcio
from warcio.archiveiterator import ArchiveIterator
from collections import defaultdict
from bs4 import BeautifulSoup

RUN_CT = 0
js_results = defaultdict(set)
# Iterate over each entry in the WARC file
# If the entry is of type "response", then we use the raw_stream which is the HTML source to extract script tags
with open('data/unzipped_data/CC-MAIN-20230609164851-20230609194851-00737.warc', 'rb') as stream:

    for record in ArchiveIterator(stream):
        if record.rec_type == 'response' and RUN_CT <= 1000:
            target_uri = record.rec_headers.get_header('WARC-Target-URI')
            raw_text = record.raw_stream.read()
            soup = BeautifulSoup(raw_text, 'html.parser')
            script_tags = soup.find_all('script')
            js_libs = [k['src'].split("?")[0].split("/")[-1] for k in script_tags if k.has_attr('src')]
            js_results[target_uri] = set(js_libs)
            RUN_CT = RUN_CT+  1

with open(f'data_warc.txt', 'wb') as fi:
    for k,v in js_results.items():
        print(k,"--> ", ",".join(v),"\n")
        # fi.write(k,"--> ", ",".join(v),"\n")

from collections import Counter

js_lib_count = Counter()

for uri, js_libs in js_results.items():
        js_lib_count.update(js_libs)
        
js_lib_count.most_common(10)