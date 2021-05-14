from bs4 import BeautifulSoup
import json
import requests
import sys

global_list = []

class DPLA():

    def get_metadata(self,  url):
        res = requests.get(url)
        return res.json(), res.status_code

    def get_original_record(self, record):
        try:
            json.loads(record['originalRecord']['stringValue'])
        except json.decoder.JSONDecodeError as e:
            print(f"No original record found in {record['@id']}")
            return False
        data = json.loads(record['originalRecord']['stringValue'])
        data['dpla_id'] = record['id']

        return data

    def params_to_dict(self, url):
        base_url = url.split("?")[0]
        qstring = url.split("?")[1]

        params = {}

        for p in qstring.split("&"):
            p = p.split("=")
            params[p[0]] = p[1]

        return base_url, params

    def dict_to_url(self, base_url, params):
        qstring = '&'.join([f"{p[0]}={p[1]}" for p in params.items()])
        return f"{base_url}?{qstring}"

    def get_institutions(partner):
        url = 'https://dp.la/search?partner="{}"'.format(partner)
        res = requests.get(url)
        soup = BeautifulSoup(res.content, 'html.parser')
        script = soup.find('script', {'id': '__NEXT_DATA__'})

        data = json.loads(script.getText())

        institutions = data["props"]["pageProps"]["results"]["facets"]["admin.contributingInstitution"]

        return [term["term"] for term in institutions["terms"]]

    def crawl_large_set(self, url, page=1, sort_by="sourceResource.title", sort_order="desc"):
        """
        DPLA's API limits to 100 pages, with a cap per page at 500, so we can only get 50,000 per facet, which means we have to crawl a sorted list both ascending and descending.

        :param url:
        :return:
        """

        base_url, params = self.params_to_dict(url)
        params['page'] = page
        url = self.dict_to_url(base_url, params)
        records = []

        url = url + f"&sort_by={sort_by}&sort_order={sort_order}"
        print(url)
        print(page)

        data, status = self.get_metadata(url)

        # TODO Figure out a better way to handle knowing when to end, given that even in the regular stream, there are duplicates
        total_consecutive = 0
        if status == 200 and data["docs"]:
            for record in data["docs"]:
                orecord = self.get_original_record(record)
                uid = record['id'] + orecord['@id']
                if orecord:
                    if not uid in global_list:
                        records.append(orecord)
                        global_list.append(uid)
                        total_consecutive = 0
                    else:
                        print("Duplicate ID found.")
                        print(uid)
                        total_consecutive += 1
                        print("Total consective: {}".format(total_consecutive))
                        if total_consecutive > 3:
                            print("Found more than 3 consecutive duplicate records. Ending recursion.")
                            return records

            page += 1
            records.extend(self.crawl_large_set(url, page, sort_order=sort_order))

        elif status == 400 and page == 101:
            records.extend(self.crawl_large_set(url, 1, sort_order="asc"))

        return records

    def crawl_metadata(self, institution, api_key, page_size=500, page=1):
        url = f"https://api.dp.la/v2/items?" \
              f"dataProvider=\"{institution}\"&api_key={api_key}" \
              f"&page_size={page_size}&page={page}"

        records = []

        data, status = self.get_metadata(url)
        if status == 200:
            if data["count"] > 50000:
                records = self.crawl_large_set(url)
                return records

            if status == 200 and data["docs"]:
                for record in data["docs"]:
                    if self.get_original_record(record):
                        records.append(self.get_original_record(record))
                print(page)
                page += 1
                records.extend(self.crawl_metadata(institution, api_key, page_size, page))

        return records





