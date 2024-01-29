import re
import urllib.parse
from httpx import Client, AsyncClient
from selectolax.parser import HTMLParser
from dataclasses import dataclass
import asyncio
import pandas as pd
import json

@dataclass
class ContactScraper:

    base_url: str = 'https://branchenbuch.meinestadt.de'
    user_agent: str = 'insomnia/2023.5.8'
    # user_agent: str = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36'

    async def fetch_html(self, url, limit):

        headers = {
            'user-agent': self.user_agent
        }

        limit = asyncio.Semaphore(100)
        async with AsyncClient(headers=headers, timeout=15) as aclient:
            async with limit:
                response = await aclient.post(url)
                if limit.locked():
                    await asyncio.sleep(1)
                if response.status_code != 200:
                    response.raise_for_status()

                return response.text


    async def fetch_json(self, url, limit):

        headers = {
            'user-agent': self.user_agent
        }

        async with AsyncClient(headers=headers, timeout=15) as aclient:
            async with limit:
                response = await aclient.post(url)
                if limit.locked():
                    await asyncio.sleep(1)
                if response.status_code == 200:
                    result = response.json()
                elif response.status_code == 404:
                    result = 'none'
                else:
                    response.raise_for_status()

                return result


    def fetch_page(self, url):

        headers = {
            'user-agent': self.user_agent
        }

        with Client(headers=headers, timeout=15) as client:
            response = client.post(url)
            if response.status_code == 200:
                result = response.json()
            elif response.status_code == 404:
                result = 'none'
            else:
                response.raise_for_status()

            return result

    def get_category_links(self, location):

        url = urllib.parse.urljoin(self.base_url,
                                   f'{location}')
        headers = {
            'user-agent': self.user_agent
        }
        with Client() as client:
            response = client.get(url, headers=headers)
            if response.status_code != 200:
                response.raise_for_status()
        html = response.text
        tree = HTMLParser(html)
        cat_elems = tree.css('a[data-component="iconTile"]')
        cat_links = [elem.attributes.get('href') for elem in cat_elems]
        return cat_links

    def get_total_pages(self, cat_links):
        total_pages = []
        for cat_link in cat_links:
            url = urllib.parse.urljoin(cat_link,
                                       f'?service=ajaxPoiCategory&sort=distance&page=1&size=20&radius=30&offset=0&orderBy=distance&userModified=true')
            response = self.fetch_page(url)
            total_page = response['results']['totalPages']
            total_pages.append(total_page)
        result = zip(cat_links, total_pages)

        return tuple(result)


    async def fetch_cat_json(self, cat_links):

        tasks = []
        for cat_link in cat_links:
            for i in range(1, cat_link[1] + 1):
                url = urllib.parse.urljoin(cat_link[0],
                                           f'?service=ajaxPoiCategory&sort=distance&page={i}&size=20&radius=30&offset=0&orderBy=distance&userModified=true')
                limit = asyncio.Semaphore(100)
                task = asyncio.create_task(self.fetch_json(url, limit))
                tasks.append(task)

        cat_json_datas = await asyncio.gather(*tasks)

        return cat_json_datas


    async def fetch_detail_html(self, detail_links):

        tasks = []
        for detail_link in detail_links:
            limit = asyncio.Semaphore(100)
            task = asyncio.create_task(self.fetch_html(detail_link, limit))
            tasks.append(task)

        detail_html = await asyncio.gather(*tasks)

        return detail_html


    def parse_links(self, cat_json_data):

        detail_links = [x['detailLink'] for x in cat_json_data['results']['items']]
        return detail_links


    def get_links(self, cat_jsons):

        results = []
        for cat_json in cat_jsons:
            try:
                links = self.parse_links(cat_json)
            except:
                links = ''
            results.extend(links)

        return results

    def get_data(self, detail_htmls):
        datas = []
        # print(len(detail_htmls))
        for html in detail_htmls:
            content = None
            template = pd.read_csv('Kontakte rev1.csv', delimiter=';')
            for x in template.columns:
                template[x] = ''
            data = template.to_dict(orient='records')
            tree = HTMLParser(html)
            # print(tree.css_first('title').text(strip=True))
            scripts = tree.css('script')
            for script in scripts:
                # print(
                #     '==================================================================================================')
                # print(script.text())
                if '"@type": "LocalBusiness",' in script.text():
                    content = script.text()
                    # print(script.text())
                    break
            json_data = json.loads(content)
            for field in data[0]:
                if field == 'E-Mail':
                    try:
                        data[0][field] = tree.css_first('a#m-textLink24').text(strip=True)
                    except:
                        data[0][field] = ''
                if field == 'Firma':
                    try:
                        data[0][field] = json_data['name']
                    except:
                        data[0][field] = ''
                elif field == 'Straße':
                    try:
                        data[0][field] = json_data['address']['streetAddress'].rsplit(' ', 1)[0]
                    except:
                        data[0][field] = ''
                elif field == 'Hausnummer':
                    try:
                        data[0][field] = json_data['address']['streetAddress'].rsplit(' ', 1)[1]
                    except:
                        data[0][field] = ''
                elif field == 'Telefon':
                    try:
                        data[0][field] = str(json_data['telephone'])
                    except:
                        data[0][field] = ''
                elif field == 'Fax Büro':
                    try:
                        data[0][field] = str(json_data['faxNumber'])
                    except:
                        data[0][field] = ''
                elif field == 'Mobil':
                    try:
                        data[0][field] = str(json_data['mobile'])
                    except:
                        data[0][field] = ''
                elif field == 'Straße Büro':
                    try:
                        data[0][field] = json_data['address']['streetAddress']
                    except:
                        data[0][field] = ''
                elif field == 'PLZ Büro':
                    try:
                        data[0][field] = json_data['address']['postalCode']
                    except:
                        data[0][field] = ''
                elif field == 'PLZ':
                    try:
                        data[0][field] = json_data['address']['postalCode']
                    except:
                        data[0][field] = ''
                elif field == 'Webseite':
                    try:
                        data[0][field] = json_data['url']
                    except:
                        data[0][field] = ''
                elif field == 'Ort':
                    try:
                        data[0][field] = json_data['address']['addressLocality']
                    except:
                        data[0][field] = ''
                elif field == 'Land':
                    try:
                        data[0][field] = 'Deutschland'
                    except:
                        data[0][field] = ''

            datas.extend(data.copy())
        df = pd.DataFrame(datas)
        df.to_csv('Kontakte_rev7.csv', index=False)
        return df


    def main(self):
        cat_links = self.get_category_links('unterschleissheim')
        cat_links = self.get_total_pages(cat_links)
        cat_jsons = asyncio.run(self.fetch_cat_json(cat_links))
        detail_links = self.get_links(cat_jsons)
        # print(detail_links)
        detail_html = asyncio.run(self.fetch_detail_html(detail_links))
        self.get_data(detail_html)

if __name__ == '__main__':
    scraper = ContactScraper()
    scraper.main()