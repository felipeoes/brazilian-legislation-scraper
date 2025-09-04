from typing import Optional
from urllib.parse import urljoin, urlencode

import re
from datetime import datetime
from bs4 import BeautifulSoup
from io import BytesIO
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
from src.scraper.base.scraper import BaseScaper, YEAR_START

TYPES = {
    #"Ato Deliberativo": "ato-deliberativo",
    #"Ato Normativo": "ato-normativo",
    #"Emenda Constitucional": "legislacao5/const_e/ement.htm",  # lei complementar, lei ordinaria and emenda constitucional share the same scraping logic
    "Lei Complementar": "ementario/lc.htm",
    "Lei Ordinária": "lei_ordinaria.htm",
    "Resolução": "resolucao",  # ato normativo, ato deliberativo and resolução share the same scraping logic
}


VALID_SITUATIONS = [
    "Não consta"
]  # Alece does not have a situation field, invalid norms will have an indication in the document text

INVALID_SITUATIONS = []  # norms with these situations are invalid norms (no lon

# the reason to have invalid situations is in case we need to train a classifier to predict if a norm is valid or something else similar
SITUATIONS = VALID_SITUATIONS + INVALID_SITUATIONS


class CearaAleceScraper(BaseScaper):
    """Webscraper for Ceara state legislation website (https://www.al.ce.gov.br/)

    Example search request: https://www.al.ce.gov.br/legislativo/leis-e-normativos-internos?categoria=ato-normativo&page=1
    """

    def __init__(
        self,
        base_url: str = "https://www.al.ce.gov.br/legislativo",
        **kwargs,
    ):
        super().__init__(base_url, types=TYPES, situations=SITUATIONS, **kwargs)
        self.docs_save_dir = self.docs_save_dir / "CEARA"
        self.params = {
            "categoria": "",
            "page": 1,
        }
        self._initialize_saver()

    def _format_search_url(self, norm_type_id: str, page: int) -> str:
        """Format url for search request"""
        self.params["categoria"] = norm_type_id
        self.params["page"] = page
        return f"{self.base_url}/leis-e-normativos-internos?{urlencode(self.params)}"

    def _get_docs_links(self, norm_type: str, url: str) -> list:
        """Get documents html links from given page.
        Returns a list of dicts with keys 'title', 'year', 'norm_number', 'summary', 'document_url'
        """

        soup = self._get_soup(url)
        docs = []

        # check if the page is empty
        empty_tag = soup.find("p", class_="mt-5")
        if empty_tag and "nenhum dado localizado" in empty_tag.text.lower():
            return []

        # there may be 2 tables in the page, we want the second one
        tables = soup.find_all("table")
        if len(tables) < 2:
            table = tables[0]
        else:
            table = tables[1]
        items = table.find_all("tr")
        for item in items:
            tds = item.find_all("td")
            if len(tds) != 5:
                continue

            norm_number = tds[0].text.strip()
            year = norm_number.split("/")[1]
            title = norm_number
            summary = tds[2].text.strip()
            document_url = tds[4].find("a")["href"]
            docs.append(
                {
                    "title": f"{norm_type} {title}",
                    "year": year,
                    "norm_number": norm_number,
                    "summary": summary,
                    "document_url": document_url,
                }
            )

        return docs

    def _get_doc_data(self, doc_info: dict) -> dict:
        """Get document data from given document dict"""
        # document url will be a link to pdf document
        text_markdown = self._get_markdown(doc_info["document_url"])
        doc_info["text_markdown"] = text_markdown

        return doc_info

    def _scrape_norms(self, situation: str, norm_type: str, norm_type_id: str) -> list:
        """Scrape laws and norms from given situation and norm type"""
        url = self._format_search_url(norm_type_id, 1)
        soup = self._get_soup(url)

        # get total pages
        pagination = soup.find("ul", class_="pagination")
        if pagination:
            pages = pagination.find_all("li")
            last_page = pages[-2].find("a")["href"]
            total_pages = int(last_page.split("page=")[-1])
        else:
            total_pages = 1

        # Get documents html links
        documents = []
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = [
                executor.submit(
                    self._get_docs_links,
                    norm_type,
                    self._format_search_url(norm_type_id, page),
                )
                for page in range(1, total_pages + 1)
            ]

            for future in tqdm(
                as_completed(futures),
                total=total_pages,
                desc="CEARA | Get document link",
                disable=not self.verbose,
            ):
                docs = future.result()
                if docs:
                    documents.extend(docs)

        # Get document data
        results = []
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = [executor.submit(self._get_doc_data, doc) for doc in documents]

            for future in tqdm(
                as_completed(futures),
                total=len(documents),
                desc="CEARA | Get document data",
                disable=not self.verbose,
            ):
                result = future.result()
                if result is None:
                    continue

                # save to one drive
                queue_item = {
                    # hardcode since we only get valid documents in search request
                    "situation": situation,
                    "type": norm_type,
                    **result,
                }

                self.queue.put(queue_item)
                results.append(queue_item)

            self.results.extend(results)
            self.count += len(results)

            if self.verbose:
                print(
                    f"Finished scraping for | Situation: {situation} | Type: {norm_type} | Results: {len(results)} | Total: {self.count}"
                )

    def _get_laws_constitution_amendments_docs_links(
        self, url: str, norm_type: str
    ) -> list:
        """Get documents html links from given page.
        Returns a list of dicts with keys 'title', 'year', 'summary', 'html_link'
        """
        soup = self._get_soup(url)
        docs = []

        table = soup.find_all("table")
        if len(table) > 1:
            table = table[1]
        else:
            table = table[0]
        items = table.find_all("tr")
        for index in range(len(items)):
            # skip first row since it's the header
            if index == 0:
                continue

            item = items[index]

            tds = item.find_all("td")

            # for leis ordinarias, the table has at most 3 columns
            if norm_type == "Lei Ordinária" and len(tds) > 3:
                continue

            title = tds[0].text.strip()

            # don't need for lei ordinaria
            year = None
            if norm_type != "Lei Ordinária":
                # regex to get year part directly (FORMATs: "DE 20.10.09",  "DE 20.10.2009", "DE 06/03/25" or "DE 06/03/2009")
                year_text = re.search(
                    r"\s+\d{2}\s*[./]\s*\d{2}\s*[./]\s*(\d{2}|\d{4})\b", title
                ).group(1)

                if len(year_text) == 2:
                    year_now = datetime.now().year
                    year_text = f"20{year_text}"
                    if int(year_text) > year_now:
                        year_text = f"19{year_text[2:]}"

                year = int(year_text)

            try:  # some documents are not available, so we skip them
                summary = tds[1].text.strip()
                html_link = tds[0].find("a")["href"]
                # remove "../" from html_link
                html_link = html_link.replace("../", "")
                docs.append(
                    {
                        "title": title,
                        "year": year,
                        "summary": summary,
                        "html_link": html_link,
                    }
                )

            except Exception as e:
                print(f"Error getting html link: {e}")
                continue

        return docs

    def construct_url(self, norm_type: str, html_link: str, year: Optional[int]) -> str:
        """Construct the full url for the document page"""
        if "https://" in html_link:
            # don't need to do anything, it's already a full url
            return html_link

        # file:///\\10.85.100.8\10.85.100.8\legislativo\legislacao5\leis2014\15517.htm.
        if "file://" in html_link:
            # get content after \legislacao5\
            html_link = html_link.split("legislacao5")[-1].replace("\\", "/")
            if html_link.startswith("/"):
                html_link = html_link[1:]

            base_url = "https://www2.al.ce.gov.br/legislativo/legislacao5/"
            return urljoin(base_url, html_link)

        if norm_type == "Lei Complementar":
            base_url = "https://www2.al.ce.gov.br/legislativo/"
            if not "ementario" in html_link:
                html_link = f"ementario/{html_link}"
        else:
            base_url = "https://www2.al.ce.gov.br/legislativo/legislacao5/"
            if norm_type == "Lei Ordinária":
                if not "leis" in html_link:
                    year = year if year >= 2000 else str(year)[2:]

                    html_link = f"leis{year}/{html_link}"

                elif (
                    "legislacao5" in html_link
                    or "legislativo/legislacao5/" in html_link
                    or "/legislativo/legislacao5/" in html_link
                ):
                    html_link = (
                        html_link.replace("/legislativo/legislacao5/", "")
                        .replace("legislativo/legislacao5/", "")
                        .replace("legislacao5/", "")
                    )

        return urljoin(base_url, html_link)

    def _get_laws_constitution_amendments_doc_data(
        self, doc_info: dict, norm_type: str, year: Optional[int] = None
    ) -> Optional[dict]:
        """Get document data from given document dict"""
        # html_link will be a link to the document page

        html_link = doc_info.pop("html_link")
        url = self.construct_url(norm_type, html_link, year)

        response = self._make_request(url)

        if not response:
            print(
                f"Error fetching document page: {url}. Year: {year}. HTML link: {html_link}"
            )
            return None

        soup = BeautifulSoup(response.content, "html.parser")

        # remove <a href="javascript:%20history.back();"><b><span style="color:blue">VOLTAR</span></b></a>
        voltar_link = soup.find("a", text=re.compile(r"^VOLTAR$", re.IGNORECASE))
        if voltar_link:
            voltar_link.decompose()

        html_string = soup.prettify().strip()

        # check if invalid document
        if "NÄO EXISTE LEI COM ESTE NÚMERO".lower() in html_string.lower():
            return None

        buffer = BytesIO()
        buffer.write(soup.html.encode())
        buffer.seek(0)

        text_markdown = self._get_markdown(stream=buffer)
        # remove header
        text_markdown = text_markdown.replace("**VOLTAR**", "").strip()

        if not text_markdown:
            print(f"Error converting document to markdown: {url}. Year: {year}")
            return None

        doc_info["html_string"] = html_string
        doc_info["text_markdown"] = text_markdown
        doc_info["document_url"] = url

        return doc_info

    def _scrape_laws_constitution_amendments(
        self, situation: str, norm_type: str, norm_type_id: str, year: int = None
    ):
        """Scrape constitution amendments"""
        # for laws and constitution amendments we need to scrape a different page

        if year is not None:  # norm_type is ordinary laws
            # if year >= 2021 link will be in the format: legislacao5/leis{year}/LEIS{year}.htm
            if year >= 2021:
                url = f"https://www2.al.ce.gov.br/legislativo/legislacao5/leis{year}/LEIS{year}.htm"

            # if year < 2000, url will be like leis91/e91.htm
            elif year >= 2000:
                url = f"https://www2.al.ce.gov.br/legislativo/legislacao5/leis{year}/e{year}.htm"
            else:
                year2_digits = str(year)[2:]
                url = f"https://www2.al.ce.gov.br/legislativo/legislacao5/leis{year2_digits}/e{year2_digits}.htm"

        else:
            url = f"https://www2.al.ce.gov.br/legislativo/{norm_type_id}"

        docs = self._get_laws_constitution_amendments_docs_links(url, norm_type)

        results = []
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = [
                executor.submit(
                    self._get_laws_constitution_amendments_doc_data,
                    doc,
                    norm_type,
                    year,
                )
                for doc in docs
            ]

            for future in tqdm(
                as_completed(futures),
                total=len(docs),
                desc="CEARA | Get norms data",
                disable=not self.verbose,
            ):
                result = future.result()
                if result is None:
                    continue

                # save to one drive
                queue_item = {
                    # hardcode since we only get valid documents in search request
                    "situation": situation,
                    "type": norm_type,
                    **result,
                }

                if queue_item["year"] is None:
                    queue_item["year"] = year

                self.queue.put(queue_item)
                results.append(queue_item)

            self.results.extend(results)
            self.count += len(results)

            if self.verbose:
                print(
                    f"Finished scraping for Year: {year} | Situation: {situation} | Type: {norm_type} | Results: {len(results)} | Total: {self.count}"
                )

    def scrape(self) -> list:
        """Scrape data from all years"""
        # start saver thread
        self.saver.start()

        # check if can resume from last scrapped year
        resume_from = self.year_start  # 1808
        forced_resume = self.year_start != YEAR_START
        if self.saver.last_year is not None and not forced_resume:
            print(f"Resuming from {self.saver.last_year}")
            resume_from = int(self.saver.last_year)
        else:
            print(f"Starting from {resume_from}")

        # scrape data
        for situation in tqdm(
            self.situations,
            desc="CEARA | Situations",
            total=len(self.situations),
            disable=not self.verbose,
        ):
            for norm_type, norm_type_id in tqdm(
                self.types.items(),
                desc="CEARA | Types",
                total=len(self.types),
                disable=not self.verbose,
            ):

                if norm_type in ["Ato Deliberativo", "Ato Normativo", "Resolução"]:
                    self._scrape_norms(situation, norm_type, norm_type_id)
                elif norm_type in [
                    "Emenda Constitucional",
                    "Lei Complementar",
                ]:
                    self._scrape_laws_constitution_amendments(
                        situation, norm_type, norm_type_id
                    )
                else:
                    # get available years
                    url = f"https://www2.al.ce.gov.br/legislativo/{norm_type_id}"

                    soup = self._get_soup(url)

                    table = soup.find("table", {"class": "MsoNormalTable"})
                    rows = table.find_all("tr")

                    available_years = []
                    for index in range(
                        1, len(rows)
                    ):  # skip the first  row, which is the header
                        item = rows[index]
                        tds = item.find_all("td")
                        for td in tds:
                            a = td.find("a")
                            if a:
                                available_years.append(int(a.text))

                    available_years.sort()

                    for year in tqdm(
                        available_years,
                        desc="CEARA | Years",
                        total=len(self.years),
                        disable=not self.verbose,
                    ):
                        # skip years before resume_from
                        if year < resume_from:
                            continue

                        self._scrape_laws_constitution_amendments(
                            situation, norm_type, norm_type_id, year
                        )

        return self.results
