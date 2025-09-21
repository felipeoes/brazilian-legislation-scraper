import base64
import re
import urllib.parse
from concurrent.futures import ThreadPoolExecutor, as_completed
from io import BytesIO
from typing import Any, Dict, List

import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from tqdm import tqdm

from src.scraper.base.scraper import BaseScaper
from src.database.saver import FileSaver

load_dotenv()

# obs: LeiComp = Lei Complementar; LeiOrd = Lei Ordinária;
TYPES = [
    "Constituição Estadual",
    "Decreto",
    "Emenda",
    "LeiComp",
    "LeiOrd",
    "Resolucao",
]

# situations will be inferred from the text of the norm
VALID_SITUATIONS = []
INVALID_SITUATIONS = []
# norms with these situations are invalid norms (no longer have legal effect)

# the reason to have invalid situations is in case we need to train a classifier to predict if a norm is valid or something else similar
SITUATIONS = VALID_SITUATIONS + INVALID_SITUATIONS


class RJAlerjScraper(BaseScaper):
    """Webscraper for Alesp (Assembleia Legislativa do Rio de Janeiro) website (https://www.alerj.rj.gov.br/)

    Example search request: http://alerjln1.alerj.rj.gov.br/contlei.nsf/DecretoAnoInt?OpenForm&Start=1&Count=300

    Observation: Only valid norms are published on the Alerj website (the invalid ones are archived and available only on another search engine that is not working currently), so we don't need to check for validity
    """

    def __init__(
        self,
        base_url: str = "http://alerjln1.alerj.rj.gov.br/contlei.nsf",
        **kwargs,
    ):
        super().__init__(base_url, types=TYPES, situations=SITUATIONS, **kwargs)
        self.params = {
            "OpenForm": "",
            "Start": 1,
            "Count": 500,
        }
        self.docs_save_dir = self.docs_save_dir / "RIO_DE_JANEIRO"
        self.fetched_constitution = False
        # Initialize the FileSaver
        self.saver = FileSaver(self.docs_save_dir)

    def _format_search_url(self, norm_type: str) -> str:
        """Format url for search request"""
        return f"{self.base_url}/{norm_type}AnoInt?OpenForm&Start={self.params['Start']}&Count={self.params['Count']}"

    def _get_docs_html_links(
        self, norm_type: str, soup: BeautifulSoup
    ) -> List[Dict[str, Any]]:
        """Get documents html links from soup object."""
        html_links = []
        for tr in soup.find_all("tr", valign="top"):
            tds = tr.find_all("td")
            if len(tds) != 6:
                continue

            link_tag = tds[1].find("a")
            if not link_tag or not link_tag.has_attr("href"):
                continue

            url = urllib.parse.urljoin(self.base_url, link_tag["href"])
            html_links.append(
                {
                    "title": f"{norm_type} {tds[1].text.strip()}",
                    "date": tds[2].text.strip(),
                    "author": tds[3].text.strip(),
                    "summary": tds[4].text.strip(),
                    "html_link": url,
                }
            )
        return html_links

    def _html_to_markdown(self, html_string: str) -> str:
        """Converts an HTML string to Markdown."""
        # Add <html><body> tags to the html string to avoid error with markdown conversion
        full_html = f"<html><body>{html_string}</body></html>"
        buffer = BytesIO(full_html.encode())
        return self._get_markdown(stream=buffer).strip()

    def _get_doc_data(self, doc_info: dict) -> dict:
        """Get document data from given html link"""
        doc_html_link = doc_info["html_link"]
        soup = self._get_soup(doc_html_link)

        body = soup.body
        if not body:
            return None

        # Decompose all content after the main section
        section2_tag = body.find("a", attrs={"name": "_Section2"})
        if section2_tag:
            for tag in section2_tag.find_all_next():
                tag.decompose()
            section2_tag.decompose()

        # Clean up the HTML
        for img_tag in body.find_all("img"):
            img_tag.decompose()
        for a_tag in body.find_all("a"):
            a_tag.unwrap()

        # Determine situation
        situation = (
            "Revogada"
            if soup.find("font", text=re.compile(r"\s*\[ Revogado \]\s*"))
            else "Sem revogação expressa"
        )

        html_string = body.prettify().replace("\n", "")
        text_markdown = self._html_to_markdown(html_string)

        return {
            **doc_info,
            "situation": situation,
            "html_string": html_string,
            "text_markdown": text_markdown,
            "document_url": doc_html_link.strip().replace("?OpenDocument", ""),
        }

    def _build_constitution_section_url(self, data_role: str) -> str:
        """Builds the URL for a section of the constitution."""
        base_url = "http://www3.alerj.rj.gov.br/lotus_notes/default.asp"
        encoded_path = base64.b64encode(data_role.encode("utf-8")).decode("ascii")
        query_params = {"id": 73, "url": encoded_path}
        return f"{base_url}?{urllib.parse.urlencode(query_params)}"

    def _clean_constitution_section_soup(self, soup: BeautifulSoup):
        """Cleans the BeautifulSoup object of a constitution section."""
        for div_to_remove in soup.find_all("div", class_="alert alert-warning"):
            div_to_remove.decompose()
        for div_to_remove in soup.find_all("div", id="barraBotoes"):
            div_to_remove.decompose()
        for tag_to_remove in soup.find_all(
            text=re.compile(r"Texto do Título|Texto do Capítulo|Texto da Seção")
        ):
            parent = tag_to_remove.parent
            if parent and not parent.decomposed:
                parent.decompose()
        for img_to_remove in soup.find_all("img"):
            img_to_remove.decompose()

    def scrape_constitution(self):
        """Scrape constitution data"""
        constitution_url = "http://www3.alerj.rj.gov.br/lotus_notes/default.asp?id=73&url=L2NvbnN0ZXN0Lm5zZi9JbmRpY2VJbnQ/T3BlbkZvcm0mU3RhcnQ9MSZDb3VudD0zMDA="
        soup = self._get_soup(constitution_url)

        a_links = [
            a
            for a in soup.find_all("a", attrs={"data-role": True})
            if "Indice" not in a["data-role"]
            and "OpenNavigator" not in a["data-role"]
            and "EMENDAS CONSTITUCIONAIS" not in a.text.strip().upper()
        ]

        html_parts = []
        for a_link in tqdm(a_links, desc="RJ - ALERJ | Constitution"):
            section_url = self._build_constitution_section_url(a_link["data-role"])
            section_soup = self._get_soup(section_url)
            self._clean_constitution_section_soup(section_soup)

            content_div = section_soup.find("div", id="divConteudo")
            if content_div:
                html_parts.append(content_div.prettify().replace("\n", ""))

        html_string = "<hr/>".join(html_parts)
        text_markdown = self._html_to_markdown(html_string)

        queue_item = {
            "year": 1989,
            "type": "Constituição Estadual",
            "title": "Constituição Estadual do Rio de Janeiro",
            "date": "05/10/1989",
            "author": "",
            "summary": "",
            "html_link": constitution_url,
            "html_string": f"<html><body>{html_string}</body></html>",
            "text_markdown": text_markdown,
            "situation": "Sem revogação expressa",
            "document_url": constitution_url,
        }

        self.queue.put(queue_item)
        self.results.append(queue_item)
        self.count += 1
        self.fetched_constitution = True

    def _scrape_year(self, year: str) -> List[Dict]:
        """Scrape data from given year"""
        all_results = []
        
        for norm_type in tqdm(
            self.types, desc=f"RJ - ALERJ | {year} | Types", total=len(self.types)
        ):
            if norm_type == "Constituição Estadual":
                if not self.fetched_constitution:
                    self.scrape_constitution()
                continue

            url = self._format_search_url(norm_type)
            soup = self._get_soup(url)

            if soup.find("tr", valign="top") is None:
                continue

            img_item = soup.find("img", alt=f"Show details for {year}")
            if not img_item:
                continue

            year_item = img_item.find_parent("a")
            if not year_item or not year_item.has_attr("href"):
                continue

            year_url = urllib.parse.urljoin(url, year_item["href"])
            soup = self._get_soup(year_url)

            documents_html_links = self._get_docs_html_links(norm_type, soup)

            scraped_docs = []
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                future_to_doc = {
                    executor.submit(self._get_doc_data, doc): doc
                    for doc in documents_html_links
                }
                for future in tqdm(
                    as_completed(future_to_doc),
                    desc=f"RJ - ALERJ | {year} | {norm_type}",
                    total=len(documents_html_links),
                    leave=False,
                ):
                    result = future.result()
                    if result:
                        queue_item = {"year": year, "type": norm_type, **result}
                        scraped_docs.append(queue_item)

            all_results.extend(scraped_docs)
            self.results.extend(scraped_docs)
            self.count += len(scraped_docs)

            if self.verbose:
                print(
                    f"Scraped {len(scraped_docs)} {norm_type} documents in {year}"
                )
        
        return all_results
