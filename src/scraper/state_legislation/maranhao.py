from urllib.parse import urljoin

import asyncio
import re
from bs4 import BeautifulSoup
from loguru import logger
from src.scraper.base.scraper import StateScraper

TYPES = {
    "Constituição Estadual": "constituicao-estadual/detalhe.html?dswid=-4293",
    "Lei": {
        "id": 1,
        "subtypes": {
            "Lei Ordinária": 2,
            "Lei Complementar": 3,
        },
    },
    "Emenda Constitucional": 5,
    "Decreto Legislativo": 6,
    "Resolução Legislativa": 7,
    "Resolução Administrativa": 8,
}

VALID_SITUATIONS = [
    "Não consta"
]  # Alema does not have a situation field, invalid norms will have an indication in the document text

INVALID_SITUATIONS = []  # norms with these situations are invalid norms (no longer have legal effect)

# the reason to have invalid situations is in case we need to train a classifier to predict if a norm is valid or something else similar
SITUATIONS = VALID_SITUATIONS + INVALID_SITUATIONS


class MaranhaoAlemaScraper(StateScraper):
    """Webscraper for Maranhao state legislation website (https://legislacao.al.ma.leg.br)

    Example search request: https://legislacao.al.ma.leg.br/ged/busca.html?dswid=1381

    payload: {
        javax.faces.partial.ajax: true
        javax.faces.source: table_resultados
        javax.faces.partial.execute: table_resultados
        javax.faces.partial.render: table_resultados
        javax.faces.behavior.event: page
        javax.faces.partial.event: page
        table_resultados_pagination: true
        table_resultados_first: 0
        table_resultados_rows: 10
        table_resultados_skipChildren: true
        table_resultados_encodeFeature: true
        j_idt44: j_idt44
        in_tipo_doc_focus:
        in_tipo_doc_input: 1
        j_idt53: 2
        in_nro_doc:
        in_ano_doc: 2020
        ementa:
        in_nro_proj_lei:
        in_ano_proj_lei:
        in_ini_public_input:
        in_fim_public_input:
        table_resultados_rowExpansionState:
        javax.faces.ViewState: -1509641436052460021:2441054440402057157
        javax.faces.ClientWindow: 1381
    }

    """

    def __init__(
        self,
        base_url: str = "https://legislacao.al.ma.leg.br",
        **kwargs,
    ):
        super().__init__(
            base_url, name="MARANHAO", types=TYPES, situations=SITUATIONS, **kwargs
        )
        self._rows_per_page = 10
        self.scraped_constitution: bool = False

    def _build_params(
        self, norm_type_id: str, year: int, page: int, subtype_id=""
    ) -> dict:
        """Build a fresh params dict for a specific query (no shared state mutation)."""
        return {
            "javax.faces.partial.ajax": "true",
            "javax.faces.source": "table_resultados",
            "javax.faces.partial.execute": "table_resultados",
            "javax.faces.partial.render": "table_resultados",
            "javax.faces.behavior.event": "page",
            "javax.faces.partial.event": "page",
            "table_resultados_pagination": "true",
            "table_resultados_first": page * self._rows_per_page,
            "table_resultados_rows": self._rows_per_page,
            "table_resultados_skipChildren": "true",
            "table_resultados_encodeFeature": "true",
            "j_idt44": "j_idt44",
            "in_tipo_doc_focus": "",
            "in_tipo_doc_input": norm_type_id,
            "j_idt53": subtype_id if subtype_id else "",
            "in_nro_doc": "",
            "in_ano_doc": year,
            "ementa": "",
            "in_nro_proj_lei": "",
            "in_ano_proj_lei": "",
            "in_ini_public_input": "",
            "in_fim_public_input": "",
            "table_resultados_rowExpansionState": "",
            "javax.faces.ViewState": "-1509641436052460021:2441054440402057157",
            "javax.faces.ClientWindow": 1381,
        }

    def _build_search_url(self) -> str:
        """Build the search URL (no shared state mutation)."""
        return f"{self.base_url}/ged/busca.html?dswid=1381"

    async def _click_page(self, page: int):
        """Click on page number"""

        # check if page number is available to click
        page_elements = await self.page.locator(".ui-paginator-page").all()
        current_visible_pages = [int(await p.text_content()) for p in page_elements]

        # if no pages are visible it may have only one page. Just return and do nothing
        if len(current_visible_pages) == 0:
            return

        # click next page until the desired page is visible
        while page not in current_visible_pages:
            next_page = self.page.locator(".ui-paginator-next")
            await next_page.click()
            page_elements = await self.page.locator(".ui-paginator-page").all()
            current_visible_pages = [int(await p.text_content()) for p in page_elements]

        # click on the desired page
        page_element = self.page.locator(f"xpath=//a[text()='{page}']")
        await page_element.click()

        await asyncio.sleep(3)

    async def _get_docs_links(self, page: int, norm_type: str) -> list:
        """Get documents links from given page.
        Returns a list of dicts with keys 'title', 'publication', 'project', 'summary', 'pdf_link'
        """

        # navigate to the page
        await self._click_page(page)
        page_source = await self.page.content()
        soup = BeautifulSoup(page_source, "html.parser")

        docs = []

        items = soup.find_all("tr", class_="ui-widget-content")
        for item in items:
            title = item.find("label", class_="ui-outputlabel ui-widget").text
            publication = item.find_all("label", class_="ui-outputlabel ui-widget")[
                3
            ].text
            project = item.find_all("label", class_="ui-outputlabel ui-widget")[2].text
            summary = item.find("label", class_="ui-outputlabel ui-widget ementa").text
            pdf_link = item.find("a")["href"]
            docs.append(
                {
                    "title": f"{norm_type} - {title}",
                    "publication": publication,
                    "project": project,
                    "summary": summary,
                    "pdf_link": pdf_link,
                }
            )

        return docs

    async def _get_doc_data(self, doc_info: dict) -> dict:
        """Get document data from given document dict"""
        # remove pdf_link from doc_info
        pdf_link = doc_info.pop("pdf_link")

        if self._is_already_scraped(pdf_link, doc_info.get("title", "")):
            return None

        text_markdown, raw_content, content_ext = await self._download_and_convert(
            pdf_link
        )
        if not text_markdown:
            logger.error(f"Failed to get markdown for {pdf_link}")
            await self._save_doc_error(
                title=doc_info.get("title", ""),
                year=doc_info.get("year", ""),
                html_link=pdf_link,
                error_message="Failed to get markdown from PDF",
            )
            return None

        # check for error with url (The requested URL was not found on this server)
        if "the requested url was not found on this server" in text_markdown.lower():
            logger.warning(f"Invalid document: {pdf_link}")
            await self._save_doc_error(
                title=doc_info.get("title", ""),
                year=doc_info.get("year", ""),
                html_link=pdf_link,
                error_message="URL not found on server",
            )
            return None

        doc_info["text_markdown"] = text_markdown
        doc_info["document_url"] = pdf_link
        doc_info["_raw_content"] = raw_content
        doc_info["_content_extension"] = content_ext
        return doc_info

    async def _search_norms(
        self,
        norm_type: str,
        _norm_type_id: str,
        year: int,
        _page: int,
        subtype: str = None,
        subtype_id: str = None,
    ) -> BeautifulSoup:
        """Use Playwright to search for norms for a specific year and type"""
        url = self._build_search_url()
        await self.page.goto(url, wait_until="domcontentloaded")

        # change option via click
        in_tipo_doc = self.page.locator("#in_tipo_doc")
        await in_tipo_doc.click()

        # go down to the desired option
        for type, _ in self.types.items():
            if type == norm_type:
                break
            await self.page.keyboard.press("ArrowDown")
        await self.page.keyboard.press("Enter")

        await asyncio.sleep(3)

        if subtype_id:
            # let only the subtype checkbox checked
            checkbox_trs = await self.page.locator("#j_idt53").locator("tr").all()
            for checkbox_tr in checkbox_trs:
                checkbox = checkbox_tr.locator("input")
                label = checkbox_tr.locator("label")
                label_text = await label.text_content()
                checked = await checkbox.get_attribute("checked")
                if label_text == subtype and not checked == "true":
                    await label.click()
                elif label_text != subtype and checked == "true":
                    await label.click()

        in_ano_doc = self.page.locator("#in_ano_doc")
        await in_ano_doc.fill(str(year))

        await asyncio.sleep(1)

        # submit form
        submit_button = self.page.locator("#j_idt71")
        await asyncio.sleep(1)
        await submit_button.click()

        await asyncio.sleep(3)

        page_source = await self.page.content()
        return BeautifulSoup(page_source, "html.parser")

    async def _scrape_norms(
        self,
        norm_type: str,
        norm_type_id: str,
        year: int,
        situation: str,
        subtype: str = None,
        subtype_id: str = None,
    ) -> list[dict]:
        """Scrape norms for a specific year, type and situation"""
        # url = self._format_search_url(norm_type_id, year, 0, subtype_id)

        soup = await self._search_norms(
            norm_type, norm_type_id, year, 0, subtype, subtype_id
        )

        # get total pages
        total_docs = soup.find(
            "div", class_="ui-datatable-header ui-widget-header ui-corner-top"
        )
        if not total_docs:  # no documents found for the given year, type and situation
            return []

        total_docs_regex = re.search(
            r"(\d+) registro\(s\) encontrado\(s\)", total_docs.text
        )

        total_docs = int(total_docs_regex.group(1))

        # total_docs = int(total_docs.text.split(" ")[-3])
        total_pages = total_docs // self._rows_per_page
        if total_docs % self._rows_per_page:
            total_pages += 1

        # Get documents html links
        documents = []
        tasks = [
            self._get_docs_links(page, norm_type if not subtype else subtype)
            for page in range(1, total_pages + 1)
        ]
        valid_results = await self._gather_results(
            tasks,
            context={
                "year": year,
                "type": norm_type if not subtype else subtype,
                "situation": situation,
            },
            desc=f"MARANHAO | {norm_type} | get_docs_links",
        )
        for result in valid_results:
            if result:
                documents.extend(result)

        # Get document data
        ctx = {
            "year": year,
            "situation": situation,
            "type": norm_type if not subtype else subtype,
        }
        tasks = [self._with_save(self._get_doc_data(doc), ctx) for doc in documents]
        results = await self._gather_results(
            tasks,
            context=ctx,
            desc=f"MARANHAO | {norm_type}",
        )

        if self.verbose:
            logger.info(
                f"Finished scraping for Year: {year} | Situation: {situation} | Type: {norm_type} | Results: {len(results)}"
            )

        return results

    async def _scrape_constitution(
        self, norm_type: str, norm_type_id: str
    ) -> dict | None:
        """Scrape state constitution"""
        url = urljoin(f"{self.base_url}/ged/", norm_type_id)
        soup = await self.request_service.get_soup(url)

        # get pdf link <object class="view-pdf-constituicao" data="https://arquivos.al.ma.leg.br:8443/ged/codigos_juridicos/CE89_EC101_2025" type="application/pdf"></object>
        pdf_link = soup.find("object", {"class": "view-pdf-constituicao"})["data"]

        if self._is_already_scraped(pdf_link, "Constituição Estadual do Maranhão"):
            return None

        text_markdown, raw_content, content_ext = await self._download_and_convert(
            pdf_link
        )
        if not text_markdown:
            logger.error(f"Failed to get markdown for Constitution | {pdf_link}")
            return None

        queue_item = {
            "year": 1989,
            # hardcode since it seems we only get valid documents in search request
            "situation": "Não consta revogação expressa",
            "type": norm_type,
            "title": "Constituição Estadual do Maranhão",
            "summary": "",
            "text_markdown": text_markdown,
            "document_url": pdf_link,
            "_raw_content": raw_content,
            "_content_extension": content_ext,
        }

        await self._save_doc_result(queue_item)
        self.scraped_constitution = True
        return queue_item

    async def _scrape_situation_type(
        self, situation: str, norm_type: str, norm_type_id, year: int
    ) -> list[dict]:
        """Scrape norms for a specific situation and type combination"""
        results = []

        if norm_type == "Constituição Estadual" and not self.scraped_constitution:
            result = await self._scrape_constitution(norm_type, norm_type_id)
            if result:
                results.append(result)
            return results

        if isinstance(norm_type_id, dict):
            subtypes = norm_type_id["subtypes"]
            norm_type_id = norm_type_id["id"]
            subtype_tasks = [
                self._scrape_norms(
                    norm_type,
                    norm_type_id,
                    year,
                    situation,
                    subtype=subtype,
                    subtype_id=subtype_id,
                )
                for subtype, subtype_id in subtypes.items()
            ]
            subtype_results_list = await asyncio.gather(
                *subtype_tasks, return_exceptions=True
            )
            for r in subtype_results_list:
                if isinstance(r, list):
                    results.extend(r)
                elif isinstance(r, BaseException):
                    logger.error(f"MA | {norm_type} | subtype error: {r}")
        else:
            subtype_results = await self._scrape_norms(
                norm_type, norm_type_id, year, situation
            )
            results.extend(subtype_results)

        return results

    async def _scrape_year(self, year: int) -> list[dict]:
        """Scrape norms for a specific year"""
        tasks = [
            self._scrape_situation_type(sit, nt, ntid, year)
            for sit in self.situations
            for nt, ntid in self.types.items()
        ]
        valid = await self._gather_results(
            tasks,
            context={"year": year, "type": "N/A", "situation": "N/A"},
            desc=f"{self.name} | Year {year}",
        )
        return self._flatten_results(valid)
