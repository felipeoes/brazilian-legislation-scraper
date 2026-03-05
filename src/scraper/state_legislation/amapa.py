from urllib.parse import urljoin

from bs4 import BeautifulSoup
from loguru import logger
from src.scraper.base.scraper import StateScraper


TYPES = {
    "Decreto Legislativo": 14,
    "Lei Complementar": 12,
    "Lei Ordinária": 13,
    "Resolução": 15,
    "Emenda Constitucional": 11,
}

VALID_SITUATIONS = [
    "Não consta"
]  # Alap does not have a situation field, invalid norms will have an indication in the document text

INVALID_SITUATIONS = []  # norms with these situations are invalid norms (no longer have legal effect)

# the reason to have invalid situations is in case we need to train a classifier to predict if a norm is valid or something else similar
SITUATIONS = VALID_SITUATIONS + INVALID_SITUATIONS


class AmapaAlapScraper(StateScraper):
    """Webscraper for Amapa state legislation website (https://al.ap.leg.br)

    Example search request: https://al.ap.leg.br/pagina.php?pg=buscar_legislacao&aba=legislacao&submenu=listar_legislacao&especie_documento=13&ano=2020&pesquisa=&n_doeB=&n_leiB=&data_inicial=&data_final=&orgaoB=&autor=&legislaturaB=&pagina=2
    """

    def __init__(
        self,
        base_url: str = "https://al.ap.leg.br",
        **kwargs,
    ):
        super().__init__(
            base_url, name="AMAPA", types=TYPES, situations=SITUATIONS, **kwargs
        )

    def _build_search_url(self, norm_type_id: str, year: int, page: int) -> str:
        """Build search URL from arguments (no shared state mutation)."""
        params = {
            "pg": "buscar_legislacao",
            "aba": "legislacao",
            "submenu": "listar_legislacao",
            "especie_documento": norm_type_id,
            "ano": year,
            "pesquisa": "",
            "n_doeB": "",
            "n_leiB": "",
            "data_inicial": "",
            "data_final": "",
            "orgaoB": "",
            "autor": "",
            "legislaturaB": "",
            "pagina": page,
        }
        return f"{self.base_url}/pagina.php?{'&'.join([f'{key}={value}' for key, value in params.items()])}"

    async def _get_docs_links(self, url: str) -> tuple[list, bool]:
        """Get documents html links from given page.
        Returns (docs, reached_end) where docs is a list of dicts and
        reached_end indicates there are no more pages.
        """
        soup = await self.request_service.get_soup(url)
        if not soup:
            raise ValueError(f"Failed to get soup for URL: {url}")

        docs = []
        items = soup.find("tbody").find_all("tr")

        # check if the page is empty (tbody is empty)
        if len(items) == 0:
            return [], True

        for item in items:
            tds = item.find_all("td")
            if len(tds) != 6:
                continue

            title = tds[0].text.strip()
            summary = tds[1].text.strip()
            doe_number = tds[2].text.strip()
            date = tds[3].text.strip()
            proposition_number = tds[4].text.strip()

            a_tag = tds[5].find("a")
            if not a_tag:
                logger.warning(f"No link found for document '{title}' — skipping")
                continue
            html_link = a_tag["href"]

            docs.append(
                {
                    "title": title,
                    "summary": summary,
                    "doe_number": doe_number,
                    "date": date,
                    "proposition_number": proposition_number,
                    "html_link": html_link,
                }
            )

        return docs, False

    async def _get_doc_data(self, doc_info: dict) -> dict:
        """Get document data from given document dict"""
        html_link = doc_info.pop("html_link")
        url = urljoin(self.base_url, html_link)

        if self._is_already_scraped(url, doc_info.get("title", "")):
            return None

        response = await self.request_service.make_request(url)
        if not response:
            doc_info["text_markdown"] = None
            doc_info["document_url"] = url
            return doc_info
        soup = BeautifulSoup(await response.read(), "html.parser")

        # The page always has two header tables to remove:
        #   Table 1 — print-version link  (contains <a class="texto_noticia3">)
        #   Table 2 — coat of arms logo + "ESTADO DO AMAPÁ / ASSEMBLEIA LEGISLATIVA"
        #             (identified by the <img src="brasaoamapa.jpg">)
        # Everything else is the norm body.

        # Remove print-link table
        for a in soup.find_all("a", class_="texto_noticia3"):
            tbl = a.find_parent("table")
            if tbl:
                tbl.decompose()

        # Remove coat-of-arms header table
        for img in soup.find_all("img", src=lambda s: s and "brasao" in s.lower()):
            tbl = img.find_parent("table")
            if tbl:
                tbl.decompose()

        # After header removal the remaining table is the content wrapper.
        # Pass only the inner <td> contents (the <p> tags) to markitdown — without
        # the outer <table> wrapper — so paragraph breaks are preserved.
        remaining_table = soup.find("table")
        if remaining_table:
            # The law text is in the innermost <td>: outer table → inner table → td
            inner_table = remaining_table.find("table")
            content_td = (
                inner_table.find("td") if inner_table else remaining_table.find("td")
            )
            container = content_td or remaining_table

            # Unwrap <font> tags — markitdown's HTML parser silently discards all
            # <p> content nested inside <font> wrappers, only retaining heading
            # tags (<h3>/<h1>) which are why only the signature block was returned.
            for font in container.find_all("font"):
                font.unwrap()

            # Strip inline style attributes so markitdown doesn't skip paragraphs.
            for tag in container.find_all(style=True):
                del tag["style"]

            html_string = self._wrap_html(container.decode_contents())
        else:
            html_string = f"<html>{soup.prettify()}</html>"
            container = None

        text_markdown = await self._get_markdown(html_content=html_string)

        # Fallback: markitdown cannot parse <p><span>…</span></p> from old ALAP docs
        # (only heading tags survive). Build plain markdown directly from the
        # BeautifulSoup <p> tags when the result is suspiciously short.
        if container is not None and len((text_markdown or "").strip()) < 100:
            lines = []
            for p in container.find_all("p"):
                text = p.get_text(" ", strip=True)
                if not text:
                    continue
                # Preserve bold for paragraphs that were fully inside <strong>
                if p.find("strong") and len(p.find_all("strong")) == 1:
                    inner = p.find("strong").get_text(" ", strip=True)
                    if inner == text:
                        text = f"**{text}**"
                lines.append(text)
            if lines:
                text_markdown = "\n\n".join(lines)

        doc_info["text_markdown"] = text_markdown
        doc_info["document_url"] = url
        doc_info["_raw_content"] = html_string.encode("utf-8")
        doc_info["_content_extension"] = ".html"

        return doc_info

    async def _scrape_situation_type(
        self, situation: str, norm_type: str, norm_type_id: int, year: int
    ) -> list:
        """Scrape norms for a specific situation and type"""
        total_pages = (
            1  # just to start and avoid making a lot of requests for empty pages
        )
        reached_end_page = False

        # Get documents html links
        documents = []

        current_page = 1
        while not reached_end_page:
            page_docs = []

            tasks = [
                self._get_docs_links(
                    self._build_search_url(norm_type_id, year, page),
                )
                for page in range(current_page, current_page + total_pages)
            ]
            valid_results = await self._gather_results(
                tasks,
                context={
                    "year": year,
                    "type": norm_type,
                    "situation": situation,
                },
                desc=f"AMAPA | {norm_type} | get_docs_links",
            )
            for result in valid_results:
                docs, ended = result
                if ended:
                    reached_end_page = True
                if docs:
                    page_docs.extend(docs)

            # Add the documents from this batch to our total documents list
            documents.extend(page_docs)

            # If we didn't get any docs or reached the end page, break the loop
            if not page_docs or reached_end_page:
                break

            # Move to the next batch of pages
            current_page += total_pages
            total_pages = min(
                total_pages + 2, self.max_workers
            )  # Gradually increase pages but don't exceed max_workers

        # Only process documents if we found any
        if documents:
            # Get document data
            ctx = {"year": year, "situation": situation, "type": norm_type}
            tasks = [
                self._with_save(self._get_doc_data(doc_info), ctx)
                for doc_info in documents
            ]
            results = await self._gather_results(
                tasks,
                context=ctx,
                desc=f"AMAPA | {norm_type}",
                min_length=100,
            )

            if self.verbose:
                logger.info(
                    f"Finished scraping for Year: {year} | Situation: {situation} | Type: {norm_type} | Results: {len(results)}"
                )

            return results

        return []

    async def _scrape_year(self, year: int) -> list:
        """Scrape norms for a specific year"""
        tasks = [
            self._scrape_situation_type(sit, nt, ntid, year)
            for sit in self.situations
            for nt, ntid in self.types.items()
        ]
        valid = await self._gather_results(
            tasks,
            context={"year": year, "type": "NA", "situation": "NA"},
            desc=f"{self.name} | Year {year}",
        )
        return self._flatten_results(valid)
