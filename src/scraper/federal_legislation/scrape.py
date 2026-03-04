import asyncio
from typing import Optional
from urllib.parse import urljoin
from loguru import logger
from tenacity import retry, stop_after_attempt, wait_exponential
from docling.document_converter import DocumentConverter
from docling_core.types.doc.document import ContentLayer, DocItemLabel
from src.scraper.base.scraper import BaseScraper
from src.scraper.base.concurrency import run_in_thread

VALID_SITUATIONS = [
    "Não%20consta%20revogação%20expressa",
    "Não%20Informado",  # since there is no explicit information about it's not valid, we consider it valid
    "Convertida%20em%20Lei",
    "Reeditada",
    "Reeditada%20com%20alteração",
]  # only norms with these situations (are actually valid norms)

INVALID_SITUATIONS = [
    "Arquivada",
    "Rejeitada",
    "Revogada",
    "Sem%20Eficácia",
]  # norms with these situations are invalid norms (no longer have legal effect)

# the reason to have invalid situations is in case we need to train a classifier to predict if a norm is valid or something else similar
SITUATIONS = VALID_SITUATIONS + INVALID_SITUATIONS

# OBS: empty string means all (Toda legislação). OPTIONS: 'Legislação+Interna' 'OR Legislação+Federal'
COVERAGE = [""]

TYPES = [
    "Alvará",
    "Ato",
    "Carta%20Régia",
    "Carta+Imperial",
    "Constitui%C3%A7%C3%A3o",
    "Decisão",
    "Decreto",
    "Decreto+Sem+N%C3%BAmero",
    "Decreto-Lei",
    "Emenda+Constitucional",
    "Instrução",
    "Lei",
    "Manifesto",
    "Mensagem",
    "Pacto",
    "Proclamação",
    "Protocolo",
    "Medida+Provis%C3%B3ria",
    "Ordem+de+Serviço",
    "Portaria",
    "Regulamento",
    "Resolu%C3%A7%C3%A3o+da+Assembl%C3%A9ia+Nacional+Constituinte",
    "Resolu%C3%A7%C3%A3o+da+C%C3%A2mara+dos+Deputados",
    "Resolução+da+Mesa",
    "Resolu%C3%A7%C3%A3o+do+Congresso+Nacional",
    "Resolu%C3%A7%C3%A3o+do+Senado+Federal",
]
ORDERING = "data%3AASC"
YEAR_START = 1808


class CamaraDepScraper(BaseScraper):
    """Webscraper for Camara dos Deputados website (https://www.camara.leg.br/legislacao/)

    Example search request url: https://www.camara.leg.br/legislacao/busca?geral=&ano=&situacao=&abrangencia=&tipo=Decreto%2CDecreto+Legislativo%2CDecreto-Lei%2CEmenda+Constitucional%2CLei+Complementar%2CLei+Ordin%C3%A1ria%2CMedida+Provis%C3%B3ria%2CResolu%C3%A7%C3%A3o+da+C%C3%A2mara+dos+Deputados%2CConstitui%C3%A7%C3%A3o%2CLei%2CLei+Constitucional%2CPortaria%2CRegulamento%2CResolu%C3%A7%C3%A3o+da+Assembl%C3%A9ia+Nacional+Constituinte%2CResolu%C3%A7%C3%A3o+do+Congresso+Nacional%2CResolu%C3%A7%C3%A3o+do+Senado+Federal&origem=&numero=&ordenacao=data%3AASC
    """

    def __init__(
        self,
        base_url: str = "https://www.camara.leg.br/legislacao/",
        **kwargs,
    ):
        super().__init__(
            base_url,
            name="LEGISLACAO_FEDERAL",
            types=TYPES,
            situations=SITUATIONS,
            **kwargs,
        )
        self.base_url = base_url
        self.coverage = kwargs.get("coverage", COVERAGE)
        self.ordering = kwargs.get("ordering", ORDERING)
        self.params = {
            "abrangencia": "",
            "geral": "",
            "ano": "",
            "situacao": "",
            "origem": "",
            "numero": "",
            "ordenacao": "",
        }

    def _format_search_url(self, year: str, situation: str, type: str) -> str:
        """Format search url with given year"""
        self.params["ano"] = year
        self.params["abrangencia"] = self.coverage[0]
        self.params["ordenacao"] = self.ordering
        self.params["situacao"] = situation
        self.params["tipo"] = type

        url = (
            self.base_url
            + "busca?"
            + "&".join([f"{key}={value}" for key, value in self.params.items()])
        )

        return url

    @retry(
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=2, min=2, max=30),
        reraise=True,
    )
    async def _get_documents_html_links(self, url: str) -> "list[dict]":
        """Get html links from given url. Returns a list of dictionaries in the format {
            "title": str,
            "summary": str,
            "html_link": str
        }"""
        documents_html_links_info = []

        # Get soup from url
        soup = await self.request_service.get_soup(url)

        if soup is None:
            return documents_html_links_info

        # Get all documents html links from page
        documents = soup.find_all("li", class_="busca-resultados__item")
        for document in documents:
            a_tag = document.find("h3", class_="busca-resultados__cabecalho").find("a")
            document_html_link = a_tag["href"]
            title = a_tag.text.strip()
            summary = document.find(
                "p", class_="busca-resultados__descricao js-fade-read-more"
            ).text.strip()
            documents_html_links_info.append(
                {
                    "title": title,
                    "summary": summary,
                    "html_link": document_html_link,
                }
            )
        if not documents_html_links_info:
            if self.verbose:
                logger.info(f"No documents found for url: {url}")
            return documents_html_links_info

        return documents_html_links_info

    async def _get_document_text_link(
        self, document_html_link: str, title: str, summary: str
    ) -> Optional[dict]:
        """Get proper document text link from given document html link"""

        soup = await self.request_service.get_soup(document_html_link)
        if soup is None:
            logger.error(f"Could not get soup for document: {title}")
            error_data = {
                "title": title,
                "year": self.params["ano"],
                "situation": self.params["situacao"],
                "type": self.params["tipo"],
                "summary": summary,
                "html_link": document_html_link,
            }
            await self.saver.save_error(
                error_data, error_message="Could not fetch page HTML"
            )
            return None

        # check if not found (text not available)
        not_found = soup.find("h1", text="Not Found")
        if not_found:
            logger.warning(f"Document not found: {title}")
            await self.saver.save_error(
                {
                    "title": title,
                    "year": self.params["ano"],
                    "situation": self.params["situacao"],
                    "type": self.params["tipo"],
                    "summary": summary,
                    "html_link": document_html_link,
                },
                error_message="Document text not found (404)",
            )
            return None

        document_text_links = soup.find("div", class_="sessao")
        if not document_text_links:
            # probably link doesn't exist (error in website)
            logger.error(f"Could not find text link for document: {title}")
            error_data = {
                "title": title,
                "year": self.params["ano"],
                "situation": self.params["situacao"],
                "type": self.params["tipo"],
                "summary": summary,
                "html_link": document_html_link,
                "soup": str(soup.prettify()),  # include soup for debugging
            }
            await self.saver.save_error(
                error_data,
                error_message="Could not find text link div.sessao in page",
            )
            return None

        document_text_link = None

        if document_text_links and hasattr(document_text_links, "find_all"):
            original_doc_text_links = []
            repub_doc_text_links = []
            doc_text_links_list = []

            for link in document_text_links.find_all("a"):
                link_text = link.text.strip().lower()
                if "texto - publicação original" in link_text:
                    original_doc_text_links.append(link)
                elif "texto - republicação" in link_text:
                    repub_doc_text_links.append(link)
                elif "texto -" in link_text:
                    doc_text_links_list.append(link)
                elif "texto" in link_text:
                    doc_text_links_list.append(link)

            # priority for the link with "texto - republicação", then "texto - publicação original", and then any link with "texto -"
            if repub_doc_text_links:
                # if there is a link with "texto - republicação" text, use it
                document_text_link = repub_doc_text_links[-1]["href"]
                document_text_link = urljoin(document_html_link, document_text_link)
            elif original_doc_text_links:
                # if there is a link with "texto - publicação original" text, use it
                document_text_link = original_doc_text_links[-1]["href"]
                document_text_link = urljoin(document_html_link, document_text_link)
            elif doc_text_links_list:
                # if there is a link with "texto -" or just "texto" in the text, use it
                document_text_link = doc_text_links_list[-1]["href"]
                document_text_link = urljoin(document_html_link, document_text_link)

        if document_text_link is None:
            logger.error(f"Could not find text link for document: {title}")
            error_data = {
                "title": title,
                "year": self.params["ano"],
                "situation": self.params["situacao"],
                "type": self.params["tipo"],
                "summary": summary,
                "html_link": document_html_link,
                "soup": str(soup.prettify()),  # include soup for debugging
            }
            await self.saver.save_error(
                error_data,
                error_message="No text link found in page anchors",
            )
            return None

        return {"title": title, "summary": summary, "html_link": document_text_link}

    async def _get_document_data(
        self, document_text_link: str, title: str, summary: str
    ) -> Optional[dict]:
        """Get data from given document text link using docling to convert HTML to markdown.
        Data will be in the format {
            "title": str,
            "summary": str,
            "html_string": str,
            "text_markdown": str,
            "document_url": str
        }"""
        try:
            # Define filtering rules for document conversion
            remove_contents = {
                DocItemLabel.TITLE: ["Legislação"],
                DocItemLabel.LIST_ITEM: ["Dados da Norma"],
                DocItemLabel.TEXT: ["Por favor, aguarde.", "Veja também:"],
                DocItemLabel.SECTION_HEADER: ["Legislação Informatizada", "Carregando"],
            }

            # Use base scraper's _get_markdown with filtering
            text_markdown = await self._get_markdown(
                url=document_text_link,
                remove_contents=remove_contents,
                remove_hyperlinks=True,
                export_md_kwargs={"included_content_layers": {ContentLayer.BODY}},
            )

            if not text_markdown or not text_markdown.strip():
                logger.warning(f"Document text is empty after conversion: {title}")
                error_data = {
                    "title": title,
                    "year": self.params["ano"],
                    "situation": self.params["situacao"],
                    "type": self.params["tipo"],
                    "summary": summary,
                    "html_link": document_text_link,
                }
                await self.saver.save_error(
                    error_data,
                    error_message="Document text is empty after docling conversion",
                )
                return None

            # Get HTML string by converting again without filtering (for storage)
            # We need a separate conversion for HTML export
            converter = DocumentConverter()
            conversion = await run_in_thread(
                converter.convert, source=document_text_link
            )
            doc = conversion.document

            # Apply same filtering to HTML export
            items_to_delete = []
            for item, item_index in doc.iterate_items():
                if item.content_layer == ContentLayer.BODY:
                    keywords = remove_contents.get(item.label, [])
                    if any(keyword in item.text for keyword in keywords):
                        items_to_delete.append(item)
                    if hasattr(item, "hyperlink"):
                        item.hyperlink = None

            doc.delete_items(node_items=items_to_delete)

            html_string = doc.export_to_html(
                included_content_layers={ContentLayer.BODY},
            )

            return {
                "title": title,
                "summary": summary,
                "html_string": html_string,
                "text_markdown": text_markdown.strip(),
                "document_url": document_text_link,
            }
        except Exception as e:
            logger.error(f"Error converting document to markdown: {title} - {e}")
            error_data = {
                "title": title,
                "year": self.params["ano"],
                "situation": self.params["situacao"],
                "type": self.params["tipo"],
                "summary": summary,
                "html_link": document_text_link,
            }
            await self.saver.save_error(error_data, error_message=str(e))
            return None

    async def _scrape_situation_type(
        self, year: int, situation: str, type: str
    ) -> list:
        """Scrape data for a specific year, situation, and type combination"""
        results = []

        url = self._format_search_url(str(year), situation, type)
        per_page = 20
        soup = await self.request_service.get_soup(url)

        if soup is None:
            logger.warning(f"Could not get soup for url: {url}")
            return results

        total_element = soup.find(
            "div",
            class_="busca-info__resultado busca-info__resultado--informado",
        )

        if total_element is None:
            logger.warning(f"Could not find total element for url: {url}")
            return results

        total = total_element.text
        total = int(total.strip().split()[-1])

        if total == 0:
            return results
        pages = total // per_page + 1

        # Get documents html links from all pages
        documents_html_links_info = []
        tasks = [
            self._get_documents_html_links(url + f"&pagina={page}")
            for page in range(1, pages + 1)
        ]
        page_results = await asyncio.gather(*tasks, return_exceptions=True)
        for result in page_results:
            if isinstance(result, Exception):
                logger.error(f"Error fetching page: {result}")
                continue
            if result is not None:
                if isinstance(result, list):
                    documents_html_links_info.extend(result)
                else:
                    logger.warning(f"Unexpected result type: {type(result)}")
                    logger.warning(f"Result: {result}")

        # Get proper document text link from each document html link
        documents_text_links = []
        tasks = [
            self._get_document_text_link(
                document_html_link.get("html_link"),
                document_html_link.get("title"),
                document_html_link.get("summary"),
            )
            for document_html_link in documents_html_links_info
            if document_html_link is not None
        ]
        text_link_results = await asyncio.gather(*tasks)
        for result in text_link_results:
            documents_text_links.append(result)

        # Get data from all documents text links
        tasks = [
            self._get_document_data(
                document_text_link.get("html_link"),
                document_text_link.get("title"),
                document_text_link.get("summary"),
            )
            for document_text_link in documents_text_links
            if document_text_link is not None
        ]
        doc_data_results = await asyncio.gather(*tasks)
        for result in doc_data_results:
            if result is None:
                continue

            queue_item = {
                "year": year,
                "situation": situation,
                "type": type,
                **result,
            }
            results.append(queue_item)

        if self.verbose:
            logger.info(
                f"Finished scraping for Year: {year} | Situation: {situation} | Type: {type} | Results: {len(results)}"
            )

        return results

    async def _scrape_year(self, year: int) -> list:
        """Scrape data from given year"""
        tasks = [
            self._scrape_situation_type(year, situation, type)
            for situation in self.situations
            for type in self.types
        ]
        valid = await self._gather_results(
            tasks,
            context={"year": year, "type": "NA", "situation": "NA"},
            desc=f"{self.name} | Year {year}",
        )
        return [
            item
            for result in valid
            for item in (result if isinstance(result, list) else [result])
        ]
