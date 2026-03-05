import asyncio
import re
from urllib.parse import urljoin
from bs4 import BeautifulSoup, Tag
from src.scraper.base.scraper import StateScraper
from loguru import logger


# Types based on the website dropdown and links
TYPES = {
    "Ato Administrativo Normativo": 0,
    "Ato Administrativo Parlamentar": 1,
    "Constituição Estadual": 2,
    "Decreto do Executivo": 3,
    "Decreto Legislativo": 4,
    "Decreto-Lei": 5,
    "Emenda Constitucional": 6,
    "Lei Complementar": 7,
    "Lei Delegada": 8,
    "Lei Ordinária": 9,
    "Lei Provincial": 10,
    "Portaria Administrativa da Alepe": 11,
    "Resolução da Alepe": 12,
    "Resolução do Poder Judiciário": 13,
}

# Pernambuco website doesn't seem to have explicit situation filters. Situation will be inferred from document information.
VALID_SITUATIONS = []

INVALID_SITUATIONS = []

SITUATIONS = VALID_SITUATIONS + INVALID_SITUATIONS


class PernambucoAlepeScraper(StateScraper):
    """Webscraper for Assembleia Legislativa de Pernambuco website (https://legis.alepe.pe.gov.br)

    This website uses ASP.NET forms with viewstate for search functionality.

    Example search URL: https://legis.alepe.pe.gov.br/pesquisaAvancada.aspx

    payload = {
        "__EVENTTARGET": "",  # This is usually empty for initial load
        "__EVENTARGUMENT: "",  # This is usually empty for initial load
        "__VIEWSTATE": "",  # ASP.NET viewstate
        "__VIEWSTATEGENERATOR": "",  # ASP.NET viewstate generator
        "__EVENTVALIDATION": "",  # ASP.NET event validation
        "ctl00$hfUrl": "https://legis.alepe.pe.gov.br/Paginas/pesquisaAvancada.aspx",
        "ctl00$tbxLogin": "",
        "ctl00$tbxSenha": "",
        "ctl00$conteudo$tbxNumero": "",
        "ctl00$conteudo$tbxAno": "2001",
        "ctl00$conteudo$cblTipoNorma$cblTipoNorma_3": "Decreto do Executivo",
        "ctl00$conteudo$tbxTextoPesquisa": "",
        "ctl00$conteudo$tbxTextoPesquisaNeg": "",
        "ctl00$conteudo$cbxlPesquisa$cbxlPesquisa_0": "CONTTXTORIGINAL",
        "ctl00$conteudo$cbxlPesquisa$cbxlPesquisa_1": "EMENTA",
        "ctl00$conteudo$cbxlPesquisa$cbxlPesquisa_2": "APELIDO",
        "ctl00$conteudo$cbxlPesquisa$cbxlPesquisa_3": "NOME",
        "ctl00$conteudo$tbxThesaurus": "",
        "ctl00$conteudo$rbOpThesaurus": "Todos",
        "ctl00$conteudo$tbxAssuntoGeral": "",
        "ctl00$conteudo$rbOpAssuntoGeral": "Todos",
        "ctl00$conteudo$tbxDataInicialNorma": "",
        "ctl00$conteudo$tbxDataFinalNorma": "",
        "ctl00$conteudo$ddlPublicacao": "",
        "ctl00$conteudo$tbxDataInicialPublicacao": "",
        "ctl00$conteudo$tbxDataFinalPublicacao": "",
        "ctl00$conteudo$tbxIniciativa": "",
        "ctl00$conteudo$tbxNumeroProjeto": "",
        "ctl00$conteudo$tbxAnoProjeto": "",
        "ctl00$conteudo$btnPesquisar": "Pesquisar",
        "ctl00$tbxNomeErro": "",
        "ctl00$tbxEmailErro": "",
        "ctl00$tbxMensagemErro": "",
        "ctl00$tbxLoginMob": "",
        "ctl00$tbxSenhaMob": ""
    }
    """

    def __init__(
        self,
        base_url: str = "https://legis.alepe.pe.gov.br",
        **kwargs,
    ):
        super().__init__(
            base_url, types=TYPES, situations=SITUATIONS, name="PERNAMBUCO", **kwargs
        )
        self.params = {
            "__EVENTTARGET": "",
            "__EVENTARGUMENT": "",
            "__VIEWSTATE": "",
            "__VIEWSTATEGENERATOR": "",
            "__EVENTVALIDATION": "",
            "ctl00$hfUrl": "https://legis.alepe.pe.gov.br/Paginas/pesquisaAvancada.aspx",
            "ctl00$tbxLogin": "",
            "ctl00$tbxSenha": "",
            "ctl00$conteudo$tbxNumero": "",
            "ctl00$conteudo$tbxAno": "",
            "ctl00$conteudo$tbxTextoPesquisa": "",
            "ctl00$conteudo$tbxTextoPesquisaNeg": "",
            "ctl00$conteudo$cbxlPesquisa$cbxlPesquisa_0": "CONTTXTORIGINAL",
            "ctl00$conteudo$cbxlPesquisa$cbxlPesquisa_1": "EMENTA",
            "ctl00$conteudo$cbxlPesquisa$cbxlPesquisa_2": "APELIDO",
            "ctl00$conteudo$cbxlPesquisa$cbxlPesquisa_3": "NOME",
            "ctl00$conteudo$tbxThesaurus": "",
            "ctl00$conteudo$rbOpThesaurus": "Todos",
            "ctl00$conteudo$tbxAssuntoGeral": "",
            "ctl00$conteudo$rbOpAssuntoGeral": "Todos",
            "ctl00$conteudo$tbxDataInicialNorma": "",
            "ctl00$conteudo$tbxDataFinalNorma": "",
            "ctl00$conteudo$ddlPublicacao": "",
            "ctl00$conteudo$tbxDataInicialPublicacao": "",
            "ctl00$conteudo$tbxDataFinalPublicacao": "",
            "ctl00$conteudo$tbxIniciativa": "",
            "ctl00$conteudo$tbxNumeroProjeto": "",
            "ctl00$conteudo$tbxAnoProjeto": "",
            "ctl00$conteudo$btnPesquisar": "Pesquisar",
            "ctl00$tbxNomeErro": "",
            "ctl00$tbxEmailErro": "",
            "ctl00$tbxMensagemErro": "",
            "ctl00$tbxLoginMob": "",
            "ctl00$tbxSenhaMob": "",
        }

    def _get_form_state(self, soup: BeautifulSoup) -> dict[str, str]:
        """Extract ASP.NET form state from page"""
        state = {}

        # Get viewstate
        viewstate_input = soup.find("input", {"name": "__VIEWSTATE"})
        if viewstate_input and isinstance(viewstate_input, Tag):
            state["__VIEWSTATE"] = viewstate_input.get("value", "")

        # Get viewstate generator
        viewstate_gen_input = soup.find("input", {"name": "__VIEWSTATEGENERATOR"})
        if viewstate_gen_input and isinstance(viewstate_gen_input, Tag):
            state["__VIEWSTATEGENERATOR"] = viewstate_gen_input.get("value", "")

        # Get event validation
        event_val_input = soup.find("input", {"name": "__EVENTVALIDATION"})
        if event_val_input and isinstance(event_val_input, Tag):
            state["__EVENTVALIDATION"] = event_val_input.get("value", "")

        return state

    def _format_search_url(
        self, norm_type: str, norm_type_id: str, year: int
    ) -> tuple[str, dict[str, str]]:
        """Format url for search request"""
        url = urljoin(self.base_url, "/Paginas/pesquisaAvancada.aspx")

        params = self.params.copy()
        params[f"ctl00$conteudo$cblTipoNorma$cblTipoNorma_{norm_type_id}"] = norm_type
        params["ctl00$conteudo$tbxAno"] = str(year)

        return url, params

    def _extract_documents(self, soup: BeautifulSoup) -> list[dict]:
        """Extract document links from search results page"""
        docs = []

        div_resultado = soup.find("div", id="divResultado")
        if not div_resultado:
            return []

        table = div_resultado.find("table")
        if not table:
            return []

        tbody = table.find("tbody")
        if not tbody:
            return []

        items = tbody.find_all("tr")

        for item in items:
            try:
                title_span = item.find("span", class_="nome-norma")
                if not title_span:
                    continue
                title = title_span.text.strip()

                summary_div = item.find("div", class_="fLeft")
                summary = summary_div.text.strip() if summary_div else ""

                additional_data_td = item.find("td", class_="ementa-norma")
                additional_data_url = ""
                if additional_data_td:
                    additional_data_link = additional_data_td.find("a", href=True)
                    if additional_data_link:
                        additional_data_url = urljoin(
                            self.base_url, additional_data_link["href"]
                        )

                document_link = item.find("a", href=True)
                document_url = ""
                if document_link:
                    document_url = urljoin(self.base_url, document_link["href"])

                if title and document_url:  # Only add if we have essential data
                    docs.append(
                        {
                            "title": title,
                            "summary": summary,
                            "additional_data_url": additional_data_url,
                            "document_url": document_url,
                        }
                    )
            except Exception as e:
                logger.error(f"Error parsing item: {e}")
                continue

        return docs

    async def _get_additional_data(self, url: str) -> dict[str, str | int | None]:
        """Get additional data from the document page. Returns a dict with keys 'situation', 'date', 'initiative', 'publication', 'subject', 'updates'."""
        soup = await self.request_service.get_soup(url)
        if not soup:
            logger.warning(f"Failed to retrieve additional data for URL: {url}")
            return None

        # Extract additional data
        additional_data = {}

        # Check if the document is revoked
        revoked_div = soup.find("div", id="divRevogada")
        if revoked_div:
            additional_data["situation"] = "Revogada"
        else:
            additional_data["situation"] = "Não consta revogação expressa"

        # Extract date
        date_td = soup.find("td", text=re.compile(r"\d{2}/\d{2}/\d{4}"))
        additional_data["date"] = date_td.text.strip() if date_td else ""

        # Extract initiative
        initiative = soup.find("th", text="Iniciativa")
        if initiative and isinstance(initiative, Tag):
            additional_data["initiative"] = initiative.find_next_sibling(
                "td"
            ).text.strip()
        else:
            additional_data["initiative"] = ""

        # Extract publication
        publication = soup.find("th", text="Publicação")
        if publication and isinstance(publication, Tag):
            additional_data["publication"] = publication.find_next_sibling(
                "td"
            ).text.strip()
        else:
            additional_data["publication"] = ""

        # Extract subject
        subject = soup.find("th", text="Assunto Geral")
        if subject and isinstance(subject, Tag):
            additional_data["subject"] = subject.find_next_sibling("td").text.strip()
        else:
            additional_data["subject"] = ""

        # Extract updates
        updates_div = soup.find("div", class_="lista-atualizacoes")
        additional_data["updates"] = updates_div.text.strip() if updates_div else ""

        return additional_data

    async def _get_doc_data(self, doc_info: dict, year: int, norm_type: str) -> dict:
        """Get document data from document link"""
        url = doc_info.get("document_url")

        if self._is_already_scraped(url, doc_info.get("title", "")):
            return None

        soup = await self.request_service.get_soup(url)
        if not soup:
            logger.error(f"Failed to retrieve document data for URL: {url}")
            await self._save_doc_error(
                title=doc_info.get("title", ""),
                year=doc_info.get("year", ""),
                html_link=url,
                error_message="Failed to retrieve document page",
            )
            return None

        # Extract document content
        content_div = soup.find("div", class_="WordSection1")
        html_string = content_div.prettify()

        html_string = self._wrap_html(html_string)

        # Use direct HTML content conversion
        text_markdown = await self._get_markdown(html_content=html_string)

        if not text_markdown:
            logger.warning(f"Failed to convert HTML to Markdown for URL: {url}")
            await self._save_doc_error(
                title=doc_info.get("title", ""),
                year=doc_info.get("year", ""),
                html_link=url,
                error_message="Failed to convert HTML to markdown",
            )
            return None

        doc_info["html_string"] = html_string
        doc_info["text_markdown"] = text_markdown
        doc_info["_raw_content"] = html_string.encode("utf-8")
        doc_info["_content_extension"] = ".html"

        doc_info["year"] = year
        doc_info["type"] = norm_type

        # Get additional data
        additional_data = await self._get_additional_data(
            doc_info.pop("additional_data_url")
        )
        if not additional_data:
            logger.warning(f"Failed to retrieve additional data for URL: {url}")
            return doc_info

        doc_info.update(additional_data)

        return doc_info

    async def _get_docs_links(
        self, url: str, base_params: dict, norm_type: str, year: int
    ) -> list[dict]:
        """Get document links and metadata by paginating through search results"""
        # Get initial form state using regular request
        soup = await self.request_service.get_soup(url)
        if not soup:
            logger.warning(f"Failed to retrieve initial page for URL: {url}")
            return []

        form_state = self._get_form_state(soup)
        if not form_state:
            logger.warning(f"Failed to retrieve form state for URL: {url}")
            return []

        params = base_params.copy()
        params.update(form_state)

        documents = []
        current_page = 1

        while True:
            # For page > 1, update params for pagination
            if current_page > 1:
                params["__EVENTTARGET"] = f"ctl00$conteudo$lbtn{current_page}"
                params["__LASTFOCUS"] = ""
                params["ctl00$conteudo$hfPage"] = "0"
                params.pop("ctl00$conteudo$btnPesquisar", None)

            response = await self.request_service.make_request(
                url, method="POST", payload=params
            )
            if not response:
                logger.error(f"Failed to make POST request for page {current_page}")
                break

            html = await response.text(errors="replace")
            page_soup = BeautifulSoup(html, "html.parser")

            docs = self._extract_documents(page_soup)
            if not docs:
                break

            documents.extend(docs)
            if self.verbose:
                logger.info(
                    f"PERNAMBUCO | {norm_type} | Year {year} | Page {current_page} | Found {len(docs)} docs on page"
                )

            # Check if there is a next page
            next_page = current_page + 1
            next_btn = page_soup.find("a", id=f"lbtn{next_page}")
            if not next_btn:
                break

            # Extract new form state for next request
            new_state = self._get_form_state(page_soup)
            params.update(new_state)
            current_page = next_page

            # Small delay to be polite
            await asyncio.sleep(1)

        return documents

    async def _scrape_type(
        self, norm_type: str, norm_type_id: int, year: int
    ) -> list[dict]:
        """Scrape norms for a specific type and year"""
        url, base_params = self._format_search_url(norm_type, str(norm_type_id), year)

        documents = await self._get_docs_links(url, base_params, norm_type, year)
        if not documents:
            return []

        # Get document data concurrently now that we have all links
        ctx = {"year": year, "type": norm_type, "situation": "N/A"}
        tasks = [
            self._with_save(self._get_doc_data(doc_info, year, norm_type), ctx)
            for doc_info in documents
        ]
        results = await self._gather_results(
            tasks,
            context=ctx,
            desc=f"PERNAMBUCO | {norm_type}",
        )

        if self.verbose:
            logger.info(
                f"Finished scraping for Year: {year} | Type: {norm_type} | Results: {len(results)} | Total: {self.count}"
            )

        return results

    # _scrape_year uses default from BaseScraper
