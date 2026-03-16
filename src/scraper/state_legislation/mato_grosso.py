from __future__ import annotations
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from src.scraper.base.schemas import ScrapedDocument
import re
from urllib.parse import urljoin, urlencode
from bs4 import BeautifulSoup
from loguru import logger
from src.scraper.base.converter import calc_pages, strip_html_chrome, valid_markdown
from src.scraper.base.scraper import StateScraper


TYPES = {
    "Constituição Estadual": 1,
    "Emenda Constitucional": 2,
    "Lei Complementar": 3,
    "Lei Ordinária": 4,
    "Decreto Legislativo": 6,
    "Resolução": 7,
    "Ato": 8,
}

HISTORIC_TYPES = {
    "Emenda Constitucional": "emenda-constitucional",
    "Lei Complementar": "lei-complementar",
    "Lei Ordinária": "lei-ordinaria",
    "Lei Provincial": "lei-provincial",
    "Decreto Legislativo": "decreto-legislativo",
    "Resolução": "resolucao",
    "Resolução Provincial": "resolucao-provincial",
    "Regulamento": "regulamento",
}  # types for historic data (https://www.al.mt.gov.br/norma-juridica/pesquisa-historica)

# situations are gotten from doc data while scraping
SITUATIONS = {}


class MTAlmtScraper(StateScraper):
    """Webscraper for Mato Grosso state legislation website (https://www.al.mt.gov.br/norma-juridica)

    Year start (earliest on source): 2017

    Example search request: https://www.al.mt.gov.br/norma-juridica

    params = {
        almt_form_norma_juridica_ato_busca_avancada[atoTipo][autocomplete]: 4
        almt_form_norma_juridica_ato_busca_avancada[conteudoDispositivo]:
        almt_form_norma_juridica_ato_busca_avancada[ementa]:
        almt_form_norma_juridica_ato_busca_avancada[numero]:
        almt_form_norma_juridica_ato_busca_avancada[ano]: 1977
        almt_form_norma_juridica_ato_busca_avancada[autor][autocomplete]:
        almt_form_norma_juridica_ato_busca_avancada[apelido]:
        almt_form_norma_juridica_ato_busca_avancada[tagCondicao]: e
        almt_form_norma_juridica_ato_busca_avancada[dataPublicacaoDe]:
        almt_form_norma_juridica_ato_busca_avancada[dataPublicacaoAte]:
        almt_form_norma_juridica_ato_busca_avancada[dataPromulgacaoDe]:
        almt_form_norma_juridica_ato_busca_avancada[dataPromulgacaoAte]:
        almt_form_norma_juridica_ato_busca_avancada[dataInicioVigenciaDe]:
        almt_form_norma_juridica_ato_busca_avancada[dataInicioVigenciaAte]:
        almt_form_norma_juridica_ato_busca_avancada[dataFimVigenciaDe]:
        almt_form_norma_juridica_ato_busca_avancada[dataFimVigenciaAte]:
        almt_form_norma_juridica_ato_busca_avancada[revogarNormaJuridica]: nao
        almt_form_norma_juridica_ato_busca_avancada[possuiVeto]:
        almt_form_norma_juridica_ato_busca_avancada[possuiRemissao]:
        almt_form_norma_juridica_ato_busca_avancada[_token]: token
        page: 1
    }

    Example search request for historic data: https://www.al.mt.gov.br/norma-juridica/pesquisa-historica

    params = {
        almt_form_norma_juridica_pesquisa_historica[tipo]: lei-ordinaria
        almt_form_norma_juridica_pesquisa_historica[restringeBusca]: c
        almt_form_norma_juridica_pesquisa_historica[palavraChave]:
        almt_form_norma_juridica_pesquisa_historica[numero]:
        almt_form_norma_juridica_pesquisa_historica[ano]: 1958
        almt_form_norma_juridica_pesquisa_historica[observacao]:
        almt_form_norma_juridica_pesquisa_historica[dataInicio]:
        almt_form_norma_juridica_pesquisa_historica[dataFim]:
        almt_form_norma_juridica_pesquisa_historica[_token]: token
        page: 1
    }
    """

    def __init__(self, base_url: str = "https://www.al.mt.gov.br", **kwargs):
        super().__init__(
            base_url, types=TYPES, situations=SITUATIONS, name="MATO_GROSSO", **kwargs
        )
        self.historic_types = HISTORIC_TYPES
        self.max_year_historic = 1978
        self.min_year = 1979
        self.token = None
        self.regex_total_items = re.compile(r"Total de registros:\s+([\d.]+)")

    def _infer_norm_type(self, title: str, *, is_historic: bool = False) -> str:
        """Infer the norm type from a listing title when the dash separator is absent."""
        normalized_title = " ".join(title.split())
        type_names = HISTORIC_TYPES if is_historic else TYPES
        for type_name in sorted(type_names, key=len, reverse=True):
            if normalized_title.casefold().startswith(type_name.casefold()):
                return type_name
        return ""

    async def _set_token(self):
        """Get token for search request (optional — field was removed from the form)."""
        url = f"{self.base_url}/norma-juridica"
        soup = await self.request_service.get_soup(url)
        if not soup:
            self.token = ""
            return
        token_element = soup.find(
            "input", {"name": "almt_form_norma_juridica_ato_busca_avancada[_token]"}
        )
        self.token = token_element["value"] if token_element else ""

    def _build_search_url(
        self, norm_type_id: str, year: int, page: int, is_historic: bool = False
    ) -> str:
        """Build search URL from arguments (no shared state mutation)."""
        if is_historic:
            params = {
                "almt_form_norma_juridica_pesquisa_historica[tipo]": norm_type_id,
                "almt_form_norma_juridica_pesquisa_historica[restringeBusca]": "c",
                "almt_form_norma_juridica_pesquisa_historica[palavraChave]": "",
                "almt_form_norma_juridica_pesquisa_historica[numero]": "",
                "almt_form_norma_juridica_pesquisa_historica[ano]": year,
                "almt_form_norma_juridica_pesquisa_historica[observacao]": "",
                "almt_form_norma_juridica_pesquisa_historica[dataInicio]": "",
                "almt_form_norma_juridica_pesquisa_historica[dataFim]": "",
                "almt_form_norma_juridica_pesquisa_historica[_token]": self.token or "",
                "page": page,
            }
            return (
                f"{self.base_url}/norma-juridica/pesquisa-historica?{urlencode(params)}"
            )
        else:
            params = {
                "almt_form_norma_juridica_ato_busca_avancada[atoTipo][autocomplete]": norm_type_id,
                "almt_form_norma_juridica_ato_busca_avancada[conteudoDispositivo]": "",
                "almt_form_norma_juridica_ato_busca_avancada[ementa]": "",
                "almt_form_norma_juridica_ato_busca_avancada[numero]": "",
                "almt_form_norma_juridica_ato_busca_avancada[ano]": year,
                "almt_form_norma_juridica_ato_busca_avancada[autor][autocomplete]": "",
                "almt_form_norma_juridica_ato_busca_avancada[apelido]": "",
                "almt_form_norma_juridica_ato_busca_avancada[tagCondicao]": "e",
                "almt_form_norma_juridica_ato_busca_avancada[dataPublicacaoDe]": "",
                "almt_form_norma_juridica_ato_busca_avancada[dataPublicacaoAte]": "",
                "almt_form_norma_juridica_ato_busca_avancada[dataPromulgacaoDe]": "",
                "almt_form_norma_juridica_ato_busca_avancada[dataPromulgacaoAte]": "",
                "almt_form_norma_juridica_ato_busca_avancada[dataInicioVigenciaDe]": "",
                "almt_form_norma_juridica_ato_busca_avancada[dataInicioVigenciaAte]": "",
                "almt_form_norma_juridica_ato_busca_avancada[dataFimVigenciaDe]": "",
                "almt_form_norma_juridica_ato_busca_avancada[dataFimVigenciaAte]": "",
                "almt_form_norma_juridica_ato_busca_avancada[revogado]": "",
                "almt_form_norma_juridica_ato_busca_avancada[possuiVeto]": "",
                "almt_form_norma_juridica_ato_busca_avancada[possuiRemissao]": "",
                "page": page,
            }
            return f"{self.base_url}/norma-juridica?{urlencode(params)}"

    def _get_total_norms(self, soup: BeautifulSoup) -> int:
        if not soup:
            return 0

        """Get total number of norms from search page"""
        total_items = self.regex_total_items.search(soup.prettify())
        if total_items:
            return int(total_items.group(1).replace(".", ""))

        return 0

    def _extract_docs_from_soup(
        self, soup: BeautifulSoup, is_historic: bool = False
    ) -> list:
        """Parse document list from a search-results soup.

        Extracts norm_type from each item's title (format: "Lei Ordinária - 12383/2023")
        and includes it as ``"type"`` so _merge_context can override the context type.
        """
        items = soup.find_all(
            lambda tag: tag.name == "div" and tag.get("class") == ["col-12"]
        )
        items = items[:-1]  # last item is pagination
        if not is_historic:
            items = items[2:]  # first two are not norms

        docs = []
        for item in items:
            title_raw = item.find("h5").text.strip()
            norm_type = (
                title_raw.split(" - ")[0].strip()
                if " - " in title_raw
                else self._infer_norm_type(title_raw, is_historic=is_historic)
            )
            summary = item.find("div", class_="text-muted").text.strip()
            links = item.find_all("a", href=True)
            if len(links) < 2:
                continue
            norm_link = links[-1]["href"]
            docs.append(
                {
                    "title": title_raw,
                    "type": norm_type,
                    "summary": summary,
                    "norm_link": norm_link,
                    "document_url": urljoin(self.base_url, norm_link),
                }
            )
        return docs

    async def _get_docs_links(self, url: str, is_historic: bool = False) -> list:
        """Fetch a search-results page and return document dicts."""
        soup = await self.request_service.get_soup(url)
        if not soup:
            logger.error(f"Failed to get soup for url: {url}")
            return []
        return self._extract_docs_from_soup(soup, is_historic)

    async def _get_doc_data(
        self, doc_info: dict, is_historic: bool = False
    ) -> ScrapedDocument | None:
        """Get document data from given document dict"""
        doc_info = dict(doc_info)
        norm_link = doc_info.pop("norm_link")

        if self._is_already_scraped(
            doc_info.get("document_url", ""), doc_info.get("title", "")
        ):
            return None

        # Derive marcoHistorico date from URN path (e.g. "lei.ordinaria:2020-12-30;11281")
        date_match = re.search(r":(\d{4}-\d{2}-\d{2});", norm_link)
        marco = date_match.group(1) if date_match else ""
        norm_base_url = urljoin(self.base_url, norm_link)

        if is_historic:
            ficha_url = norm_base_url
        else:
            ficha_url = f"{norm_base_url}/ficha-tecnica?exibirAnotacao=1"
        compilado_url = (
            f"{norm_base_url}/compilado?exibirAnotacao=1&marcoHistorico={marco}"
        )

        ficha_soup = await self.request_service.get_soup(ficha_url)
        compilado_soup = await self.request_service.get_soup(compilado_url)

        if not ficha_soup:
            logger.error(f"Error getting soup for {ficha_url}")
            await self._save_doc_error(
                title=doc_info.get("title", ""),
                year=doc_info.get("year", ""),
                html_link=ficha_url,
                error_message="Failed to get document page",
            )
            return None

        # Extract metadata from ficha_soup
        author = ficha_soup.find("strong", string=re.compile(r"Autor:|Autores:"))
        if author:
            author = author.find_parent("li").text
            author = re.sub(r"Autor:|Autores:", "", author).strip()

        publication = ficha_soup.find("strong", string="Publicação:")
        if publication:
            publication = (
                publication.find_parent("li").text.replace("Publicação:", "").strip()
            )
        date = ficha_soup.find("strong", string="Data da promulgação:")
        if date:
            date = (
                date.find_parent("li").text.replace("Data da promulgação:", "").strip()
            )

        subject_regex = re.compile(r"Assunto:|Assuntos:")
        subject = ficha_soup.find("strong", string=subject_regex)
        if subject:
            subject = subject.find_parent("li").text
            subject = re.sub(subject_regex, "", subject).strip()

        tags = ficha_soup.find("strong", string="Tags:")
        if tags:
            tags = tags.find_parent("li").text.replace("Tags:", "").strip()
        situation = ficha_soup.find("strong", string="Situação:")
        if situation:
            situation = (
                situation.find_parent("li").text.replace("Situação:", "").strip()
            )

        if not compilado_soup:
            await self._save_doc_error(
                title=doc_info.get("title", ""),
                year=doc_info.get("year", ""),
                html_link=compilado_url,
                error_message="Failed to get compilado page",
            )
            return None

        frame = compilado_soup.find("turbo-frame")
        if not frame:
            await self._save_doc_error(
                title=doc_info.get("title", ""),
                year=doc_info.get("year", ""),
                html_link=compilado_url,
                error_message="turbo-frame not found in compilado page",
            )
            return None

        strip_html_chrome(frame)
        html_str = str(frame)
        text_markdown = await self._get_markdown(html_content=html_str)

        # MHTML captures the canonical norm page (document_url), not the compilado
        capture_url = doc_info.get("document_url", compilado_url)
        try:
            raw_content = await self._capture_mhtml(capture_url)
            content_ext = ".mhtml"
        except Exception as exc:
            logger.warning(f"MHTML capture failed for {capture_url}: {exc}")
            await self._save_doc_error(
                title=doc_info.get("title", ""),
                year=doc_info.get("year", ""),
                html_link=capture_url,
                error_message=f"MHTML capture failed: {exc}",
            )
            return None

        valid, reason = valid_markdown(text_markdown)
        if not valid:
            await self._save_doc_error(
                title=doc_info.get("title", ""),
                year=doc_info.get("year", ""),
                html_link=doc_info.get("document_url", compilado_url),
                error_message=reason,
            )
            return None

        doc_data = {
            **doc_info,
            "author": author if author else "",
            "publication": publication if publication else "",
            "date": date if date else "",
            "subject": subject if subject else "",
            "tags": tags if tags else "",
            "situation": situation if situation else "",
            "text_markdown": text_markdown,
            "raw_content": raw_content,
            "content_extension": content_ext,
        }

        from src.scraper.base.schemas import ScrapedDocument

        return ScrapedDocument(**doc_data)

    async def _scrape_year(self, year: int) -> list[dict]:
        """Scrape all norms for a specific year using a single all-types search."""
        if not self.token:
            await self._set_token()

        is_historic = year <= self.max_year_historic

        # Page 1: fetch once for total count + first page of docs
        url_p1 = self._build_search_url("", year, 1, is_historic)
        soup_p1 = await self.request_service.get_soup(url_p1)
        total_items = self._get_total_norms(soup_p1)
        if total_items == 0:
            return []

        pages = calc_pages(total_items, 10)
        documents = self._extract_docs_from_soup(soup_p1, is_historic)

        # Pages 2..N: fetch concurrently
        if pages > 1:
            tasks = [
                self._get_docs_links(
                    self._build_search_url("", year, page, is_historic), is_historic
                )
                for page in range(2, pages + 1)
            ]
            valid_results = await self._gather_results(
                tasks,
                context={"year": year, "type": "", "situation": "NA"},
                desc=f"MATO GROSSO | {year} | get_docs_links",
            )
            for result in valid_results:
                if result:
                    documents.extend(result)

        return await self._process_documents(
            documents,
            year=year,
            norm_type="",
            situation="Não consta",
            doc_data_kwargs={"is_historic": is_historic},
        )
