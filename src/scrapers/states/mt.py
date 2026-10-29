import asyncio
import re
from collections.abc import AsyncIterator
from typing import Any
from urllib.parse import urlencode, urljoin

from bs4 import BeautifulSoup
from loguru import logger

from src.core.convert import (
    calc_pages,
    infer_type_by_prefix,
    strip_html_chrome,
    valid_markdown,
)
from src.core.document import Document
from src.core.scraper import DEFAULT_VALID_SITUATION, BaseScraper

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
}
SITUATIONS = {}


class MTAlmtScraper(BaseScraper):
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

    types = TYPES
    situations = SITUATIONS
    name = "MATO_GROSSO"
    base_url = "https://www.al.mt.gov.br"

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.historic_types = HISTORIC_TYPES
        self.max_year_historic = 1978
        self.token = None
        self.regex_total_items = re.compile("Total de registros:\\s+([\\d.]+)")

    def _infer_norm_type(self, title: str, *, is_historic: bool = False) -> str:
        return infer_type_by_prefix(
            title, list(HISTORIC_TYPES if is_historic else TYPES)
        )

    async def _set_token(self) -> None:
        """Get token for search request (optional — field was removed from the form)."""
        url = f"{self.base_url}/norma-juridica"
        soup = (await self.http.request(url)).soup()
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
        """Get total number of norms from search page"""
        if not soup:
            return 0
        total_items = self.regex_total_items.search(str(soup))
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
        items = items[:-1]
        if not is_historic:
            items = items[2:]
        docs = []
        for item in items:
            h5 = item.find("h5")
            muted = item.find("div", class_="text-muted")
            if h5 is None or muted is None:
                logger.debug(
                    "Skipping malformed listing item (missing h5 or text-muted)"
                )
                continue
            title_raw = h5.text.strip()
            norm_type = (
                title_raw.split(" - ")[0].strip()
                if " - " in title_raw
                else self._infer_norm_type(title_raw, is_historic=is_historic)
            )
            summary = muted.text.strip()
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
        soup = (await self.http.request(url)).soup()
        if not soup:
            logger.error(f"Failed to get soup for url: {url}")
            return []
        return self._extract_docs_from_soup(soup, is_historic)

    async def fetch(self, doc_info: dict) -> Document | None:
        is_historic = doc_info.get("is_historic", False)
        doc_info = dict(doc_info)
        norm_link = doc_info.pop("norm_link")
        date_match = re.search(":(\\d{4}-\\d{2}-\\d{2});", norm_link)
        marco = date_match.group(1) if date_match else ""
        norm_base_url = urljoin(self.base_url, norm_link)
        if is_historic:
            ficha_url = norm_base_url
        else:
            ficha_url = f"{norm_base_url}/ficha-tecnica?exibirAnotacao=1"
        compilado_url = (
            f"{norm_base_url}/compilado?exibirAnotacao=1&marcoHistorico={marco}"
        )
        responses = await asyncio.gather(
            self.http.request(ficha_url), self.http.request(compilado_url)
        )
        ficha_soup, compilado_soup = [response.soup() for response in responses]
        if not ficha_soup:
            logger.error(f"Error getting soup for {ficha_url}")
            raise ValueError("Failed to get document page")
        author = ficha_soup.find("strong", string=re.compile("Autor:|Autores:"))
        if author:
            author = author.find_parent("li").text
            author = re.sub("Autor:|Autores:", "", author).strip()
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
        subject_regex = re.compile("Assunto:|Assuntos:")
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
            raise ValueError("Failed to get compilado page")
        frame = compilado_soup.find("turbo-frame")
        if not frame:
            raise ValueError("turbo-frame not found in compilado page")
        strip_html_chrome(frame)
        html_str = str(frame)
        text_markdown = await self.markdown(html=html_str)
        valid, reason = valid_markdown(text_markdown)
        if not valid:
            raise ValueError(reason)
        capture_url = doc_info.get("document_url", compilado_url)
        try:
            raw = (await self.browser.snapshot(capture_url))[1]
            content_ext = ".mhtml"
        except Exception as exc:
            logger.warning(f"MHTML capture failed for {capture_url}: {exc}")
            raise ValueError(f"MHTML capture failed: {exc}")
        doc_data = {
            **doc_info,
            "author": author if author else "",
            "publication": publication if publication else "",
            "date": date if date else "",
            "subject": subject if subject else "",
            "tags": tags if tags else "",
            "situation": situation if situation else doc_info.get("situation", ""),
            "text_markdown": text_markdown,
            "raw": raw,
            "raw_ext": content_ext,
        }
        return Document(**doc_data)

    async def list_documents(self, year: int) -> AsyncIterator[dict]:
        if not self.token:
            await self._set_token()
        historic = year <= self.max_year_historic
        soup = (
            await self.http.request(self._build_search_url("", year, 1, historic))
        ).soup()

        async def fetch_page(page: int) -> list[dict]:
            return (
                self._extract_docs_from_soup(soup, historic)
                if page == 1
                else await self._get_docs_links(
                    self._build_search_url("", year, page, historic), historic
                )
            )

        async for ref in self.paginate(
            fetch_page, calc_pages(self._get_total_norms(soup), 10)
        ):
            yield {
                "year": year,
                "situation": DEFAULT_VALID_SITUATION,
                **ref,
                "is_historic": historic,
                "document_url": urljoin(self.base_url, ref["norm_link"]),
            }
