import re
from io import BytesIO
from typing import Optional
from urllib.parse import urljoin, urlencode
from bs4 import BeautifulSoup
import asyncio
from loguru import logger
from tenacity import retry, stop_after_attempt, wait_exponential
from src.scraper.base.scraper import BaseScraper, STATE_LEGISLATION_SAVE_DIR


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
}  # types to be used when scraping historic data (https://www.al.mt.gov.br/norma-juridica/pesquisa-historica)

# situations are gotten from doc data while scraping
VALID_SITUATIONS = []
INVALID_SITUATIONS = []  # norms with these situations are invalid norms (no longer have legal effect)

# the reason to have invalid situations is in case we need to train a classifier to predict if a norm is valid or something else similar
SITUATIONS = VALID_SITUATIONS + INVALID_SITUATIONS


class MTAlmtScraper(BaseScraper):
    """Webscraper for Mato Grosso state legislation website (https://www.al.mt.gov.br/norma-juridica)

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
        if STATE_LEGISLATION_SAVE_DIR:
            kwargs.setdefault("docs_save_dir", STATE_LEGISLATION_SAVE_DIR)
        super().__init__(
            base_url, types=TYPES, situations=SITUATIONS, name="MATO_GROSSO", **kwargs
        )
        self.historic_types = HISTORIC_TYPES
        self.max_year_historic = 1978
        self.min_year = 1979
        self.token = None
        self.regex_total_items = re.compile(r"Total de registros:\n\s+(\d+)\n")
        self.header_remove_regex = re.compile(
            r"http://www.al.mt.gov.br/TNX/viewLegislacao.php\?cod=\d+"
        )

    @retry(
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=2, min=2, max=30),
        reraise=True,
    )
    async def _set_token(self):
        """Get token for search request"""
        url = f"{self.base_url}/norma-juridica"
        soup = await self.request_service.get_soup(url)
        token_element = soup.find(
            "input", {"name": "almt_form_norma_juridica_ato_busca_avancada[_token]"}
        )
        if not token_element:
            raise ValueError("Token element not found in the page")

        token = token_element["value"]
        self.token = token

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
                "almt_form_norma_juridica_ato_busca_avancada[revogarNormaJuridica]": "nao",
                "almt_form_norma_juridica_ato_busca_avancada[possuiVeto]": "",
                "almt_form_norma_juridica_ato_busca_avancada[possuiRemissao]": "",
                "almt_form_norma_juridica_ato_busca_avancada[_token]": self.token or "",
                "page": page,
            }
            return f"{self.base_url}/norma-juridica?{urlencode(params)}"

    def _get_total_norms(self, soup: BeautifulSoup) -> int:
        if not soup:
            return 0

        """Get total number of norms from search page"""
        total_items = self.regex_total_items.search(soup.prettify())
        if total_items:
            return int(total_items.group(1))

        return 0

    async def _get_docs_links(self, url: str, is_historic: bool = False) -> list:
        """Get documents html links from given page.
        Returns a list of dicts with keys 'title', 'summary', 'norm_link', 'document_url'
        """
        soup = await self.request_service.get_soup(url)

        if not soup:
            logger.error(f"Failed to get soup for url: {url}")
            return []

        # check if the page is empty (no norms found for the given search)
        total_items = self._get_total_norms(soup)
        if total_items == 0:
            return []

        docs = []
        # items = soup.find_all("div", class_="col-12")
        # exact match for class name == "col-12"
        items = soup.find_all(
            lambda tag: tag.name == "div" and tag.get("class") == ["col-12"]
        )

        # last item is the pagination, remove it
        items = items[:-1]
        # for non-historic search, skip two first items (they are not norms)
        if not is_historic:
            items = items[2:]

        for item in items:
            title = item.find("h5").text.strip()
            summary = item.find("div", class_="text-muted").text.strip()
            links = item.find_all("a", href=True)

            if len(links) < 2:  # some documents are not available, so we skip them
                continue
            document_url = links[0]["href"]
            norm_link = links[-1]["href"]
            # last link is the one to the norm, some norms include a link for proposition, that's why we need to get the last link
            docs.append(
                {
                    "title": title,
                    "summary": summary,
                    "norm_link": norm_link,
                    "document_url": urljoin(self.base_url, document_url),
                }
            )

        return docs

    async def _get_doc_data(
        self, doc_info: dict, is_historic: bool = False
    ) -> Optional[dict]:
        """Get document data from given document dict"""
        norm_link = doc_info.pop("norm_link")

        if is_historic:
            url = urljoin(self.base_url, norm_link)
        else:
            url = f"{urljoin(self.base_url, norm_link)}/ficha-tecnica?exibirAnotacao=1"
            url = f"{urljoin(self.base_url, norm_link)}/ficha-tecnica?exibirAnotacao=1"

        soup = await self.request_service.get_soup(url)
        if not soup:
            logger.error(f"Error getting soup for {url}")
            await self._save_doc_error(
                title=doc_info.get("title", ""),
                year=doc_info.get("year", ""),
                html_link=url,
                error_message="Failed to get document page",
            )
            return None

        # autor or autores
        author = soup.find("strong", text=re.compile(r"Autor:|Autores:"))
        if author:
            author = author.find_parent("li").text
            author = re.sub(r"Autor:|Autores:", "", author).strip()

        publication = soup.find("strong", text="Publicação:")
        if publication:
            publication = (
                publication.find_parent("li").text.replace("Publicação:", "").strip()
            )
        date = soup.find("strong", text="Data da promulgação:")
        if date:
            date = (
                date.find_parent("li").text.replace("Data da promulgação:", "").strip()
            )

        subject_regex = re.compile(r"Assunto:|Assuntos:")
        subject = soup.find("strong", text=subject_regex)
        if subject:
            subject = subject.find_parent("li").text
            subject = re.sub(subject_regex, "", subject).strip()

        tags = soup.find("strong", text="Tags:")
        if tags:
            tags = tags.find_parent("li").text.replace("Tags:", "").strip()
        situation = soup.find("strong", text="Situação:")
        if situation:
            situation = (
                situation.find_parent("li").text.replace("Situação:", "").strip()
            )

        pdf_content = await self.request_service.make_request(
            doc_info["document_url"]
        )  # need to make a request to get pdf content first, using directly _get_markdown will not work
        if not pdf_content:
            logger.error(f"Error getting pdf content for {doc_info['document_url']}")
            await self._save_doc_error(
                title=doc_info.get("title", ""),
                year=doc_info.get("year", ""),
                html_link=doc_info.get("document_url", url),
                error_message="Failed to download PDF content",
            )
            return

        text_markdown = await self._get_markdown(response=pdf_content)

        # text_markdown = self._get_markdown(doc_info["document_url"])
        # remove header with link at beginning of document
        # http://www.al.mt.gov.br/TNX/viewLegislacao.php?cod=44

        text_markdown = self.header_remove_regex.sub("", text_markdown).strip()

        # 150 comes from empirical results. Docs with less than this length are usually pdf images
        if (
            "Powered by TCPDF".lower() in text_markdown.lower()
            or len(text_markdown) < 150
        ):
            # probably pdf is an image

            pdf_content_response = await self.request_service.make_request(
                doc_info["document_url"]
            )
            if not pdf_content_response:
                await self._save_doc_error(
                    title=doc_info.get("title", ""),
                    year=doc_info.get("year", ""),
                    html_link=doc_info.get("document_url", url),
                    error_message="Failed to download PDF for image extraction",
                )
                return

            pdf_content = await pdf_content_response.read()

            text_markdown = (
                (await self._get_markdown(stream=BytesIO(pdf_content)))
                .replace("Powered by TCPDF (www.tcpdf.org)", "")
                .strip()
            )

        if not text_markdown:
            await self._save_doc_error(
                title=doc_info.get("title", ""),
                year=doc_info.get("year", ""),
                html_link=doc_info.get("document_url", url),
                error_message="Empty markdown after processing",
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
        }

        return doc_data

    async def _scrape_year(self, year: int) -> list[dict]:
        """Scrape norms for a specific year"""
        # Build list of all types (regular + historic)
        all_types = []
        for norm_type, norm_type_id in self.types.items():
            all_types.append(
                {
                    "id": norm_type_id,
                    "norm_type": norm_type,
                    "is_historic": False,
                }
            )

        for norm_type, norm_type_id in self.historic_types.items():
            all_types.append(
                {
                    "id": norm_type_id,
                    "norm_type": norm_type,
                    "is_historic": True,
                }
            )

        async def _scrape_type(norm_type_data: dict) -> list[dict]:
            """Scrape a single type (historic or regular) for the given year."""
            is_historic = norm_type_data["is_historic"]

            if is_historic and year > self.max_year_historic:
                return []
            elif not is_historic and year < self.min_year:
                return []

            if not self.token:
                await self._set_token()

            norm_type = norm_type_data["norm_type"]
            norm_type_id = norm_type_data["id"]
            url = self._build_search_url(norm_type_id, year, 1, is_historic)
            soup = await self.request_service.get_soup(url)

            # get total pages (always 10 records per page)
            total_items = self._get_total_norms(soup)
            if total_items == 0:
                return []

            pages = total_items // 10 + 1

            documents = []
            tasks = [
                self._get_docs_links(
                    self._build_search_url(norm_type_id, year, page, is_historic),
                    is_historic,
                )
                for page in range(1, pages + 1)
            ]
            valid_results = await self._gather_results(
                tasks,
                context={"year": year, "type": norm_type, "situation": "N/A"},
                desc=f"MATO GROSSO | {norm_type} | get_docs_links",
            )
            for result in valid_results:
                if result:
                    documents.extend(result)

            results = []
            tasks = [
                self._get_doc_data(doc_info, is_historic=is_historic)
                for doc_info in documents
            ]
            valid_results = await self._gather_results(
                tasks,
                context={"year": year, "type": norm_type, "situation": "N/A"},
                desc=f"MATO GROSSO | {norm_type}",
            )
            for norm in valid_results:
                queue_item = {
                    **norm,
                    "year": year,
                    "type": norm_type,
                    "situation": (
                        norm["situation"] if norm.get("situation") else "Não consta"
                    ),
                }
                results.append(queue_item)

            if self.verbose:
                logger.info(
                    f"Finished scraping for Year: {year} | Type: {norm_type} | Results: {len(results)}"
                )

            return results

        # Scrape all types concurrently
        tasks = [_scrape_type(type_data) for type_data in all_types]
        all_type_results = await asyncio.gather(*tasks, return_exceptions=True)

        # Flatten results and handle errors
        all_results = []
        for type_result in all_type_results:
            if isinstance(type_result, Exception):
                logger.error(f"Error scraping type for year {year}: {type_result}")
                continue
            all_results.extend(type_result)

        return all_results
