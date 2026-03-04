from typing import Optional
from urllib.parse import urljoin
from io import BytesIO

from loguru import logger
from tenacity import retry, stop_after_attempt, wait_exponential
from src.scraper.base.scraper import BaseScraper, STATE_LEGISLATION_SAVE_DIR


# We don't have situations for São Paulo, since the websitew only publishes valid documents (no invalid, no expired, no archived, no revoked, etc.)


VALID_SITUATIONS = {
    "Sem revogação expressa": 1,
}  # only norms with these situations (are actually valid norms)

INVALID_SITUATIONS = {
    "Declarada inconstitucional": 2,
    "Eficácia suspensa": 3,
    "Eficácia exaurida": 4,
    "Revogada": 5,
    "Anulada": 6,
}  # norms with these situations are invalid norms (no longer have legal effect)

# the reason to have invalid situations is in case we need to train a classifier to predict if a norm is valid or something else similar
SITUATIONS = {**VALID_SITUATIONS, **INVALID_SITUATIONS}

TYPES = {  # dict with norm type and its id
    "Decreto": 3,
    "Decreto Legislativo": 28,
    "Decreto-Lei": 25,
    "Decreto-Lei Complementar": 1,
    "Emenda Constitucional": 55,
    "Lei": 9,
    "Lei Complementar": 2,
    "Resolução": 14,
    "Resolução da Alesp": 19,
    "Decisão da Mesa": 12,
    "Ato da Mesa": 21,
    "Ato do Presidente": 22,
    "Decisão do Presidente": 23,
    "Constituição Estadual": 59,
}


class SaoPauloAlespScraper(BaseScraper):
    """Webscraper for Alesp (Assembleia Legislativa do Estado de São Paulo) website (https://www.al.sp.gov.br/)

    Example search request url: # https://www.al.sp.gov.br/norma/resultados?page=0&size=500&tipoPesquisa=E&buscaLivreEscape=&buscaLivreDecode=&_idsTipoNorma=1&idsTipoNorma=3&nuNorma=&ano=&complemento=&dtNormaInicio=&dtNormaFim=&idTipoSituacao=1&_idsTema=1&palavraChaveEscape=&palavraChaveDecode=&_idsAutorPropositura=1&_temQuestionamentos=on&_pesquisaAvancada=on
    """

    def __init__(
        self,
        base_url: str = "https://www.al.sp.gov.br/norma/resultados",
        max_workers: int = 16,  # low max_workers bacause alesp website often returns server error
        **kwargs,
    ):
        if STATE_LEGISLATION_SAVE_DIR:
            kwargs.setdefault("docs_save_dir", STATE_LEGISLATION_SAVE_DIR)
        super().__init__(
            base_url=base_url,
            types=TYPES,
            situations=SITUATIONS,
            max_workers=max_workers,
            name="SAO_PAULO",
            **kwargs,
        )
        self._page_size = 500
        self.headers = {
            "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 \
                (KHTML, like Gecko) Chrome/51.0.2704.103 Safari/537.36"
        }

    def _build_search_url(
        self, year: str, norm_type_id: int, norm_situation_id: int
    ) -> str:
        """Build search URL from arguments (no shared state mutation)."""
        params = {
            "size": self._page_size,
            "tipoPesquisa": "E",
            "buscaLivreEscape": "",
            "buscaLivreDecode": "",
            "_idsTipoNorma": 1,
            "idsTipoNorma": norm_type_id,
            "nuNorma": "",
            "ano": year,
            "complemento": "",
            "dtNormaInicio": "",
            "dtNormaFim": "",
            "idTipoSituacao": norm_situation_id,
            "_idsTema": 1,
            "palavraChaveEscape": "",
            "palavraChaveDecode": "",
            "_idsAutorPropositura": 1,
            "_temQuestionamentos": "on",
            "_pesquisaAvancada": "on",
        }
        return (
            self.base_url
            + "?"
            + "&".join([f"{key}={value}" for key, value in params.items()])
        )

    async def _get_docs_links(self, url: str) -> list:
        """Get documents html links from given page.
        Returns a list of dicts with keys 'title', 'summary', 'html_link'"""
        soup = await self.request_service.get_soup(url)

        if not soup:
            return []

        # Get all documents html links from page
        trs = soup.find_all("tr")
        docs_html_links = []
        for tr in trs:
            tds = tr.find_all("td")
            if len(tds) == 2:
                if "Mostrando".lower() in tds[0].text.strip().lower():
                    continue
                title = tds[0].find("span").text
                summary = tds[1].find("span").text
                # first <a> tag which contains the html link for the html document
                url = tds[0].find("a", href=True)["href"]
                norm_link = tds[0].find("a", class_="link_norma", href=True)
                norm_link = urljoin(
                    self.base_url.replace("/norma/resultados", ""), norm_link["href"]
                )
                html_link = urljoin(self.base_url.replace("/norma/resultados", ""), url)
                docs_html_links.append(
                    {
                        "title": title,
                        "summary": summary,
                        "html_link": html_link,
                        "norm_link": norm_link,
                    }
                )

        return docs_html_links

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=2, min=2, max=15),
        reraise=True,
    )
    async def _get_norm_data(self, norm_link: str) -> dict:
        """Get norm data from given norm link"""

        soup = await self.request_service.get_soup(norm_link)

        if not soup:
            return {}

        # get "promulgacao", "projeto", "temas", "palavras-chave" if they exist
        promulgacao = soup.find("label", text="Promulgação")
        if promulgacao:
            promulgacao = promulgacao.find_next("label").text
            if not promulgacao:
                promulgacao = ""

        projeto = soup.find("label", text="Projeto")
        if projeto:
            projeto = projeto.find_next("label").text
            if not projeto:
                projeto = ""

        temas = soup.find("label", text="Temas")
        if temas:
            temas = [
                button.text for button in temas.find_next("div").find_all("button")
            ]

        palavras_chave = soup.find("label", text="Palavras-chave")
        if palavras_chave:
            palavras_chave = [
                a.text for a in palavras_chave.find_next("div").find_all("a")
            ]

        return {
            "promulgation": promulgacao,
            "project": projeto,
            "themes": temas,
            "keywords": palavras_chave,
        }

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=2, min=2, max=15),
        reraise=True,
    )
    async def _get_doc_data(self, doc_info: dict, norm_type: str) -> Optional[dict]:
        """Get document data from given html link"""
        doc_html_link = doc_info["html_link"]

        # get norm data
        norm_link = doc_info["norm_link"]
        try:
            norm_data = await self._get_norm_data(norm_link)
        except Exception as e:
            logger.error(f"Failed to fetch norm data after retries: {e}")
            norm_data = {}

        data = {
            "title": doc_info["title"],
            "summary": doc_info["summary"],
            "html_string": "",
            "text_markdown": "",
            "document_url": doc_html_link,
            **norm_data,
        }

        # check if pdf
        if doc_html_link.endswith(".pdf"):
            text_markdown = await self._get_markdown(url=doc_html_link)

            # check if got html content
            if "<html>" in text_markdown or "<!DOCTYPE html>" in text_markdown:
                if self.verbose:
                    logger.info(f"Got HTML content for PDF: {doc_html_link}")

                # Use direct HTML content conversion
                text_markdown = await self._get_markdown(html_content=text_markdown)

            if not text_markdown or not text_markdown.strip():
                logger.error(f"Failed to get markdown for PDF: {doc_html_link}")
                await self._save_doc_error(
                    title=doc_info["title"],
                    year="",
                    situation="",
                    norm_type=norm_type,
                    html_link=doc_html_link,
                    error_message="Failed to extract markdown from PDF",
                )
                return None

            data["text_markdown"] = text_markdown
            return data

        soup = await self.request_service.get_soup(doc_html_link)
        if not soup or not soup.body:
            raise Exception(f"Failed to get valid soup for {doc_html_link}")

        # check if pdf embedded in iframe
        panel_div = soup.find("div", id="UpdatePanel1")
        if panel_div:
            iframe = panel_div.find("iframe", src=True)
            pdf_link = iframe["src"]
            pdf_link = urljoin(doc_html_link, pdf_link)
            if self.verbose:
                logger.info(f"Found PDF link in iframe: {pdf_link}")
            pdf_response = await self.request_service.make_request(pdf_link)
            if pdf_response is None:
                raise Exception(f"No response downloading iframe PDF: {pdf_link}")
            pdf_content = await pdf_response.read()
            text_markdown = await self._get_markdown(stream=BytesIO(pdf_content))
            if not text_markdown or not text_markdown.strip():
                logger.error(f"Failed to get markdown for PDF: {pdf_link}")
                await self._save_doc_error(
                    title=doc_info["title"],
                    year="",
                    situation="",
                    norm_type=norm_type,
                    html_link=pdf_link,
                    error_message="Failed to extract markdown from iframe PDF",
                )
                return None

            data["text_markdown"] = text_markdown
            return data

        # remove a tags with 'Assembleia Legislativa do Estado de São Paulo' and 'Ficha informativa'
        for a in soup.find_all("a"):
            if a.decomposed:
                continue

            a_text = a.text.lower()
            a_href = a.get("href", "").lower()
            if (
                "Assembleia Legislativa do Estado de São Paulo".lower() in a_text
                or "Ficha informativa".lower() in a_text
                or "http://www.al.sp.gov.br".lower() in a_href
                or "https://www.al.sp.gov.br".lower() in a_href
            ):
                a.decompose()

        # get data
        if soup.body:
            html_string = soup.body.prettify(formatter="html")
            html_string = "<html>" + html_string + "</html>"
        else:
            html_string = soup.prettify(formatter="html")
            if "<html>" not in html_string:
                html_string = "<html><body>" + html_string + "</body></html>"

        # get text markdown
        text_markdown = await self._get_markdown(html_content=html_string)

        # <p><img src="decisao.da.mesa-1311-img1-02.05.2005.jpg"></p>
        # For some Decisão da Mesa norms, it will have the content as image, so we need to get that and append to the markdown
        if "Decisão da Mesa".lower() in norm_type.lower():
            img = soup.find("img")
            if img:
                img_url = img.get("src")
                if self.verbose:
                    logger.info(
                        f"Getting image for Decisão da Mesa: {doc_html_link} | img source: {img_url}"
                    )
                img_url = urljoin(doc_html_link, img_url)
                img_response = await self.request_service.make_request(img_url)
                if img_response is None:
                    logger.error(f"No response downloading image: {img_url}")
                else:
                    buffer = BytesIO()
                    buffer.write(await img_response.read())
                    buffer.seek(0)

                    img_markdown = await self._get_markdown(stream=buffer)
                    if img_markdown and img_markdown.strip():
                        text_markdown += "\n\n" + img_markdown
                    else:
                        logger.error(f"Failed to get markdown for image: {img_url}")

        return {
            "title": doc_info["title"],
            "summary": doc_info["summary"],
            "html_string": html_string,
            "text_markdown": text_markdown,
            "document_url": doc_html_link,
            **norm_data,
        }

    async def _scrape_situation_type(
        self,
        year: str,
        situation: str,
        situation_id: int,
        norm_type: str,
        norm_type_id: int,
    ) -> list[dict]:
        """Scrape norms for a specific situation and type"""
        url = self._build_search_url(year, norm_type_id, situation_id)
        soup = await self.request_service.get_soup(url)

        if not soup:
            return []

        # check if <div class="card cinza text-center">Nenhuma norma encontrada como os parâmetros informados</div> exists
        if (
            "Nenhuma norma encontrada como os parâmetros informados".lower()
            in soup.text.lower()
        ):
            return []

        # get number of pages
        total = soup.find("span", text="página")
        if total is None:
            total = soup.find("span", text="páginas")

        if not total:
            return []

        total = total.previous_sibling.previous_sibling.text
        total = int(total.strip().split()[-1])

        if total == 0:
            if self.verbose:
                logger.info(
                    f"No results for {norm_type} in {year} with situation {situation}"
                )
            return []

        pages = total // self._page_size + 1

        # Get documents html links from all pages
        documents_html_links = []
        tasks = [self._get_docs_links(url + f"&page={page}") for page in range(pages)]
        valid_results = await self._gather_results(
            tasks,
            context={"year": year, "type": norm_type, "situation": situation},
            desc=f"SAO PAULO | {norm_type} | get_docs_links",
        )
        for result in valid_results:
            documents_html_links.extend(result)

        # Get data from all documents text links
        results = []
        tasks = [
            self._get_doc_data(doc_html_link, norm_type)
            for doc_html_link in documents_html_links
        ]
        valid_results = await self._gather_results(
            tasks,
            context={"year": year, "type": norm_type, "situation": situation},
            desc=f"SAO PAULO | {norm_type}",
        )
        for result in valid_results:
            queue_item = {
                "year": year,
                "situation": situation,
                "type": norm_type,
                **result,
            }
            results.append(queue_item)

        if self.verbose:
            logger.info(
                f"Finished scraping for Year: {year} | Situation: {situation} | Type: {norm_type} | Results: {len(results)}"
            )

        return results

    async def _scrape_year(self, year: str) -> list[dict]:
        """Scrape norms for a specific year"""
        tasks = [
            self._scrape_situation_type(
                year, situation, situation_id, norm_type, norm_type_id
            )
            for situation, situation_id in self.situations.items()
            for norm_type, norm_type_id in self.types.items()
        ]
        valid = await self._gather_results(
            tasks,
            context={"year": year, "type": "N/A", "situation": "N/A"},
            desc=f"{self.name} | Year {year}",
        )
        return [
            item
            for result in valid
            for item in (result if isinstance(result, list) else [result])
        ]
