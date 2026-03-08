import re
from io import BytesIO
from urllib.parse import urlencode, urljoin

from loguru import logger
from tenacity import retry, stop_after_attempt, wait_exponential

from src.scraper.base.scraper import StateScraper


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

SITUATIONS = {**VALID_SITUATIONS, **INVALID_SITUATIONS}

TYPES = {  # dict with norm type and its id (kept as reference metadata)
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


class SaoPauloAlespScraper(StateScraper):
    """Webscraper for Alesp (Assembleia Legislativa do Estado de São Paulo) website (https://www.al.sp.gov.br/)

    Year start (earliest on source): 1835

    Example search request url: # https://www.al.sp.gov.br/norma/resultados?page=0&size=500&tipoPesquisa=E&buscaLivreEscape=&buscaLivreDecode=&_idsTipoNorma=1&nuNorma=&ano=2020&complemento=&dtNormaInicio=&dtNormaFim=&idTipoSituacao=0&_idsTema=1&palavraChaveEscape=&palavraChaveDecode=&_idsAutorPropositura=1&_temQuestionamentos=on&_pesquisaAvancada=on
    """

    def __init__(
        self,
        base_url: str = "https://www.al.sp.gov.br/norma/resultados",
        max_workers: int = 16,  # low max_workers bacause alesp website often returns server error
        **kwargs,
    ):
        super().__init__(
            base_url=base_url,
            types=TYPES,
            situations=SITUATIONS,
            max_workers=max_workers,
            name="SAO_PAULO",
            **kwargs,
        )
        self._page_size = 500
        self._site_base = "https://www.al.sp.gov.br"
        self.headers = {
            "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 \
                (KHTML, like Gecko) Chrome/51.0.2704.103 Safari/537.36"
        }

    def _build_search_url(self, year: int, page: int = 0) -> str:
        """Build search URL for all types/situations (idTipoSituacao=0)."""
        params = {
            "page": page,
            "size": self._page_size,
            "tipoPesquisa": "E",
            "buscaLivreEscape": "",
            "buscaLivreDecode": "",
            "_idsTipoNorma": 1,
            "nuNorma": "",
            "ano": year,
            "complemento": "",
            "dtNormaInicio": "",
            "dtNormaFim": "",
            "idTipoSituacao": 0,
            "_idsTema": 1,
            "palavraChaveEscape": "",
            "palavraChaveDecode": "",
            "_idsAutorPropositura": 1,
            "_temQuestionamentos": "on",
            "_pesquisaAvancada": "on",
        }
        return f"{self.base_url}?{urlencode(params)}"

    @staticmethod
    def _extract_result_counts(soup) -> tuple[int, int] | None:
        """Extract total result and page counts from the results header."""
        for bold_tag in soup.find_all("b"):
            text = " ".join(bold_tag.stripped_strings)
            match = re.search(r"Resultado:\s*(\d+)\s+normas\s+em\s+(\d+)", text)
            if match:
                return int(match.group(1)), int(match.group(2))
        return None

    def _parse_docs_from_soup(self, soup) -> list[dict]:
        """Extract doc entries from a pre-fetched result page soup."""
        trs = soup.find_all("tr")
        docs_html_links = []
        for tr in trs:
            tds = tr.find_all("td")
            if len(tds) == 2:
                if "Mostrando".lower() in tds[0].text.strip().lower():
                    continue
                title = tds[0].find("span").text
                summary = tds[1].find("span").text
                doc_href = tds[0].find("a", href=True)["href"]
                norm_link_tag = tds[0].find("a", class_="link_norma", href=True)
                norm_link = urljoin(self._site_base, norm_link_tag["href"])
                html_link = urljoin(self._site_base, doc_href)
                docs_html_links.append(
                    {
                        "title": title,
                        "summary": summary,
                        "html_link": html_link,
                        "norm_link": norm_link,
                    }
                )
        return docs_html_links

    async def _get_docs_links(self, url: str) -> list:
        """Get documents html links from given page.
        Returns a list of dicts with keys 'title', 'summary', 'html_link', 'norm_link'"""
        soup = await self.request_service.get_soup(url)
        if not soup:
            return []
        return self._parse_docs_from_soup(soup)

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

        # get "promulgacao", "projeto", "temas", "palavras-chave", "situacao" if they exist
        promulgacao = soup.find("label", string="Promulgação")
        if promulgacao:
            promulgacao = promulgacao.find_next("label").text
            if not promulgacao:
                promulgacao = ""

        projeto = soup.find("label", string="Projeto")
        if projeto:
            projeto = projeto.find_next("label").text
            if not projeto:
                projeto = ""

        temas = soup.find("label", string="Temas")
        if temas:
            temas = [
                button.text for button in temas.find_next("div").find_all("button")
            ]

        palavras_chave = soup.find("label", string="Palavras-chave")
        if palavras_chave:
            palavras_chave = [
                a.text for a in palavras_chave.find_next("div").find_all("a")
            ]

        situacao = soup.find("label", string="Situação")
        if situacao:
            situacao = situacao.find_next("label").text or ""

        return {
            "promulgation": promulgacao,
            "project": projeto,
            "themes": temas,
            "keywords": palavras_chave,
            "situation": situacao,
        }

    @staticmethod
    def _infer_type(title: str) -> str:
        """Infer norm type from title (longest match first to avoid prefix collisions)."""
        for type_name in sorted(TYPES, key=len, reverse=True):
            if title.lower().startswith(type_name.lower()):
                return type_name
        return "Legislação"

    async def _get_doc_data(
        self, doc_info: dict, norm_type: str = "NA", year: str = ""
    ) -> dict | None:
        """Get document data from given html link"""
        doc_html_link = doc_info["html_link"]
        title = doc_info["title"]

        if self._is_already_scraped(doc_html_link, title):
            return None

        norm_type = self._infer_type(title)

        # get norm data
        norm_link = doc_info["norm_link"]
        try:
            norm_data = await self._get_norm_data(norm_link)
        except Exception as e:
            logger.error(f"Failed to fetch norm data after retries: {e}")
            norm_data = {}

        situation = norm_data.pop("situation", "Não consta") or "Não consta"

        data = {
            "year": year,
            "title": title,
            "type": norm_type,
            "summary": doc_info["summary"],
            "text_markdown": "",
            "document_url": doc_html_link,
            "situation": situation,
            **norm_data,
        }

        # check if pdf
        if doc_html_link.endswith(".pdf"):
            text_markdown, raw_content, content_ext = await self._download_and_convert(
                doc_html_link
            )

            # check if got html content
            if text_markdown and (
                "<html>" in text_markdown or "<!DOCTYPE html>" in text_markdown
            ):
                if self.verbose:
                    logger.info(f"Got HTML content for PDF: {doc_html_link}")

                # Use direct HTML content conversion
                raw_content = text_markdown.encode("utf-8")
                content_ext = ".html"
                text_markdown = await self._get_markdown(html_content=text_markdown)

            valid, reason = self._valid_markdown(text_markdown)
            if not valid:
                logger.error(f"Failed to get markdown for PDF: {doc_html_link}")
                await self._save_doc_error(
                    title=title,
                    norm_type=norm_type,
                    html_link=doc_html_link,
                    error_message=f"Failed to extract markdown from PDF: {reason}",
                )
                return None

            data["text_markdown"] = text_markdown
            data["_raw_content"] = raw_content
            data["_content_extension"] = content_ext

            return data

        soup = await self.request_service.get_soup(doc_html_link)
        if not soup or not soup.body:
            await self._save_doc_error(
                title=title,
                norm_type=norm_type,
                html_link=doc_html_link,
                error_message=f"Failed to get valid soup for {doc_html_link}",
            )
            return None

        # check if pdf embedded in iframe
        panel_div = soup.find("div", id="UpdatePanel1")
        if panel_div:
            iframe = panel_div.find("iframe", src=True)
            if iframe:
                pdf_link = iframe["src"]
                pdf_link = urljoin(doc_html_link, pdf_link)
                if self.verbose:
                    logger.info(f"Found PDF link in iframe: {pdf_link}")
                pdf_response = await self.request_service.make_request(pdf_link)
                if not pdf_response:
                    await self._save_doc_error(
                        title=title,
                        norm_type=norm_type,
                        html_link=pdf_link,
                        error_message=f"No response downloading iframe PDF: {pdf_link}",
                    )
                    return None
                pdf_content = await pdf_response.read()
                text_markdown = await self._get_markdown(stream=BytesIO(pdf_content))
                valid, reason = self._valid_markdown(text_markdown)
                if not valid:
                    logger.error(f"Failed to get markdown for PDF: {pdf_link}")
                    await self._save_doc_error(
                        title=title,
                        norm_type=norm_type,
                        html_link=pdf_link,
                        error_message=f"Failed to extract markdown from iframe PDF: {reason}",
                    )
                    return None

                data["text_markdown"] = text_markdown
                data["_raw_content"] = pdf_content
                data["_content_extension"] = ".pdf"

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
                html_string = self._wrap_html(html_string)

        # get text markdown
        text_markdown = await self._get_markdown(html_content=html_string)
        raw_content = html_string.encode("utf-8")
        content_ext = ".html"

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
                if not img_response:
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

        valid, reason = self._valid_markdown(text_markdown)
        if not valid:
            await self._save_doc_error(
                title=title,
                norm_type=norm_type,
                html_link=doc_html_link,
                error_message=f"Invalid markdown from HTML: {reason}",
            )
            return None

        data["text_markdown"] = text_markdown
        data["_raw_content"] = raw_content
        data["_content_extension"] = content_ext

        return data

    async def _scrape_year(self, year: int) -> list[dict]:
        """Fetch all norms for a year in a single paginated query (idTipoSituacao=0)."""
        url_page0 = self._build_search_url(year, page=0)
        soup = await self.request_service.get_soup(url_page0)
        if not soup:
            return []
        if "Nenhuma norma encontrada".lower() in soup.text.lower():
            return []

        counts = self._extract_result_counts(soup)
        if not counts or counts[0] == 0:
            return []
        _, pages = counts

        # Reuse already-fetched page-0 soup (avoids double fetch)
        documents = self._parse_docs_from_soup(soup)
        ctx = {"year": year, "type": "NA", "situation": "NA"}
        documents.extend(
            await self._fetch_all_pages(
                lambda p: self._get_docs_links(self._build_search_url(year, page=p)),
                pages - 1,
                start_page=1,
                context=ctx,
                desc=f"SAO_PAULO | {year} | get_docs_links",
            )
        )

        return await self._process_documents(
            documents,
            year=year,
            norm_type="NA",
            situation="NA",
            doc_data_kwargs={"year": year},
        )
