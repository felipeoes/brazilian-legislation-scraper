import re
from collections.abc import AsyncIterator
from io import BytesIO
from typing import Any
from urllib.parse import urlencode, urljoin

from bs4 import BeautifulSoup
from loguru import logger

from src.core.convert import (
    bytes_to_markdown,
    infer_type_by_prefix,
    valid_markdown,
    wrap_html,
)
from src.core.document import Document
from src.core.scraper import DEFAULT_VALID_SITUATION, BaseScraper

VALID_SITUATIONS = {"Sem revogação expressa": 1}
INVALID_SITUATIONS = {
    "Declarada inconstitucional": 2,
    "Eficácia suspensa": 3,
    "Eficácia exaurida": 4,
    "Revogada": 5,
    "Anulada": 6,
}
SITUATIONS = {**VALID_SITUATIONS, **INVALID_SITUATIONS}
TYPES = {
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
_TYPE_ALIASES = {
    "Constituição do Estado": "Constituição Estadual",
    "Constituição Estadual": "Constituição Estadual",
}


class SaoPauloAlespScraper(BaseScraper):
    """Webscraper for Alesp (Assembleia Legislativa do Estado de São Paulo) website (https://www.al.sp.gov.br/)

    Year start (earliest on source): 1835

    Example search request url: # https://www.al.sp.gov.br/norma/resultados?page=0&size=500&tipoPesquisa=E&buscaLivreEscape=&buscaLivreDecode=&_idsTipoNorma=1&nuNorma=&ano=2020&complemento=&dtNormaInicio=&dtNormaFim=&idTipoSituacao=0&_idsTema=1&palavraChaveEscape=&palavraChaveDecode=&_idsAutorPropositura=1&_temQuestionamentos=on&_pesquisaAvancada=on
    """

    types = TYPES
    situations = SITUATIONS
    name = "SAO_PAULO"
    base_url = "https://www.al.sp.gov.br/norma/resultados"

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self._page_size = 500
        self._site_base = "https://www.al.sp.gov.br"
        self.headers = {
            "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36                 (KHTML, like Gecko) Chrome/51.0.2704.103 Safari/537.36"
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
    def _extract_result_counts(soup: BeautifulSoup) -> tuple[int, int] | None:
        """Extract total result and page counts from the results header."""
        for bold_tag in soup.find_all("b"):
            text = " ".join(bold_tag.stripped_strings)
            match = re.search("Resultado:\\s*(\\d+)\\s+normas\\s+em\\s+(\\d+)", text)
            if match:
                return (int(match.group(1)), int(match.group(2)))
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
        soup = (await self.http.request(url)).soup()
        if not soup:
            return []
        return self._parse_docs_from_soup(soup)

    async def _get_norm_data(self, norm_link: str) -> dict:
        """Get norm data from given norm link"""
        soup = (await self.http.request(norm_link)).soup()
        if not soup:
            return {}
        promulgacao = soup.find("label", string="Promulgação")
        if promulgacao:
            next_label = promulgacao.find_next("label")
            promulgacao = next_label.text if next_label else ""
        projeto = soup.find("label", string="Projeto")
        if projeto:
            next_label = projeto.find_next("label")
            projeto = next_label.text if next_label else ""
        temas = soup.find("label", string="Temas")
        if temas:
            temas_div = temas.find_next("div")
            temas = (
                [button.text for button in temas_div.find_all("button")]
                if temas_div
                else []
            )
        palavras_chave = soup.find("label", string="Palavras-chave")
        if palavras_chave:
            kw_div = palavras_chave.find_next("div")
            palavras_chave = [a.text for a in kw_div.find_all("a")] if kw_div else []
        situacao = soup.find("label", string="Situação")
        if situacao:
            next_label = situacao.find_next("label")
            situacao = next_label.text if next_label else ""
        return {
            "promulgation": promulgacao,
            "project": projeto,
            "themes": temas,
            "keywords": palavras_chave,
            "situation": situacao,
        }

    @staticmethod
    def _infer_type(title: str) -> str:
        """Infer norm type from the title prefix exposed by the source site."""
        normalized_title = " ".join(title.split())
        inferred = infer_type_by_prefix(title, _TYPE_ALIASES) or infer_type_by_prefix(
            title, list(TYPES)
        )
        if inferred:
            return inferred
        match = re.match(
            "(.+?)(?:\\s+(?:n[º°o]|n\\.|número)\\s*\\d|\\s+\\d)",
            normalized_title,
            re.IGNORECASE,
        )
        if match:
            return match.group(1).strip()
        return normalized_title

    async def fetch(self, doc_info: dict) -> Document | None:
        year = doc_info["year"]
        doc_html_link = doc_info["html_link"]
        title = doc_info["title"]
        norm_type = self._infer_type(title)
        norm_link = doc_info["norm_link"]
        try:
            norm_data = await self._get_norm_data(norm_link)
        except Exception as e:
            logger.warning(f"Failed to fetch norm data for {norm_link}: {e}")
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
        if doc_html_link.endswith(".pdf"):
            text_markdown, raw, content_ext = await self.download(doc_html_link)
            valid, reason = valid_markdown(text_markdown)
            if not valid:
                logger.error(f"Failed to get markdown for PDF: {doc_html_link}")
                raise ValueError(f"Failed to extract markdown from PDF: {reason}")
            data["text_markdown"] = text_markdown
            data["raw"] = raw
            data["raw_ext"] = content_ext
            from src.core.document import Document

            return Document(**data)
        try:
            soup, mhtml = await self.browser.snapshot(doc_html_link)
        except Exception as exc:
            raise ValueError(f"Failed to fetch page: {exc}")
        if not soup.body:
            raise ValueError(f"Failed to get valid soup for {doc_html_link}")
        panel_div = soup.find("div", id="UpdatePanel1")
        if panel_div:
            iframe = panel_div.find("iframe", src=True)
            if iframe:
                pdf_link = iframe["src"]
                pdf_link = urljoin(doc_html_link, pdf_link)
                logger.debug(f"Found PDF link in iframe: {pdf_link}")
                pdf_response = await self.http.request(pdf_link)
                pdf_content = pdf_response.content
                text_markdown = await bytes_to_markdown(
                    pdf_content, "", self.base_url, self.http, self.ocr
                )
                valid, reason = valid_markdown(text_markdown)
                if not valid:
                    logger.error(f"Failed to get markdown for PDF: {pdf_link}")
                    raise ValueError(
                        f"Failed to extract markdown from iframe PDF: {reason}"
                    )
                data["text_markdown"] = text_markdown
                data["raw"] = pdf_content
                data["raw_ext"] = ".pdf"
                from src.core.document import Document

                return Document(**data)
        for a in soup.find_all("a"):
            if a.decomposed:
                continue
            a_text = a.text.lower()
            a_href = a.get("href", "").lower()
            if (
                "Assembleia Legislativa do Estado de São Paulo".lower() in a_text
                or "Ficha informativa".lower() in a_text
                or "http://www.al.sp.gov.br".lower() in a_href
                or ("https://www.al.sp.gov.br".lower() in a_href)
            ):
                a.decompose()
        if soup.body:
            html_string = soup.body.prettify(formatter="html")
            html_string = "<html>" + html_string + "</html>"
        else:
            html_string = soup.prettify(formatter="html")
            if "<html>" not in html_string:
                html_string = wrap_html(html_string)
        text_markdown = await self.markdown(html=html_string)
        raw = mhtml
        content_ext = ".mhtml"
        if "Decisão da Mesa".lower() in norm_type.lower():
            img = soup.find("img")
            if img:
                img_url = img.get("src")
                logger.debug(
                    f"Getting image for Decisão da Mesa: {doc_html_link} | img source: {img_url}"
                )
                img_url = urljoin(doc_html_link, img_url)
                img_response = await self.http.request(img_url)
                buffer = BytesIO()
                buffer.write(img_response.content)
                buffer.seek(0)
                img_markdown = await bytes_to_markdown(
                    buffer.getvalue(), "", self.base_url, self.http, self.ocr
                )
                if img_markdown and img_markdown.strip():
                    text_markdown += "\n\n" + img_markdown
                else:
                    logger.error(f"Failed to get markdown for image: {img_url}")
        valid, reason = valid_markdown(text_markdown)
        if not valid:
            raise ValueError(f"Invalid markdown from HTML: {reason}")
        data["text_markdown"] = text_markdown
        data["raw"] = raw
        data["raw_ext"] = content_ext
        from src.core.document import Document

        return Document(**data)

    async def list_documents(self, year: int) -> AsyncIterator[dict]:
        soup = (await self.http.request(self._build_search_url(year, page=0))).soup()
        if "nenhuma norma encontrada" in soup.text.lower():
            return
        counts = self._extract_result_counts(soup)
        if counts is None:
            raise ValueError("Could not parse São Paulo result count")
        _, pages = counts

        async def fetch_page(page: int) -> list[dict]:
            return (
                self._parse_docs_from_soup(soup)
                if page == 0
                else await self._get_docs_links(self._build_search_url(year, page=page))
            )

        async for ref in self.paginate(fetch_page, pages - 1, start=0):
            yield {
                "year": year,
                "situation": DEFAULT_VALID_SITUATION,
                **ref,
                "document_url": urljoin(self.base_url, ref["html_link"]),
            }
