import re
from collections.abc import AsyncIterator
from urllib.parse import urlencode, urljoin

from bs4 import BeautifulSoup
from bs4.element import NavigableString, Tag
from loguru import logger

from src.core.convert import valid_markdown, wrap_html
from src.core.document import Document
from src.core.scraper import DEFAULT_VALID_SITUATION, BaseScraper

TYPES = [
    "Ato das Disposições Constitucionais Transitórias",
    "Constituição Estadual",
    "Decisão",
    "Decreto",
    "Decreto-Lei",
    "Deliberação",
    "Emenda Constitucional",
    "Instrução Normativa",
    "Lei",
    "Lei Complementar",
    "Lei Constitucional",
    "Lei Delegada",
    "Ordem de Serviço",
    "Portaria",
    "Resolução",
]
SITUATIONS = {
    DEFAULT_VALID_SITUATION: DEFAULT_VALID_SITUATION,
    "Revogada": "Revogada",
    "Declarada inconstitucional": "Declarada inconstitucional",
    "Tornada sem efeito": "Tornada sem efeito",
}
_PAGE_SIZE = 10
_COUNT_PATTERN = re.compile(
    "(\\d[\\d\\.,]*)\\s*artigo(?:s)?\\s*encontrado(?:s)?", re.IGNORECASE
)
_TYPE_CODE_MAP = {
    "ADT": "Ato das Disposições Constitucionais Transitórias",
    "CON": "Constituição Estadual",
    "DCS": "Decisão",
    "DEC": "Decreto",
    "DNE": "Decreto",
    "DSN": "Decreto",
    "DEL": "Decreto-Lei",
    "DLB": "Deliberação",
    "EMC": "Emenda Constitucional",
    "IDG": "Instrução Normativa",
    "LEI": "Lei",
    "LCP": "Lei Complementar",
    "LDL": "Lei Delegada",
    "OSV": "Ordem de Serviço",
    "PRT": "Portaria",
    "RAL": "Resolução",
}
_TYPE_PATTERNS = [
    (
        re.compile("^ato das disposições constitucionais transitórias\\b", re.I),
        TYPES[0],
    ),
    (re.compile("^constituição\\b", re.I), TYPES[1]),
    (re.compile("^decisão\\b", re.I), TYPES[2]),
    (re.compile("^decreto(?: com numeração especial| sem número)?\\b", re.I), TYPES[3]),
    (re.compile("^decreto-lei\\b", re.I), TYPES[4]),
    (re.compile("^deliberação\\b", re.I), TYPES[5]),
    (re.compile("^emenda à constituição\\b", re.I), TYPES[6]),
    (re.compile("^instrução normativa\\b", re.I), TYPES[7]),
    (re.compile("^lei complementar\\b", re.I), TYPES[9]),
    (re.compile("^lei constitucional\\b", re.I), TYPES[10]),
    (re.compile("^lei delegada\\b", re.I), TYPES[11]),
    (re.compile("^lei\\b", re.I), TYPES[8]),
    (re.compile("^ordem de serviço\\b", re.I), TYPES[12]),
    (re.compile("^portaria\\b", re.I), TYPES[13]),
    (re.compile("^resolução\\b", re.I), TYPES[14]),
]
_TITLE_SITUATION_PATTERNS = [
    (re.compile("\\(revogada\\)$", re.I), "Revogada"),
    (
        re.compile("\\(declarada inconstitucional\\)$", re.I),
        "Declarada inconstitucional",
    ),
    (re.compile("\\(tornada sem efeito\\)$", re.I), "Tornada sem efeito"),
]
_TEXT_PAGE_ATTEMPTS = 4
_TEXT_PAGE_RETRY_SLEEP_SECONDS = 1.0
_PDF_OBSERVATION_PATTERN = re.compile(
    "(?:observa(?:c|ç)(?:a|ã)o:?\\s*)?a imagem d[ao]\\s+.+?est[aá]\\s+dispon[ií]vel em:?",
    re.IGNORECASE,
)
_SUBSTANTIVE_TEXT_PATTERN = re.compile(
    "\\b(art\\.\\s*\\d+|decreta|resolve(?:m)?|delibera(?:m)?|decide(?:m)?|promulgo|promulga|sanciona|cap[ií]tulo|par[aá]grafo|belo horizonte|pal[aá]cio da inconfid[eê]ncia)\\b",
    re.IGNORECASE,
)
_ALMG_SHARE_URL_PATTERN = re.compile(
    "https?://www\\.alm[ag]\\.gov\\.br/.*?(?:utm[\\s_]*source|utm_medium|utm_campaign|BtnCompartilhar|Compartilhar|WhatsApp).*?(?=(?:\\)|</p>|<br\\s*/?>|$))",
    re.IGNORECASE | re.DOTALL,
)
_TIMESTAMPED_SHARE_LINK_PATTERN = re.compile(
    "(\\(\\s*trecho\\s+a\\s+partir\\s+de\\s+\\d{1,2}:\\d{2}:\\d{2}s?)\\s*https?://.*?(\\)\\s*:?)",
    re.IGNORECASE | re.DOTALL,
)


class MGAlmgScraper(BaseScraper):
    """Webscraper for Minas Gerais state legislation website (https://www.almg.gov.br)

    Year start (earliest on source): 1831

    Example search request: https://www.almg.gov.br/atividade-parlamentar/leis/legislacao-mineira?pagina=2&aba=pesquisa&q=&ano=1989&dataFim=&num=&ordem=0&pesquisou=true&dataInicio=
    """

    types = TYPES
    situations = SITUATIONS
    name = "MINAS_GERAIS"
    base_url = "https://www.almg.gov.br"

    @staticmethod
    def _clean_text(value: str) -> str:
        return re.sub("\\s+", " ", value.replace("\xa0", " ")).strip()

    def _find_label(self, soup: BeautifulSoup, *labels: str) -> Tag | None:
        wanted = {self._clean_text(label) for label in labels}
        for tag in soup.find_all("span"):
            if self._clean_text(tag.get_text(" ", strip=True)) in wanted:
                return tag
        return None

    def _build_search_url(self, year: int, page: int = 1) -> str:
        """Build year-only search URL from arguments."""
        params = {
            "pagina": str(page),
            "aba": "pesquisa",
            "q": "",
            "ano": str(year),
            "dataFim": "",
            "num": "",
            "ordem": "0",
            "pesquisou": "true",
            "dataInicio": "",
        }
        return f"{self.base_url}/atividade-parlamentar/leis/legislacao-mineira?{urlencode(params)}"

    def _normalize_type(self, title: str, html_link: str = "") -> str:
        clean_title = self._clean_text(title)
        code_match = re.search("/legislacao-mineira/([A-Z]{3})/", html_link)
        if code_match:
            canonical_type = _TYPE_CODE_MAP.get(code_match.group(1))
            if canonical_type:
                return canonical_type
        for pattern, canonical_type in _TYPE_PATTERNS:
            if pattern.match(clean_title):
                return canonical_type
        prefix_match = re.match(
            "(.+?)(?:\\s+(?:n[º°o]|n\\.|número)\\s*\\d|\\s+\\d)",
            clean_title,
            re.IGNORECASE,
        )
        if prefix_match:
            return prefix_match.group(1).strip()
        logger.warning(
            f"Could not normalize Minas Gerais norm type from title='{title}' link='{html_link}'"
        )
        return "Unknown"

    def _extract_total_pages(self, soup: BeautifulSoup) -> int:
        """Extract total result pages from listing page count or pagination links."""
        count_node = soup.find(
            string=re.compile("artigo(?:s)?\\s*encontrado(?:s)?", re.IGNORECASE)
        )
        if count_node:
            match = _COUNT_PATTERN.search(self._clean_text(str(count_node)))
            if match:
                count = int(re.sub("\\D", "", match.group(1)))
                if count == 0:
                    return 0
                return (count + _PAGE_SIZE - 1) // _PAGE_SIZE
        page_numbers = []
        for anchor in soup.select(".pagination a"):
            label = self._clean_text(anchor.get_text(" ", strip=True))
            if label.isdigit():
                page_numbers.append(int(label))
        if page_numbers:
            return max(page_numbers)
        return 1 if soup.find_all("article") else 0

    async def _get_docs_links(
        self, url: str, soup: BeautifulSoup | None = None
    ) -> list[dict]:
        """Get documents from a year listing page."""
        listing_soup = soup or (await self.http.request(url)).soup()
        if not listing_soup:
            return []
        docs = []
        for item in listing_soup.find_all("article"):
            link = item.find("a", href=True)
            if not link:
                continue
            title = self._clean_text(link.get_text(" ", strip=True))
            full_text = self._clean_text(item.get_text(" ", strip=True))
            summary = full_text.removeprefix(title).strip()
            href = str(link.get("href", ""))
            docs.append(
                {
                    "title": title,
                    "summary": summary,
                    "html_link": href,
                    "type": self._normalize_type(title, href),
                }
            )
        return docs

    def _extract_labeled_text(self, label_node: Tag | None) -> str:
        if label_node is None:
            return ""
        parts: list[str] = []
        for sibling in label_node.next_siblings:
            if isinstance(sibling, Tag):
                if sibling.name == "span":
                    break
                if sibling.name == "hr" and parts:
                    break
                text = sibling.get_text(" ", strip=True)
            elif isinstance(sibling, NavigableString):
                text = str(sibling).strip()
            else:
                continue
            clean_text = self._clean_text(text)
            if clean_text:
                parts.append(clean_text)
        return self._clean_text(" ".join(parts))

    def _extract_origin_text(self, soup: BeautifulSoup) -> str:
        origin_label = self._find_label(soup, "Origem", "Origens")
        if origin_label is None:
            return ""
        parent = origin_label.parent if isinstance(origin_label.parent, Tag) else None
        if parent is not None:
            hidden_titles = [
                self._clean_text(h2.get_text(" ", strip=True))
                for h2 in parent.find_all("h2", class_="d-none")
                if self._clean_text(h2.get_text(" ", strip=True))
            ]
            if hidden_titles:
                return ", ".join(hidden_titles)
        return self._extract_labeled_text(origin_label)

    def _get_text_links(self, soup: BeautifulSoup) -> list[str]:
        links: list[str] = []
        seen: set[str] = set()
        for label in ("Texto atualizado", "Texto original"):
            for anchor in soup.find_all("a", href=True):
                if self._clean_text(anchor.get_text(" ", strip=True)) == label:
                    href = str(anchor.get("href", ""))
                    if href and href not in seen:
                        links.append(href)
                        seen.add(href)
        for label in ("Texto atualizado", "Texto original"):
            for elem in soup.find_all(
                string=re.compile(re.escape(label), re.IGNORECASE)
            ):
                parent = elem.parent
                if not isinstance(parent, Tag):
                    continue
                if parent.name == "a" and parent.get("href"):
                    href = str(parent.get("href", ""))
                    if href and href not in seen:
                        links.append(href)
                        seen.add(href)
                    continue
                a_tag = parent.find("a", href=True)
                if a_tag:
                    href = str(a_tag.get("href", ""))
                    if href and href not in seen:
                        links.append(href)
                        seen.add(href)
        return links

    @staticmethod
    def _normalize_situation_value(situation: str) -> str:
        normalized = re.sub("\\s+", " ", situation).strip().casefold()
        return {
            DEFAULT_VALID_SITUATION.casefold(): DEFAULT_VALID_SITUATION,
            "revogada": "Revogada",
            "declarada inconstitucional": "Declarada inconstitucional",
            "inconstitucional": "Declarada inconstitucional",
            "tornada sem efeito": "Tornada sem efeito",
        }.get(normalized, situation.strip())

    def _infer_situation(self, title: str, soup: BeautifulSoup) -> str:
        situation = self._extract_labeled_text(self._find_label(soup, "Situação"))
        if situation:
            return self._normalize_situation_value(self._clean_text(situation))
        clean_title = self._clean_text(title)
        for pattern, canonical_situation in _TITLE_SITUATION_PATTERNS:
            if pattern.search(clean_title):
                return canonical_situation
        return DEFAULT_VALID_SITUATION

    def _is_pdf_observation_block(self, text: str) -> bool:
        return bool(_PDF_OBSERVATION_PATTERN.search(self._clean_text(text)))

    def _is_annex_stub(self, text: str) -> bool:
        normalized = self._clean_text(text.strip(' "“”'))
        if not normalized or len(normalized) > 180:
            return False
        if normalized.casefold().startswith("(a que se refere"):
            return True
        return bool(re.fullmatch("anexo(?:\\s+[ivxlcdm]+)?", normalized, re.IGNORECASE))

    def _strip_trailing_annex_stubs(self, text_norm_span: Tag) -> None:
        container = text_norm_span
        direct_children = [
            child for child in text_norm_span.children if isinstance(child, Tag)
        ]
        if len(direct_children) == 1 and direct_children[0].name == "div":
            container = direct_children[0]
        while True:
            blocks = [
                child
                for child in container.children
                if isinstance(child, Tag)
                and self._clean_text(child.get_text(" ", strip=True))
            ]
            if not blocks:
                return
            last_block = blocks[-1]
            if not self._is_annex_stub(last_block.get_text(" ", strip=True)):
                return
            last_block.decompose()

    def _prepare_text_norma_html(
        self, text_norm_span: Tag, document_page_url: str
    ) -> tuple[str, str, list[str]]:
        span_soup = BeautifulSoup(str(text_norm_span), "lxml")
        clean_span = span_soup.find("span", class_="textNorma")
        if clean_span is None:
            clean_span = span_soup
        pdf_links: list[str] = []
        for anchor in clean_span.find_all("a", href=True):
            href = str(anchor.get("href", ""))
            if "mediaserver" not in href:
                continue
            pdf_links.append(urljoin(document_page_url, href))
            block = anchor.find_parent(["p", "div", "li"])
            block_text = (
                block.get_text(" ", strip=True)
                if block
                else anchor.get_text(" ", strip=True)
            )
            if block is not None and self._is_pdf_observation_block(block_text):
                block.decompose()
            else:
                anchor.decompose()
        self._strip_trailing_annex_stubs(clean_span)
        html_string = re.sub(
            "Data da última atualização:\\s*\\d{2}/\\d{2}/\\d{4}",
            "",
            str(clean_span),
            flags=re.IGNORECASE,
        )
        html_string = self._strip_malformed_share_links(html_string)
        plain_text = self._clean_text(clean_span.get_text(" ", strip=True))
        return (html_string, plain_text, pdf_links)

    def _strip_malformed_share_links(self, html_string: str) -> str:
        html_string = _TIMESTAMPED_SHARE_LINK_PATTERN.sub("\\1\\2", html_string)
        html_string = _ALMG_SHARE_URL_PATTERN.sub("", html_string)
        return re.sub("\\s+\\)", ")", html_string)

    def _has_substantive_html_text(self, text: str) -> bool:
        clean_text = self._clean_text(text)
        if not clean_text:
            return False
        if _SUBSTANTIVE_TEXT_PATTERN.search(clean_text):
            return True
        return len(clean_text) >= 350

    @staticmethod
    def _is_mg_error_page(page_title: str) -> bool:
        """Check if a page title indicates an ALMG error/access-denied page."""
        return "Acesso Proibido" in page_title or "Erro" in page_title

    async def _resolve_mg_text_page(
        self, text_links: list[str], title: str
    ) -> tuple[str, Tag | None, bytes | None, str]:
        "Iterate candidate text-page URLs and return the first with textNorma."
        last_error = "Failed to fetch document text page"
        for text_link in text_links:
            candidate_url = urljoin(self.base_url, text_link)
            if candidate_url.rstrip("/") == self.base_url.rstrip("/"):
                last_error = (
                    "Document link resolves to base URL (no document text available)"
                )
                continue
            span, mhtml, error = await self._try_fetch_text_norma(candidate_url, title)
            if span is not None:
                return (candidate_url, span, mhtml, "")
            if error:
                last_error = error
        return ("", None, None, last_error)

    async def _convert_mg_content(
        self,
        text_norm_span: Tag,
        document_page_url: str,
        page_mhtml: bytes | None,
        title: str,
        norm_type: str,
        data: dict,
        year: int,
    ) -> Document | None:
        "Convert a textNorma span to a Document (HTML or PDF path)."
        html_string, plain_text, pdf_links = self._prepare_text_norma_html(
            text_norm_span, document_page_url
        )
        if pdf_links and (not self._has_substantive_html_text(plain_text)):
            pdf_link = pdf_links[0]
            logger.info(
                f"Document {title} is an image PDF, extracting text from image. URL: {document_page_url}"
            )
            text_markdown, raw, content_ext = await self.download(pdf_link)
            valid, reason = valid_markdown(text_markdown)
            if not valid:
                raise ValueError(f"Invalid PDF markdown: {reason}")
            return Document(
                **data,
                year=year,
                text_markdown=text_markdown,
                document_url=pdf_link,
                raw=raw,
                raw_ext=content_ext or ".pdf",
            )
        html_string = wrap_html(html_string)
        text_markdown = await self.markdown(html=html_string)
        valid, reason = valid_markdown(text_markdown)
        if not valid:
            raise ValueError(f"Invalid markdown: {reason}")
        return Document(
            **data,
            year=year,
            text_markdown=text_markdown,
            document_url=document_page_url,
            raw=page_mhtml,
            raw_ext=".mhtml",
        )

    async def fetch(self, doc_info: dict) -> Document | None:
        """Get document data from a listing entry."""
        doc_info = dict(doc_info)
        detail_link = doc_info.get("html_link", "")
        title = doc_info.get("title", "Unknown")
        detail_url = urljoin(self.base_url, detail_link)
        norm_type = doc_info.get("type", "")
        soup_data = (await self.http.request(detail_url)).soup()
        if not soup_data:
            raise ValueError("Failed to fetch document page (soup is None)")
        text_links = self._get_text_links(soup_data)
        if not text_links:
            raise ValueError("No text link found (Texto atualizado/original)")
        doc_type = doc_info.get("type") or self._normalize_type(title, detail_link)
        data = {
            "title": title,
            "summary": doc_info.get("summary", ""),
            "type": doc_type,
            "origin": self._extract_origin_text(soup_data),
            "situation": self._infer_situation(title, soup_data),
            "publication": self._extract_labeled_text(
                self._find_label(soup_data, "Fonte")
            ),
            "tags": self._extract_labeled_text(self._find_label(soup_data, "Resumo")),
            "subject": self._extract_labeled_text(
                self._find_label(soup_data, "Assunto Geral")
            ),
        }
        (
            document_page_url,
            text_norm_span,
            page_mhtml,
            error,
        ) = await self._resolve_mg_text_page(text_links, title)
        if text_norm_span is None:
            if error:
                raise ValueError(error)
            return None
        return await self._convert_mg_content(
            text_norm_span,
            document_page_url,
            page_mhtml,
            title,
            norm_type,
            data,
            doc_info.get("year", 0),
        )

    async def list_documents(self, year: int) -> AsyncIterator[dict]:
        url = self._build_search_url(year, 1)
        soup = (await self.http.request(url)).soup()

        async def fetch_page(page: int) -> list[dict]:
            return (
                await self._get_docs_links(url, soup=soup)
                if page == 1
                else await self._get_docs_links(self._build_search_url(year, page))
            )

        async for ref in self.paginate(fetch_page, self._extract_total_pages(soup)):
            yield {
                "year": year,
                "situation": DEFAULT_VALID_SITUATION,
                **ref,
                "document_url": urljoin(self.base_url, ref["html_link"]),
            }

    async def _try_fetch_text_norma(
        self, url: str, title: str
    ) -> tuple[Tag | None, bytes | None, str]:
        soup, mhtml = await self.browser.snapshot(url)
        span = soup.find("span", class_="textNorma")
        if span is not None:
            return (span, mhtml, "")
        return (None, None, "Could not find span.textNorma in document page")
