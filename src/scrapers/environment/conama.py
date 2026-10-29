import re
import urllib.parse
from collections.abc import AsyncIterator
from typing import Any

from bs4 import BeautifulSoup
from loguru import logger

from src.core.convert import (
    bytes_to_markdown,
    calc_pages,
    strip_html_chrome,
    valid_markdown,
)
from src.core.document import Document
from src.core.scraper import (
    DEFAULT_INVALID_SITUATION,
    DEFAULT_VALID_SITUATION,
    BaseScraper,
)

TYPES = {
    "Resolução": 1,
    "Moção": 2,
    "Recomendação": 3,
    "Proposição": 4,
    "Decisão": 5,
    "Portaria": 6,
}


class ConamaScraper(BaseScraper):
    """Webscraper for Conama (Conselho Nacional do Meio Ambiente) website (https://conama.mma.gov.br/atos-normativos-sistema)

    Year start (earliest on source): 1984

    The API returns all norm types in a single unfiltered request per year —
    each row already carries ``id_tipo_ato`` and ``nomeato`` — so no per-type
    iteration is needed.

    Example search request (no tipo filter):
    https://conama.mma.gov.br/?option=com_sisconama&order=asc&offset=0&limit=100&task=atosnormativos.getList&ano=2000

    Observation: Conama does not have a situation field; invalid norms will
    have an indication in the document text (status field).
    """

    name = "CONAMA"
    types = TYPES
    situations = {}
    base_url = "https://conama.mma.gov.br/"

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.params = {"option": "com_sisconama", "order": "asc", "limit": 100}
        self._situation_regex = re.compile("Revogad|Revogação", re.IGNORECASE)

    def _format_search_url(self, offset: int = 0, year: str | None = None) -> str:
        """Format URL for the unfiltered norm-list endpoint."""
        return f"{self.base_url}?option={self.params['option']}&order={self.params['order']}&offset={offset}&limit={self.params['limit']}&task=atosnormativos.getList&ano={year}"

    _DOU_GARBAGE_STRINGS = [
        "DOU - Imprensa Nacional",
        "DIÁRIO OFICIAL DA UNIÃO",
        "Imprensa Nacional",
    ]
    _PUBLICATION_DOU_RE = re.compile(
        "^(?:(?:o\\s+)?publicad[oa]\\s+n[oa]\\s+)?(?:(?:publica[cç][aã]o\\s+)?d\\s*\\.?\\s*o\\s*\\.?\\s*u\\s*\\.?(?:\\s+n[ºo°]\\s*\\d+)?),?\\s*de\\s*(?:\\d{1,2}/\\d{1,2}/\\d{2,4}|\\d{1,2}\\s+de\\s+[\\wçãéêíóôõú]+\\s+de\\s+\\d{4})(?:,\\s*se[cç][aã]o\\s*\\d+)?(?:,\\s*p[áa]g(?:s|inas)?\\.?\\s*[0-9-]+)?$",
        re.IGNORECASE,
    )
    _IMPRENSA_NACIONAL_NOTICE_RE = re.compile(
        "^https?://portal\\.imprensanacional\\.gov\\.br/?\\s+devido\\s+a\\s+data\\s+de\\s+publica[cç][aã]o\\s+ser\\s+anterior\\s+ao\\s+ano\\s+de\\s+1990$",
        re.IGNORECASE,
    )
    _BOLETIM_SERVICO_RE = re.compile(
        "^(?:(?:o\\s+)?publicad[oa]\\s+n[oa]\\s+)?boletim\\s+de\\s+servi[cç]o(?:\\s*/\\s*mi|/mi)?(?:\\s+n[ºo°]\\s*\\d+)?,?\\s*de\\s*(?:\\d{1,2}/\\d{1,2}/\\d{2,4}|\\d{1,2}º?\\s+de\\s+[\\wçãéêíóôõú]+\\s+de\\s+\\d{4})(?:,\\s*do\\s+minist[ée]rio\\s+do\\s+interior)?$",
        re.IGNORECASE,
    )
    _LOCAL_FILE_FOOTER_RE = re.compile(
        "^file:///.+\\.(?:html|htm)\\s+\\d+/\\d+$", re.IGNORECASE
    )
    _PAGE_NUMBER_BLOCK_RE = re.compile("^\\d{1,6}$")
    _DISCLAIMER_PATTERNS = [
        re.compile("Est[ea] conte.do não substitui", re.IGNORECASE),
        re.compile("Est[ea] texto não substitui", re.IGNORECASE),
        re.compile("Ess[ea] texto não substitui", re.IGNORECASE),
        re.compile("\\*?Obs:?\\*?\\*?\\s*não há registro no sítio", re.IGNORECASE),
    ]
    _PDF_CLEANUP_PATTERNS: list[tuple[str, str, int]] = [
        ("\\x0c", "", 0),
        ("Ato \\d+ \\(\\d+\\)\\s+SEI \\S+ / pg\\. \\d+", "", 0),
        ("Documento assinado eletronicamente por\\s+.+?de 2020\\s*\\.?", "", re.DOTALL),
        (
            "A autenticidade deste documento pode ser conferida no site\\s+.+?código CRC \\w+\\.?",
            "",
            re.DOTALL,
        ),
        ("Refer.ncia: Processo n. \\S+\\s*\\n*SEI n. \\d+", "", 0),
        ("\\d{2}/\\d{2}/\\d{4},\\s*\\d{2}:\\d{2}\\s*\\n*", "", 0),
        ("^.+?- DOU - Imprensa Nacional\\s*\\n*", "", re.MULTILINE),
        ("DI.RIO OFICIAL DA UNI.O\\s*\\n*", "", 0),
        ("Publicado em:.*?P.gina:.*?\\n*", "", re.DOTALL),
        (".rg.o:.*?Meio Ambiente.*?\\n+", "", 0),
        ("https?://www\\.in\\.gov\\.br/\\S+", "", 0),
        ("\\n\\d+/\\d+\\s*$", "\n", re.MULTILINE),
        ("^\\s*\\d{1,4}\\s*\\n+", "", 0),
    ]

    @staticmethod
    def _normalize_cleanup_text(text: str) -> str:
        text = re.sub("[*_]+", "", text)
        return re.sub("\\s+", " ", text).strip(" .")

    @classmethod
    def _is_publication_dou_reference(cls, text: str) -> bool:
        normalized_text = cls._normalize_cleanup_text(text)
        return bool(cls._PUBLICATION_DOU_RE.fullmatch(normalized_text))

    @classmethod
    def _is_imprensa_nacional_notice(cls, text: str) -> bool:
        normalized_text = cls._normalize_cleanup_text(text)
        return bool(cls._IMPRENSA_NACIONAL_NOTICE_RE.fullmatch(normalized_text))

    @classmethod
    def _is_boletim_servico_reference(cls, text: str) -> bool:
        normalized_text = cls._normalize_cleanup_text(text)
        return bool(cls._BOLETIM_SERVICO_RE.fullmatch(normalized_text))

    @classmethod
    def _is_local_file_footer(cls, text: str) -> bool:
        normalized_text = cls._normalize_cleanup_text(text)
        return bool(cls._LOCAL_FILE_FOOTER_RE.fullmatch(normalized_text))

    @classmethod
    def _is_numeric_metadata_block(cls, text: str) -> bool:
        lines = [line.strip() for line in text.splitlines() if line.strip()]
        if not 1 <= len(lines) <= 4:
            return False
        return all((line.isdigit() and len(line) <= 6 for line in lines))

    def _is_source_footer_block(self, text: str) -> bool:
        return any(
            (
                self._is_publication_dou_reference(text),
                self._is_imprensa_nacional_notice(text),
                self._is_boletim_servico_reference(text),
                self._is_local_file_footer(text),
            )
        )

    def _strip_publication_metadata_blocks(self, text: str) -> str:
        blocks = [
            block.strip()
            for block in re.split("\\n{2,}", text.strip())
            if block.strip()
        ]
        if not blocks:
            return text.strip()
        cleaned_blocks: list[str] = []
        idx = 0
        while idx < len(blocks):
            block = blocks[idx]
            if idx + 1 < len(blocks):
                next_block = blocks[idx + 1]
                if self._is_numeric_metadata_block(next_block):
                    combined_block = f"{self._normalize_cleanup_text(block)} {self._normalize_cleanup_text(next_block)}"
                    if self._is_publication_dou_reference(
                        combined_block
                    ) or self._is_boletim_servico_reference(combined_block):
                        idx += 2
                        continue
            if self._is_source_footer_block(block):
                idx += 1
                if idx < len(blocks) and self._is_numeric_metadata_block(blocks[idx]):
                    idx += 1
                continue
            cleaned_blocks.append(block)
            idx += 1
        return "\n\n".join(cleaned_blocks).strip()

    def _clean_dou_html(self, soup: BeautifulSoup) -> BeautifulSoup:
        """Remove DOU website chrome from HTML before markdown conversion."""
        strip_html_chrome(soup)
        for el in soup.find_all("title"):
            el.decompose()
        for el in soup.find_all(True):
            if el.decomposed:
                continue
            if el.find(True):
                continue
            text = el.get_text(" ", strip=True)
            normalized_text = self._normalize_cleanup_text(text)
            for garbage in self._DOU_GARBAGE_STRINGS:
                if garbage in normalized_text and len(normalized_text) < 300:
                    el.decompose()
                    break
            else:
                if (
                    self._is_publication_dou_reference(normalized_text)
                    or self._is_imprensa_nacional_notice(normalized_text)
                    or self._is_boletim_servico_reference(normalized_text)
                    or self._is_local_file_footer(normalized_text)
                ) and len(normalized_text) < 200:
                    el.decompose()
                    continue
                if re.match("^Publicado em:\\s*\\d", normalized_text):
                    el.decompose()
                    continue
                if re.match("^Órgão:\\s*", normalized_text):
                    el.decompose()
                    continue
                if re.match(
                    "^\\d{2}/\\d{2}/\\d{4},\\s*\\d{2}:\\d{2}$", normalized_text
                ):
                    el.decompose()
                    continue
                if re.match("^\\d+/\\d+$", normalized_text):
                    el.decompose()
                    continue
                for pat in self._DISCLAIMER_PATTERNS:
                    if pat.search(normalized_text) and len(normalized_text) < 200:
                        el.decompose()
                        break
        for el in soup.find_all("a"):
            if el.decomposed:
                continue
            href = str(el.get("href") or "")
            if "in.gov.br" in href:
                el.decompose()
            else:
                el.unwrap()
        return soup

    def _clean_pdf_markdown(self, text: str) -> str:
        "Clean SEI/PDF-specific artefacts from extracted text."
        for pattern, replacement, flags in self._PDF_CLEANUP_PATTERNS:
            text = re.sub(pattern, replacement, text, flags=flags)
        for pat in self._DISCLAIMER_PATTERNS:
            text = pat.sub("", text)
        text = self._strip_publication_metadata_blocks(text)
        return text.strip()

    async def fetch(self, doc_info: dict) -> Document | None:
        """Fetch and convert a single CONAMA norm.

        Download URL pattern:
        https://conama.mma.gov.br/?option=com_sisconama&task=arquivo.download&id={id}
        """
        doc_id = doc_info.get("aid")
        doc_number = doc_info.get("numero", "")
        doc_type = doc_info.get("nomeato", "")
        doc_year = doc_info.get("ano", "")
        title = f"{doc_type} CONAMA Nº {doc_number}/{doc_year}"
        if doc_id is None:
            logger.info(
                f"Skipping {title} as it has no document ID attached (aid is null)."
            )
            return None
        doc_url = urllib.parse.urljoin(
            self.base_url, f"?option=com_sisconama&task=arquivo.download&id={doc_id}"
        )
        doc_description = doc_info.get("descricao", "")
        doc_status = doc_info.get("status")
        doc_keyword = doc_info.get("palavra_chave", "")
        doc_origin = doc_info.get("porigem", "")
        resp = await self.http.request(doc_url)
        content_type = (resp.content_type or "").lower()
        is_html = "html" in content_type
        if is_html:
            soup = self._clean_dou_html(resp.soup())
            text_markdown = await self.markdown(html=str(soup), base_url=doc_url)
            text_markdown = self._strip_publication_metadata_blocks(text_markdown)
            raw = resp.content
            content_ext = ".html"
        else:
            body = resp.content
            text_markdown = await bytes_to_markdown(
                body, "", self.base_url, self.http, self.ocr
            )
            text_markdown = self._clean_pdf_markdown(text_markdown)
            raw = body
            content_ext = ".pdf"
        text_markdown = text_markdown.strip() if text_markdown else text_markdown
        if text_markdown is None or not text_markdown.strip():
            logger.warning(
                f"Empty markdown for {doc_type} CONAMA Nº {doc_number}/{doc_year}"
            )
            raise ValueError("Empty markdown after conversion")
        situation = DEFAULT_VALID_SITUATION
        if doc_status and self._situation_regex.search(doc_status):
            situation = DEFAULT_INVALID_SITUATION
        is_valid, reason = valid_markdown(text_markdown, 200)
        if not is_valid:
            logger.warning(
                f"Markdown text for {doc_type} CONAMA Nº {doc_number}/{doc_year} is invalid: {reason}. Length: {(len(text_markdown) if text_markdown else 0)} chars."
            )
            raise ValueError(
                "Markdown text very short after cleaning, may indicate conversion issues"
            )
        return Document(
            year=doc_year,
            title=title,
            type=doc_type,
            id=doc_id,
            number=doc_number,
            summary=doc_description,
            situation=situation,
            keyword=doc_keyword,
            origin=doc_origin,
            text_markdown=text_markdown,
            document_url=doc_url,
            raw=raw,
            raw_ext=content_ext,
        )

    async def _fetch_page_norms(self, offset: int, year_str: str) -> list[dict]:
        """Fetch norms from a single pagination page."""
        url = self._format_search_url(offset=offset, year=year_str)
        response = await self.http.request(url)
        try:
            json_response = response.json()
            return json_response["data"]["rows"]
        except (KeyError, ValueError, TypeError) as e:
            logger.error(
                f"Failed to parse pagination JSON for year {year_str} offset {offset}: {e}"
            )
            raise

    async def list_documents(self, year: int) -> AsyncIterator[dict]:
        data = (
            await self.http.request(self._format_search_url(offset=0, year=str(year)))
        ).json()["data"]
        limit = self.params["limit"]

        async def fetch_page(page: int) -> list[dict]:
            return (
                data["rows"]
                if page == 0
                else await self._fetch_page_norms(page * limit, str(year))
            )

        async for ref in self.paginate(
            fetch_page, calc_pages(data["total"], limit) - 1, start=0
        ):
            url = urllib.parse.urljoin(
                self.base_url,
                f"?option=com_sisconama&task=arquivo.download&id={ref.get('aid')}",
            )
            yield {
                **ref,
                "year": year,
                "title": f"{ref.get('nomeato', '')} CONAMA Nº {ref.get('numero', '')}/{year}",
                "type": ref.get("nomeato", ""),
                "document_url": url,
                "situation": DEFAULT_VALID_SITUATION,
            }
