import re
from collections.abc import AsyncIterator
from urllib.parse import urlencode, urljoin

from bs4 import BeautifulSoup
from loguru import logger

from src.core.convert import valid_markdown, wrap_html
from src.core.document import Document
from src.core.scraper import DEFAULT_VALID_SITUATION, BaseScraper

TYPES = {}
SITUATIONS = {"Não consta": "Não consta"}


class RSAlrsScraper(BaseScraper):
    """Webscraper for Rio Grande do Sul state legislation website (https://www.al.rs.gov.br/legis)

    Year start (earliest on source): 1830

    Example search request (GET): https://www.al.rs.gov.br/legis/M010/M0100008.asp?txthNRO_PROPOSICAO=&txthAdin=&txthQualquerPalavra=&cboTipoNorma=&TxtNumero_Norma=&TxtAno=1830&txtData=&txtDataInicial=&txtDataFinal=&txtPalavraChave=&TxtQualquerPalavra=&CmbPROPOSICAO=&txtProcAdin=&cmbNumero_Docs=50&txtOrdenacao=data&txtOperacaoFormulario=Pesquisar&pagina=1
    """

    types = TYPES
    situations = SITUATIONS
    name = "RIO_GRANDE_DO_SUL"
    base_url = "https://www.al.rs.gov.br"

    def _build_search_url(self, year: int, page: int = 1) -> str:
        """Build search URL for page 1 (initial search with year filter).

        Page 1 uses the search form endpoint (M0100008.asp) with all filter params.
        Pages 2+ use the pagination endpoint (M0100017.asp?txtPage=N) which relies
        on the server-side session state set by the page-1 request.
        """
        if page > 1:
            return (
                f"{self.base_url}/legis/M010/M0100017.asp?txtPage={page}&txtNumDocs=50"
            )
        params = {
            "txthNRO_PROPOSICAO": "",
            "txthAdin": "",
            "txthQualquerPalavra": "",
            "cboTipoNorma": "",
            "TxtNumero_Norma": "",
            "TxtAno": year,
            "txtData": "",
            "txtDataInicial": "",
            "txtDataFinal": "",
            "txtPalavraChave": "",
            "TxtQualquerPalavra": "",
            "CmbPROPOSICAO": "",
            "txtProcAdin": "",
            "cmbNumero_Docs": 50,
            "txtOrdenacao": "data",
            "txtOperacaoFormulario": "Pesquisar",
        }
        return f"{self.base_url}/legis/M010/M0100008.asp?{urlencode(params)}"

    async def _get_docs_links(
        self, url: str, soup: BeautifulSoup | None = None
    ) -> list:
        """Get documents html links from given page.
        Returns a list of dicts with keys 'type', 'title', 'date',  'summary', 'html_link'
        """
        if soup is None:
            soup = (await self.http.request(url)).soup()
        table = soup.find("table", class_="TableResultado")
        if table is None:
            raise ValueError("Missing RS results table")
        items = table.find_all("tr")
        html_links = []
        for item in items:
            tds = item.find_all("td")
            if len(tds) != 4:
                continue
            if "tipo norma" in "".join([td.text for td in tds]).lower():
                continue
            type = tds[0].text.strip().capitalize()
            norm_number = tds[1].text.strip()
            date = tds[2].text.strip()
            title = f"{type} {norm_number} DE {date}"
            summary = tds[3].text.strip()
            norm_id = tds[1].find("a")["onclick"].split('"')[1]
            html_link = f"{self.base_url}/legis/M010/M0100018.asp?Hid_IdNorma={norm_id}&Texto=&Origem=1"
            html_links.append(
                {
                    "type": type,
                    "title": title,
                    "date": date,
                    "summary": summary,
                    "html_link": html_link,
                }
            )
        return html_links

    @staticmethod
    def _clean_rs_markdown(text: str) -> str:
        """Remove RS ALRS PDF footer artifacts that leak between pages.

        The footer of every page in the RS ALRS PDFs contains the URL
        http://www.al.rs.gov.br/legis which html-to-markdown extracts at each
        page break in several forms:
          - bare line:            'http://www.al.rs.gov.br/legis'
          - with page number:     'http://www.al.rs.gov.br/legis  4'
          - inside a table row:   '| http://www.al.rs.gov.br/legis  | … | 3 |'
        Matching the whole line (any leading chars, URL, any trailing chars)
        covers all observed variants.
        """
        return re.sub(
            "\\n?[^\\n]*https?://www\\.al\\.rs\\.gov\\.br/legis[^\\n]*", "", text
        ).strip()

    def _get_html_string(self, soup: BeautifulSoup) -> str:
        """Get html string from soup"""
        table = soup.find("table")
        if not table:
            return ""
        table = table.find("tbody") or table
        items = table.find_all("tr", recursive=False)
        html_string = ""
        if len(items) > 5:
            tr = items[-1]
            norm_text = tr.text.strip()
            html_string = wrap_html(norm_text)
        return html_string

    async def fetch(self, doc_info: dict) -> Document | None:
        """Get document data from given document dict"""
        if doc_info.get("constitution"):
            return await self.fetch_pdf(doc_info)
        doc_info = dict(doc_info)
        html_link = doc_info.pop("html_link")
        soup = (await self.http.request(html_link)).soup()
        if not soup:
            logger.error(f"Error getting document data: {html_link}")
            raise ValueError("Page could not be displayed or soup is None")
        soup_text = str(soup).lower()
        if "a página não pode ser exibida" in soup_text:
            logger.error(f"Error getting document data: {html_link}")
            raise ValueError("Page could not be displayed or soup is None")
        situation = soup.find("td", string="Situação:")
        if situation:
            situation = situation.find_next("td").text.strip()
        subject = soup.find("td", string="Assunto:")
        if subject:
            subject = subject.find_next("td").text.strip()
        html_link = (
            soup.find("td", string=re.compile("Links:"))
            .find_next("td")
            .find("a")["href"]
        )
        if not html_link.startswith("http"):
            html_link = f"{self.base_url}/legis/M010/{html_link}"
        soup = (await self.http.request(html_link)).soup()
        if not soup:
            raise ValueError("Norm has no text content")
        soup_text = str(soup).lower()
        if "norma sem texto" in soup_text or "sem texto para exibi" in soup_text:
            raise ValueError("Norm has no text content")
        pdf_link = None
        html_string = self._get_html_string(soup)
        if not html_string:
            pdf_link = soup.find("iframe")
            if pdf_link:
                pdf_link = pdf_link["src"]
            else:
                m = re.search("window\\.open\\('([^']+)'", soup_text)
                if not m:
                    logger.error(f"Could not find PDF link for document: {html_link}")
                    raise ValueError("No PDF link found (no iframe, no window.open)")
                pdf_link = m.group(1)
        pdf_link = urljoin(html_link, pdf_link) if pdf_link else None
        document_url = pdf_link if pdf_link else html_link
        if html_string:
            text_markdown = await self.markdown(html=html_string)
            try:
                _, mhtml_bytes = await self.browser.snapshot(html_link)
                raw = mhtml_bytes
                content_ext = ".mhtml"
            except Exception as exc:
                logger.warning(f"MHTML capture failed for {html_link}: {exc}")
                raise ValueError(f"MHTML capture failed: {exc}")
        else:
            text_markdown, raw, content_ext = await self.download(pdf_link)
        text_markdown = self._clean_rs_markdown(text_markdown)
        valid, reason = valid_markdown(text_markdown)
        if not valid:
            logger.error(f"Error getting markdown from pdf: {pdf_link}")
            raise ValueError(f"Invalid markdown: {reason}")
        result = {
            **doc_info,
            "situation": situation or DEFAULT_VALID_SITUATION,
            "subject": subject,
            "text_markdown": text_markdown.strip(),
            "document_url": document_url,
            "raw": raw,
            "raw_ext": content_ext,
        }
        return Document(**result)

    async def list_documents(self, year: int) -> AsyncIterator[dict]:
        if year == self.years.start:
            yield {
                "year": 1989,
                "type": "Constituição Estadual",
                "title": "Constituição do Estado do Rio Grande do Sul",
                "document_url": "https://ww2.al.rs.gov.br/dal/LinkClick.aspx?fileticket=9p-X_3esaNg%3d&tabid=3683&mid=5358",
                "situation": DEFAULT_VALID_SITUATION,
                "constitution": True,
            }
        url = self._build_search_url(year, 1)
        soup = (await self.http.request(url)).soup()
        last = soup.find("img", alt="Última Página")
        pages = (
            int(last.find_parent("a")["href"].split("txtPage=")[-1].split("&")[0])
            if last
            else 1
        )
        for page in range(1, pages + 1):
            refs = (
                await self._get_docs_links(url, soup=soup)
                if page == 1
                else await self._get_docs_links(self._build_search_url(year, page))
            )
            for ref in refs:
                yield {
                    "year": year,
                    "situation": DEFAULT_VALID_SITUATION,
                    **ref,
                    "document_url": ref["html_link"],
                }
