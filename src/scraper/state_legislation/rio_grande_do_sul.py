from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from src.scraper.base.schemas import ScrapedDocument
import re
from typing import TYPE_CHECKING
from urllib.parse import urlencode

from bs4 import BeautifulSoup
from loguru import logger

from src.scraper.base.converter import valid_markdown, wrap_html
from src.scraper.base.scraper import StateScraper

if TYPE_CHECKING:
    from src.scraper.base.schemas import ScrapedDocument


# ALRS does not have a type field, norm type is gotten while scraping
TYPES = {}

# ALRS does not have a situation field, cannot distinguish between valid and invalid norms
SITUATIONS = {"Não consta": "Não consta"}


class RSAlrsScraper(StateScraper):
    """Webscraper for Rio Grande do Sul state legislation website (https://www.al.rs.gov.br/legis)

    Year start (earliest on source): 1830

    Example search request (GET): https://www.al.rs.gov.br/legis/M010/M0100008.asp?txthNRO_PROPOSICAO=&txthAdin=&txthQualquerPalavra=&cboTipoNorma=&TxtNumero_Norma=&TxtAno=1830&txtData=&txtDataInicial=&txtDataFinal=&txtPalavraChave=&TxtQualquerPalavra=&CmbPROPOSICAO=&txtProcAdin=&cmbNumero_Docs=50&txtOrdenacao=data&txtOperacaoFormulario=Pesquisar&pagina=1
    """

    def __init__(
        self,
        base_url: str = "https://www.al.rs.gov.br",
        **kwargs,
    ):
        super().__init__(
            base_url,
            types=TYPES,
            situations=SITUATIONS,
            name="RIO_GRANDE_DO_SUL",
            **kwargs,
        )

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
            soup = await self.request_service.get_soup(url)
        table = soup.find("table", class_="TableResultado")
        items = table.find_all("tr")

        # get all html links
        html_links = []
        for item in items:
            tds = item.find_all("td")
            if len(tds) != 4:
                continue

            # if "Tipo Norma" in td, it is the header, skip it
            if "tipo norma" in "".join([td.text for td in tds]).lower():
                continue

            type = tds[0].text.strip().capitalize()
            norm_number = tds[1].text.strip()
            date = tds[2].text.strip()
            title = f"{type} {norm_number} DE {date}"
            summary = tds[3].text.strip()

            # html link is gotten from javascript onclick
            # https://www.al.rs.gov.br/legis/M010/M0100018.asp?Hid_IdNorma=72606&Texto=&Origem=1
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
            r"\n?[^\n]*https?://www\.al\.rs\.gov\.br/legis[^\n]*", "", text
        ).strip()

    def _get_html_string(self, soup: BeautifulSoup) -> str:
        "Get html string from soup"

        # check if norm in html format. It will be in the last tr of table
        table = soup.find("table")
        if not table:
            return ""

        table = table.find("tbody")

        # setting recursive to false to avoid getting tr from nested tables within text
        items = table.find_all("tr", recursive=False)

        html_string = ""
        if len(items) > 5:
            tr = items[-1]
            norm_text = tr.text.strip()

            html_string = wrap_html(norm_text)

        return html_string

    async def _get_doc_data(self, doc_info: dict) -> ScrapedDocument | None:
        """Get document data from given document dict"""
        doc_info = dict(doc_info)
        html_link = doc_info.pop("html_link")
        soup = await self.request_service.get_soup(html_link)

        # check for error (some documents are not available)
        if not soup:
            logger.error(f"Error getting document data: {html_link}")
            await self._save_doc_error(
                title=doc_info.get("title", "Unknown"),
                norm_type=doc_info.get("type", ""),
                html_link=html_link,
                error_message="Page could not be displayed or soup is None",
            )
            return None

        soup_text = str(soup).lower()
        if "a página não pode ser exibida" in soup_text:
            logger.error(f"Error getting document data: {html_link}")
            await self._save_doc_error(
                title=doc_info.get("title", "Unknown"),
                norm_type=doc_info.get("type", ""),
                html_link=html_link,
                error_message="Page could not be displayed or soup is None",
            )
            return None

        # get situation, subject and pdf_link.
        situation = soup.find("td", string="Situação:")
        if situation:
            situation = situation.find_next("td").text.strip()

        subject = soup.find("td", string="Assunto:")
        if subject:
            subject = subject.find_next("td").text.strip()
        html_link = (
            soup.find("td", string=re.compile(r"Links:"))
            .find_next("td")
            .find("a")["href"]
        )
        # add base url if not present
        if not html_link.startswith("http"):
            html_link = f"{self.base_url}/legis/M010/{html_link}"

        # <iframe name=txt_Texto_teste src='https://ww3.al.rs.gov.br/filerepository/repLegis/arquivos/DECR IMP SN 1830 S FRANCISCO.pdf' width=100% height=100% frameborder=0></iframe>

        # get text from pdf ( need to make a requst to html and get pdf link from iframe)
        soup = await self.request_service.get_soup(html_link)

        # invalid norm
        if not soup:
            await self._save_doc_error(
                title=doc_info.get("title", "Unknown"),
                norm_type=doc_info.get("type", ""),
                html_link=html_link,
                error_message="Norm has no text content",
            )
            return None

        soup_text = str(soup).lower()
        if "norma sem texto" in soup_text or "sem texto para exibi" in soup_text:
            await self._save_doc_error(
                title=doc_info.get("title", "Unknown"),
                norm_type=doc_info.get("type", ""),
                html_link=html_link,
                error_message="Norm has no text content",
            )
            return None

        pdf_link = None
        html_string = self._get_html_string(soup)
        if not html_string:
            pdf_link = soup.find("iframe")
            if pdf_link:
                pdf_link = pdf_link["src"]
            else:
                # pdf_link may be in the form of a javascript window.open
                m = re.search(r"window\.open\('([^']+)'", soup_text)
                if not m:
                    logger.error(f"Could not find PDF link for document: {html_link}")
                    await self._save_doc_error(
                        title=doc_info.get("title", "Unknown"),
                        norm_type=doc_info.get("type", ""),
                        html_link=html_link,
                        error_message="No PDF link found (no iframe, no window.open)",
                    )
                    return None
                pdf_link = m.group(1)

        document_url = pdf_link if pdf_link else html_link

        if self._is_already_scraped(document_url, doc_info.get("title", "")):
            return None

        if html_string:
            # Use direct HTML content conversion
            text_markdown = await self._get_markdown(html_content=html_string)
            try:
                _, mhtml_bytes = await self._fetch_soup_and_mhtml(html_link)
                raw_content = mhtml_bytes
                content_ext = ".mhtml"
            except Exception as exc:
                logger.warning(f"MHTML capture failed for {html_link}: {exc}")
                await self._save_doc_error(
                    title=doc_info.get("title", "Unknown"),
                    norm_type=doc_info.get("type", ""),
                    html_link=html_link,
                    error_message=f"MHTML capture failed: {exc}",
                )
                return None
        else:
            text_markdown, raw_content, content_ext = await self._download_and_convert(
                pdf_link
            )

        text_markdown = self._clean_rs_markdown(text_markdown)
        valid, reason = valid_markdown(text_markdown)
        if not valid:
            logger.error(f"Error getting markdown from pdf: {pdf_link}")
            await self._save_doc_error(
                title=doc_info.get("title", "Unknown"),
                norm_type=doc_info.get("type", ""),
                html_link=pdf_link if pdf_link else html_link,
                error_message=f"Invalid markdown: {reason}",
            )
            return None

        result = {
            **doc_info,
            "situation": situation,
            "subject": subject,
            "text_markdown": text_markdown.strip(),
            "document_url": document_url,
            "_raw_content": raw_content,
            "_content_extension": content_ext,
        }

        return result

    async def _before_scrape(self) -> None:
        await self._fetch_and_save_constitution(
            url="https://ww2.al.rs.gov.br/dal/LinkClick.aspx?fileticket=9p-X_3esaNg%3d&tabid=3683&mid=5358",
            title="Constituição do Estado do Rio Grande do Sul",
            year=1989,
            date="",
            summary="Texto constitucional de 3 de outubro de 1989 com as alterações adotadas pelas Emendas Constitucionais de n.º 1, de 1991, a 85, de 2023",
            situation="Sem revogação expressa",
        )

    async def _scrape_year(self, year: int) -> list[dict]:
        """Scrape norms for a specific year"""
        # get total pages
        url = self._build_search_url(year, 1)
        soup = await self.request_service.get_soup(url)

        total_pages = soup.find("img", alt="Última Página")
        if total_pages:
            total_pages = total_pages.find_parent("a")
            total_pages = int(total_pages["href"].split("txtPage=")[-1].split("&")[0])
        else:
            total_pages = 0  # no documents for this year

        if total_pages == 0:
            return []

        ctx = {"year": year, "type": "NA", "situation": "NA"}
        documents = await self._get_docs_links(url, soup=soup) or []
        documents.extend(
            await self._fetch_all_pages(
                lambda p: self._get_docs_links(self._build_search_url(year, p)),
                total_pages,
                context=ctx,
                desc="RIO GRANDE DO SUL | get_docs_links",
            )
        )

        for doc in documents:
            doc["year"] = year
        results = await self._process_documents(
            documents,
            year=year,
            norm_type="NA",
            desc="RIO GRANDE DO SUL",
        )

        return results
