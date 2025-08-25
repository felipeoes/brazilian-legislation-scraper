import re
from urllib.parse import urljoin, urlencode

import requests
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
from src.scraper.base.scraper import BaseScaper
from io import BytesIO

# ALRS does not have a type field, norm type is gotten while scraping
TYPES = []

# ALRS does not have a situation field, cannot distinguish between valid and invalid norms
VALID_SITUATIONS = ["Não consta"]

INVALID_SITUATIONS = []  # norms with these situations are invalid norms (no lon

# the reason to have invalid situations is in case we need to train a classifier to predict if a norm is valid or something else similar
SITUATIONS = VALID_SITUATIONS + INVALID_SITUATIONS


class RSAlrsScraper(BaseScaper):
    """Webscraper for Rio Grande do Sul state legislation website (https://www.al.rs.gov.br/legis)


    Example search request (GET): https://www.al.rs.gov.br/legis/M010/M0100008.asp?txthNRO_PROPOSICAO=&txthAdin=&txthQualquerPalavra=&cboTipoNorma=&TxtNumero_Norma=&TxtAno=1830&txtData=&txtDataInicial=&txtDataFinal=&txtPalavraChave=&TxtQualquerPalavra=&CmbPROPOSICAO=&txtProcAdin=&cmbNumero_Docs=50&txtOrdenacao=data&txtOperacaoFormulario=Pesquisar&pagina=1

    """

    def __init__(
        self,
        base_url: str = "https://www.al.rs.gov.br",
        **kwargs,
    ):
        super().__init__(base_url, types=TYPES, situations=SITUATIONS, **kwargs)
        self.docs_save_dir = self.docs_save_dir / "RIO_GRANDE_DO_SUL"
        self.params = {
            "txthNRO_PROPOSICAO": "",
            "txthAdin": "",
            "txthQualquerPalavra": "",
            "cboTipoNorma": "",
            "TxtNumero_Norma": "",
            "TxtAno": 1830,
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
            "pagina": 1,
        }
        self.fetched_constitution = False
        self._initialize_saver()

    def _format_search_url(self, year: int, page: int) -> str:
        self.params["TxtAno"] = year
        self.params["pagina"] = page

        return f"{self.base_url}/legis/M010/M0100008.asp?{urlencode(self.params)}"

    def _get_docs_links(self, url: str) -> list:
        """Get documents html links from given page.
        Returns a list of dicts with keys 'type', 'title', 'date',  'summary', 'html_link'
        """

        soup = self._get_soup(url)
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

    def _get_html_string(self, soup: BeautifulSoup) -> str:
        "Get html string from soup"

        # check if norm in html format. It will be in the last tr of table
        table = soup.find("table")
        if not table:
            return ""
        items = table.find_all("tr")

        html_string = ""
        if len(items) > 5:
            tr = items[-1]
            norm_text = tr.text.strip()

            html_string = f"<html><body>{norm_text}</body></html>"

        return html_string

    def _get_doc_data(self, doc_info: dict) -> dict:
        """Get document data from given document dict"""

        # remove html_link from doc_info
        html_link = doc_info.pop("html_link")
        soup = self._get_soup(html_link)

        # check for error (some documents are not available)
        # A página não pode ser exibida
        if "a página não pode ser exibida" in soup.prettify().lower():
            print(f"Error getting document data: {html_link}")
            return None

        # get situation, subject and pdf_link.
        situation = soup.find("td", text="Situação:")
        if situation:
            situation = situation.find_next("td").text.strip()

        subject = soup.find("td", text="Assunto:")
        if subject:
            subject = subject.find_next("td").text.strip()
        html_link = (
            soup.find("td", text=re.compile(r"Links:"))
            .find_next("td")
            .find("a")["href"]
        )
        # add base url if not present
        if not html_link.startswith("http"):
            html_link = f"{self.base_url}/legis/M010/{html_link}"

        # <iframe name=txt_Texto_teste src='https://ww3.al.rs.gov.br/filerepository/repLegis/arquivos/DECR IMP SN 1830 S FRANCISCO.pdf' width=100% height=100% frameborder=0></iframe>

        # get text from pdf ( need to make a requst to html and get pdf link from iframe)
        soup = self._get_soup(html_link)

        # invalid norm
        if (
            "norma sem texto" in soup.prettify().lower()
            or "sem texto para exibi" in soup.prettify().lower()
        ):
            return None

        pdf_link = None
        html_string = self._get_html_string(soup)
        if html_string:
            buffer = BytesIO()
            buffer.write(html_string.encode())
            buffer.seek(0)

            text_markdown = self._get_markdown(stream=buffer)
        else:
            pdf_link = soup.find("iframe")
            if pdf_link:
                pdf_link = pdf_link["src"]
            else:
                # pdf_link may be in the form of a javascript window.open
                # get the link from the javascript
                pdf_link = re.search(r"window\.open\('([^']+)'", soup.prettify())
                pdf_link = pdf_link.group(1)

            text_markdown = self._get_markdown(pdf_link)

        if not text_markdown or not text_markdown.strip():
            print(f"Error getting markdown from pdf: {pdf_link}")
            return None

        return {
            **doc_info,
            "situation": situation,
            "subject": subject,
            "html_string": html_string,
            "text_markdown": text_markdown.strip(),
            "document_url": pdf_link if pdf_link else html_link,
        }

    def scrape_constitution(self):
        """Scrape constitution data"""
        url = "https://ww2.al.rs.gov.br/dal/LinkClick.aspx?fileticket=9p-X_3esaNg%3d&tabid=3683&mid=5358"

        text_markdown = self._get_markdown(url)
        if not text_markdown or not text_markdown.strip():
            print("Error getting markdown for state constitution")
            return None

        # save to one drive
        queue_item = {
            "year": 1989,
            "type": "Constituição Estadual",
            "title": "Constituição do Estado do Rio Grande do Sul",
            "date": "",
            "summary": "Texto constitucional de 3 de outubro de 1989 com as alterações adotadas pelas Emendas Constitucionais de n.º 1, de 1991, a 85, de 2023",
            "situation": "Sem revogação expressa",
            "text_markdown": text_markdown.strip(),
            "document_url": url,
        }

        self.queue.put(queue_item)
        self.results.append(queue_item)
        self.count += 1

        self.fetched_constitution = True

    def _scrape_year(self, year: int):
        """Scrape norms for a specific year"""
        for situation in tqdm(
            self.situations,
            desc="RIO GRANDE DO SUL | Situations",
            total=len(self.situations),
            disable=not self.verbose,
        ):

            if not self.fetched_constitution:
                self.scrape_constitution()
                continue

            # get total pages
            url = self._format_search_url(year, 1)
            soup = self._get_soup(url)

            total_pages = soup.find("img", alt="Última Página")
            if total_pages:
                total_pages = total_pages.find_parent("a")
                total_pages = int(
                    total_pages["href"].split("txtPage=")[-1].split("&")[0]
                )
            else:
                total_pages = 0  # no documents for this year

            # Get documents html links
            documents = []

            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                futures = [
                    executor.submit(
                        self._get_docs_links,
                        self._format_search_url(year, page),
                    )
                    for page in range(1, total_pages + 1)
                ]

                for future in tqdm(
                    as_completed(futures),
                    total=len(futures),
                    desc="RIO GRANDE DO SUL | Get document link",
                    disable=not self.verbose,
                ):
                    docs = future.result()
                    if docs:
                        documents.extend(docs)

            # get all norms
            results = []
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                futures = [
                    executor.submit(
                        self._get_doc_data,
                        doc_info,
                    )
                    for doc_info in documents
                ]

                for future in tqdm(
                    as_completed(futures),
                    desc="RIO GRANDE DO SUL | Get document data",
                    total=len(futures),
                    disable=not self.verbose,
                ):
                    norm = future.result()
                    if not norm:
                        continue

                    # save to one drive
                    queue_item = {
                        **norm,
                        "year": year,
                        "situation": (
                            norm["situation"] if norm.get("situation") else "Não consta"
                        ),
                    }

                    self.queue.put(queue_item)
                    results.append(queue_item)

            self.results.extend(results)
            self.count += len(results)

            if self.verbose:
                print(
                    f"Finished scraping for Year: {year} | Results: {len(results)} | Total: {self.count}"
                )
