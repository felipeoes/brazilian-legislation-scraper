from urllib.parse import urljoin

import re
import requests
import random
from io import BytesIO
from bs4 import BeautifulSoup
from selenium.webdriver import Chrome
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
from tqdm import tqdm
import time
from src.scraper.base.scraper import (
    BaseScaper,
    DEFAULT_VALID_SITUATION,
    DEFAULT_INVALID_SITUATION,
    retry,
)

TYPES = {
    "Lei": 1,
    "Lei Complementar": 3,
    "Consituição Estadual": 10,
    "Decreto": 11,
    "Emenda Constitucional": 9,
    "Resolução": 13,
    "Portaria": 14,
}

VALID_SITUATIONS = (
    []
)  # Casa Civil for Parana does not have a situation field, invalid norms will be inferred from an indication in the document text (Revogado pelo | Revogada pela | Revogado por | Revogada por)

INVALID_SITUATIONS = []  # norms with these situations are invalid norms (no lon

# the reason to have invalid situations is in case we need to train a classifier to predict if a norm is valid or something else similar
SITUATIONS = VALID_SITUATIONS + INVALID_SITUATIONS

lock = Lock()


class ParanaCVScraper(BaseScaper):
    """Webscraper for Parana do Sul state legislation website (https://www.legislacao.pr.gov.br)

    Example search request: https://www.legislacao.pr.gov.br/legislacao/pesquisarAto.do?action=listar&opt=tm&indice=1&site=1

    payload = {
        pesquisou: true
        opcaoAno: 2
        opcaoNro: 1
        optPesquisa: tm
        tiposAtoStr: 1
        site: 1
        codigoTipoAto:
        tipoOrdenacao:
        ordAsc: false
        optTexto: 2
        texto:
        anoInicialAto:
        anoFinalAto:
        nroInicialAto:
        nroFinalAto:
        tipoAto:
        nroAto:
        anoAto:
        tema: 0
        anoInicialAtoTema: 2020
        anoFinalAtoTema: 2020
        nroInicialAtoTema:
        nroFinalAtoTema:
        tiposAtoTema: 1
    }
    """

    def __init__(
        self,
        base_url: str = "https://www.legislacao.pr.gov.br",
        **kwargs,
    ):
        super().__init__(base_url, types=TYPES, situations=SITUATIONS, **kwargs)
        self.docs_save_dir = self.docs_save_dir / "PARANA"
        self.params = {
            "pesquisou": True,
            "opcaoAno": 2,
            "opcaoNro": 1,
            "optPesquisa": "tm",
            "tiposAtoStr": "",
            "site": 1,
            "codigoTipoAto": None,
            "tipoOrdenacao": None,
            "ordAsc": False,
            "optTexto": 2,
            "texto": None,
            "anoInicialAto": None,
            "anoFinalAto": None,
            "nroInicialAto": None,
            "nroFinalAto": None,
            "tipoAto": None,
            "nroAto": None,
            "anoAto": None,
            "tema": 0,
            "anoInicialAtoTema": "",
            "anoFinalAtoTema": "",
            "nroInicialAtoTema": None,
            "nroFinalAtoTema": None,
        }
        self._regex_list_items = re.compile(r"list_cor_(sim|nao)")
        self._regex_invalid_situations = re.compile(
            r"(Revogado pelo|Revogada pela|Revogado por|Revogada por)"
        )
        self._regex_total_pages = re.compile(r"Página \d+ de (\d+)")
        self._regex_total_records = re.compile(r"Total de (\d+) registros")
        self._initialize_saver()

    def _format_search_url(
        self,
        norm_type_id: str,
        year_index: int,
        page: int = 1
    ) -> str:
        """Format url for search request"""
        self.params["tiposAtoStr"] = norm_type_id
        self.params["tiposAtoTema"] = norm_type_id
        self.params["anoInicialAtoTema"] = year_index
        self.params["anoFinalAtoTema"] = year_index

        return f"{self.base_url}/legislacao/pesquisarAto.do?action=listar&opt=tm&indice{page}&site=1"

    def _selenium_click_page(self, page: int, driver: Chrome):
        """Emulate click on page number with selenium, using javascript.

        The page url will be built using the total number of records and the page number in the following format:
        javascript:pesquisarPaginado('pesquisarAto.do?action=listar&opt=tm&indice={page}&totalRegistros={total_records}#resultado');
        """

        retries = 3
        while retries > 0:
            try:
                self._handle_blocked_access(driver)

                # find the total number of records (totalRegistros=474#resultado)
                total_records = self._regex_total_records.search(driver.page_source)
                if total_records:
                    total_records = int(total_records.group(1))
                else:
                    print("Total records not found")
                    return

                # create javascript to click on page
                js = f"javascript:pesquisarPaginado('pesquisarAto.do?action=listar&opt=tm&indice={page}&totalRegistros={total_records}#resultado');"

                driver.execute_script(js)
                time.sleep(5)

                break
            except Exception:
                time.sleep(2)
                retries -= 1

    def _is_access_blocked(self, driver: Chrome) -> bool:
        """Check if access is blocked by the website"""
        if "Acesso temporariamente bloqueado" in driver.page_source:
            # print("Access temporarily blocked")
            return True

        if "ERR_TUNNEL_CONNECTION_FAILED" in driver.page_source:
            print("Tunnel connection failed")
            return True

        if "Service unavailable" in driver.page_source:
            print("Service unavailable")
            return True

        if "ERR_EMPTY_RESPONSE" in driver.page_source:
            print("Empty response")
            return True

        if "ERR_HTTP2_SERVER_REFUSED_STREAM" in driver.page_source:
            print("HTTP2 server refused stream")
            return True

        if "ERROR" in driver.page_source:
            print("Error")
            return True

        return False

    def _connect_vpn(self, driver: Chrome):
        """Connect to VPN using the extension"""

        # check if premium popup appears and skip it
        try:
            skip_button = WebDriverWait(driver, 5).until(
                EC.element_to_be_clickable(
                    (By.CSS_SELECTOR, "button.premium-banner__skip.btn")
                )
            )
            print("Found premium popup, skipping it")
            skip_button.click()
            time.sleep(1)
        except Exception:
            pass

        # check if dialog appears and close it
        try:
            close_button = WebDriverWait(driver, 5).until(
                EC.element_to_be_clickable(
                    (By.CSS_SELECTOR, "button.rate-us-modal__close")
                )
            )
            print("Found rate us dialog, closing it")
            close_button.click()
            time.sleep(1)
        except Exception:
            pass

        connect_button_selector = (
            "button.connect-button[aria-label='connection button']"
        )

        # check if already connected and if so, disconnect
        if "VPN is ON" in driver.page_source:
            disconnect_button = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable(
                    (
                        By.CSS_SELECTOR,
                        connect_button_selector,
                    )
                )
            )
            disconnect_button.click()
            time.sleep(3)

        # pass trough the initial page, if it appears
        try:
            continue_button = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.CLASS_NAME, "intro-steps__btn"))
            )
            continue_button.click()
            time.sleep(1)

            # click again the same button
            continue_button = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, ".intro-steps__btn"))
            )
            continue_button.click()
            time.sleep(1)
        except Exception:
            pass

        # randomly select a country
        select_country_button = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable(
                (
                    By.CSS_SELECTOR,
                    "button.connect-region__location[type='button']",
                )
            )
        )
        select_country_button.click()

        country_list = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located(
                (By.CSS_SELECTOR, "ul.locations-view__country-list")
            )
        )
        countries = country_list.find_elements(
            By.CSS_SELECTOR, "li.locations-view__country-item"
        )

        # avoid russia and singapore because of latency
        while True:
            country = random.choice(countries)
            if (
                "russia" not in country.text.lower()
                and "singapore" not in country.text.lower()
            ):
                break

        # print(f"Selected country: {country.text}")

        # some countries have sublocations, choose randomly one of them
        if country.find_elements(By.CSS_SELECTOR, ".location-country__wrap"):
            country.click()
            time.sleep(1)

            try:
                sublocation = random.choice(
                    country.find_elements(By.CSS_SELECTOR, ".location-region")
                )

                # wait for sublocation to be clickable
                sublocation = WebDriverWait(driver, 10).until(
                    EC.element_to_be_clickable(sublocation)
                )

                sublocation.click()
                time.sleep(1)
            except Exception as e:
                print(f"Error selecting sublocation: {e}")
                # if there is no sublocation, probably the sublocation is already selected
                pass
        else:
            country.click()

        # Click connect button
        connect_button = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable(
                (
                    By.CSS_SELECTOR,
                    connect_button_selector,
                )
            )
        )
        connect_button.click()

        status_element_selector = ".main-view__status"  # Based on your HTML
        WebDriverWait(driver, 30).until(
            EC.text_to_be_present_in_element(
                (By.CSS_SELECTOR, status_element_selector), "VPN is ON"
            )
        )

    def _change_vpn_connection(self, driver: Chrome):
        # check if driver have more than one window and close the remaining ones
        if len(driver.window_handles) > 1:
            for window in driver.window_handles[1:]:
                driver.switch_to.window(window)
                driver.close()

        driver.switch_to.window(driver.window_handles[0])

        # open new window and switch to it
        driver.execute_script("window.open('');")
        driver.switch_to.window(driver.window_handles[-1])

        # go to extension popup page
        driver.get(self.vpn_extension_page)
        time.sleep(3)

        retries = 3

        while retries > 0:
            try:
                self._connect_vpn(driver)
                break
            except Exception as e:
                print(f"Error connecting to VPN: {e}")
                time.sleep(5)
                retries -= 1

        # switch back to the main window
        driver.close()
        driver.switch_to.window(driver.window_handles[0])

    def _handle_blocked_access(self, driver: Chrome):
        """Check if access is blocked and change vpn"""

        access_blocked = self._is_access_blocked(driver)
        while access_blocked:
            try:
                self._change_vpn_connection(driver)
                time.sleep(1)

                driver.refresh()
                access_blocked = self._is_access_blocked(driver)
            except Exception as e:
                print(f"Error handling blocked access: {e}")
                time.sleep(1)
                continue

    def _selenium_fill_form(self, year: int, norm_type_id: int, driver: Chrome):
        """Fill the search form with the given year and norm type"""

        if "Your connection was interrupted" in driver.page_source:
            print("Connection interrupted")
            driver.refresh()
            time.sleep(3)

        # fill the year
        year_input = driver.find_element(By.ID, "anoInicialAtoTema")
        year_input.clear()
        year_input.send_keys(year)

        # check the checkbox for the norm type
        norm_type_checkbox = driver.find_elements(By.ID, "tiposAtoTema")
        norm_type_checkbox = [
            checkbox
            for checkbox in norm_type_checkbox
            if checkbox.get_attribute("value") == str(norm_type_id)
        ]
        if norm_type_checkbox:
            norm_type_checkbox[0].click()
            time.sleep(5)
        else:
            print(f"Norm type checkbox not found for {norm_type_id}")
            return

        # click on the search button
        search_button = driver.find_element(By.ID, "btPesquisar3")
        search_button.click()
        time.sleep(5)

        # wait until the page is loaded
        soup = BeautifulSoup(driver.page_source, "html.parser")

        # <td class="msg_erro">Ocorreram problemas na listagem de 'TipoAto'.</td>
        while (
            not soup.find("table", id="list_tabela")
            and not soup.find(
                "td", class_="msg_sucesso", text="Nenhum registro encontrado."
            )
            and not soup.find(
                "td", class_="msg_erro", text="Ocorreram problemas na listagem"
            )
        ):
            blocked_access = self._is_access_blocked(driver)
            if blocked_access:
                return
            time.sleep(5)
            soup = BeautifulSoup(driver.page_source, "html.parser")

    @retry(max_retries=3)
    def _search_norms(
        self,
        url: str,
        year: int,
        norm_type_id: int,
        driver: Chrome
    ) -> str:
        """Search for norms in the given year and norm type"""
        retries = 6
        page_loaded = False
        while not page_loaded and retries > 0:
            try:
                driver.get(url)
                page_loaded = True
            except Exception as e:
                print(f"Error getting url: {e}")
                time.sleep(5)
                retries -= 1

        if retries == 0:
            print("Failed to load page after 6 retries")
            return

        if not page_loaded:
            # try changing vpn
            retries = 6
            self._change_vpn_connection(driver)
            while not page_loaded and retries > 0:
                try:
                    driver.get(url)
                    page_loaded = True
                except Exception as e:
                    print(f"Error getting url: {e}")
                    time.sleep(5)
                    retries -= 1

        self._handle_blocked_access(driver)
        self._selenium_fill_form(year, norm_type_id, driver)

        time.sleep(
            5
        )  # need to have big wait times to avoid being blocked by the website
        return driver.page_source

    def _get_docs_links(
        self,
        url: str,
        year: int,
        norm_type_id: int,
        page: int
    ) -> list:
        """Get documents html links from given page.
        Returns a list of dicts with keys 'id', 'title', 'summary', 'date', 'html_link'
        """

        # get available selenium driver
        driver = self._get_available_driver()

        if page > 1:
            while f"indice={page}" not in driver.current_url:
                self._search_norms(url, year, norm_type_id, driver)
                self._selenium_click_page(page, driver)

                self._handle_blocked_access(driver)
                time.sleep(5)
        else:
            while "#resultado" not in driver.current_url:
                self._search_norms(url, year, norm_type_id, driver)

        soup = BeautifulSoup(driver.page_source, "html.parser")

        docs = []

        table = soup.find("table", id="list_tabela")
        items = table.find_all("tr", class_=self._regex_list_items)

        for item in items:
            tds = item.find_all("td")

            id = tds[0].find("a", href=True)
            id = id["href"].split("'")[1]
            if not id:
                print("ID not found")

            title = tds[1].text.strip()
            summary = tds[2].text.strip()
            date = tds[3].text.strip()

            # html_link must be built from the id, in the following format:
            # https://www.legislacao.pr.gov.br/legislacao/pesquisarAto.do?action=exibir&codAto=234748

            html_link = f"/legislacao/pesquisarAto.do?action=exibir&codAto={id}"
            html_link = urljoin(self.base_url, html_link)

            docs.append(
                {
                    "id": id,
                    "title": title,
                    "summary": summary,
                    "date": date,
                    "html_link": html_link,
                }
            )

        self._release_driver(driver)

        return docs

    def _infer_invalid_situation(self, soup: BeautifulSoup) -> str:
        """Infer invalid situation from document text"""
        text = soup.get_text()
        match = self._regex_invalid_situations.search(text)
        if match:
            return DEFAULT_INVALID_SITUATION

        return None

    def _get_doc_data(self, doc_info: dict) -> dict:
        """Get document data from given doc info"""

        # get available selenium driver
        driver = self._get_available_driver()

        # remove html_link from doc_info
        html_link = doc_info.pop("html_link")

        norm_text_tag = None
        while not norm_text_tag:
            self._handle_blocked_access(driver)
            soup = self._selenium_get_soup(html_link, driver)

            if "ERR_TIMED_OUT" in soup.prettify():
                print("Connection timed out, refreshing page")
                driver.refresh()
                time.sleep(3)

            if "ERR_EMPTY_RESPONSE" in soup.prettify():
                print("Empty response, refreshing page")
                driver.refresh()
                time.sleep(3)

            # norm text will be the form name="pesquisarAtoForm"
            norm_text_tag = soup.find("form", attrs={"name": "pesquisarAtoForm"})

            time.sleep(
                5
            )  # need to have big wait times to avoid being blocked by the website

        # with lock:
        #     soup = self._selenium_get_soup(html_link, driver)
        #     self._handle_blocked_access(driver)

        #     time.sleep(
        #         5
        #     )  # need to have big wait times to avoid being blocked by the website
        #     soup = BeautifulSoup(driver.page_source, "html.parser")

        # remove table id="list_tabela" and "\n ANEXOS:" from the text
        table = norm_text_tag.find("table", id="list_tabela")
        if table:
            table.decompose()

        html_string = norm_text_tag.prettify().replace("\n ANEXOS:", "").strip()

        # inferr situation from text
        situation = self._infer_invalid_situation(soup)
        if not situation:
            situation = DEFAULT_VALID_SITUATION

        self._release_driver(driver)

        # since we're getting the form tag, need to add the html and body tags to make it a valid html for markitdown
        html_string = f"<html><body>{html_string}</body></html>"

        # get text markdown
        buffer = BytesIO()
        buffer.write(html_string.encode())
        buffer.seek(0)

        text_markdown = self._get_markdown(stream=buffer).strip()

        doc_info["html_string"] = html_string
        doc_info["text_markdown"] = text_markdown
        doc_info["document_url"] = html_link
        doc_info["situation"] = situation

        return doc_info


    def _scrape_year(self, year: int):
        """Scrape norms for a specific year"""
        for norm_type, norm_type_id in tqdm(
            self.types.items(),
            desc=f"PARANA | Year: {year} | Types",
            total=len(self.types),
            disable=not self.verbose,
        ):
            driver = self._get_available_driver()
            url = self._format_search_url(norm_type_id, year)
            time.sleep(
                5
            )  # need to have big wait times to avoid being blocked by the website
            page_html = self._search_norms(url, year, norm_type_id, driver)
            soup = BeautifulSoup(page_html, "html.parser")

            self._release_driver(driver)

            # get total pages
            total_pages = self._regex_total_pages.search(soup.get_text())
            if total_pages:
                total_pages = int(total_pages.group(1))
            else:
                continue

            # Get documents html links
            documents = []
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                futures = [
                    executor.submit(
                        self._get_docs_links,
                        url,
                        year,
                        norm_type_id,
                        page,
                    )
                    for page in range(1, total_pages + 1)
                ]

                for future in tqdm(
                    as_completed(futures),
                    total=len(futures),
                    desc="PARANA | Get document link",
                    disable=not self.verbose,
                ):
                    docs = future.result()
                    if docs:
                        documents.extend(docs)

            # get all norms
            results = []
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                futures = [
                    executor.submit(self._get_doc_data, doc_info)
                    for doc_info in documents
                ]

                for future in tqdm(
                    as_completed(futures),
                    desc="PARANA | Get document data",
                    total=len(futures),
                    disable=not self.verbose,
                ):
                    norm = future.result()
                    if not norm:
                        continue

                    # save to one drive
                    queue_item = {"year": year, "type": norm_type, **norm}

                    self.queue.put(queue_item)
                    results.append(queue_item)

            self.results.extend(results)
            self.count += len(results)

            if self.verbose:
                print(
                    f"Finished scraping for Year: {year} | Type: {norm_type} | Results: {len(results)} | Total: {self.count}"
                )
