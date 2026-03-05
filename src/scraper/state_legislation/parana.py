from urllib.parse import urljoin

import asyncio
import re
import random
from bs4 import BeautifulSoup
from playwright.async_api import Page, TimeoutError as PlaywrightTimeoutError
from tenacity import retry, stop_after_attempt, wait_exponential
from src.scraper.base.scraper import (
    StateScraper,
    DEFAULT_VALID_SITUATION,
    DEFAULT_INVALID_SITUATION,
)
from loguru import logger

TYPES = {
    "Lei": 1,
    "Lei Complementar": 3,
    "Consituição Estadual": 10,
    "Decreto": 11,
    "Emenda Constitucional": 9,
    "Resolução": 13,
    "Portaria": 14,
}

VALID_SITUATIONS = []  # Casa Civil for Parana does not have a situation field, invalid norms will be inferred from an indication in the document text (Revogado pelo | Revogada pela | Revogado por | Revogada por)

INVALID_SITUATIONS = []  # norms with these situations are invalid norms (no lon

# the reason to have invalid situations is in case we need to train a classifier to predict if a norm is valid or something else similar
SITUATIONS = VALID_SITUATIONS + INVALID_SITUATIONS


class ParanaCVScraper(StateScraper):
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
        super().__init__(
            base_url,
            types=TYPES,
            situations=SITUATIONS,
            name="PARANA",
            multiple_pages=True,
            **kwargs,
        )
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

    def _format_search_url(
        self, norm_type_id: str, year_index: int, page: int = 1
    ) -> str:
        """Format url for search request"""
        self.params["tiposAtoStr"] = norm_type_id
        self.params["tiposAtoTema"] = norm_type_id
        self.params["anoInicialAtoTema"] = year_index
        self.params["anoFinalAtoTema"] = year_index

        return f"{self.base_url}/legislacao/pesquisarAto.do?action=listar&opt=tm&indice{page}&site=1"

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=2, min=2, max=15),
        reraise=True,
    )
    async def _selenium_click_page(self, page_num: int, pw_page: Page):
        """Emulate click on page number with Playwright."""

        await self._handle_blocked_access(pw_page)

        content = await pw_page.content()
        total_records = self._regex_total_records.search(content)
        if total_records:
            total_records = int(total_records.group(1))
        else:
            logger.warning("Total records not found")
            return

        js = f"javascript:pesquisarPaginado('pesquisarAto.do?action=listar&opt=tm&indice={page_num}&totalRegistros={total_records}#resultado');"
        await pw_page.evaluate(js)
        await asyncio.sleep(5)

    async def _is_access_blocked(self, pw_page: Page) -> bool:
        """Check if access is blocked by the website"""
        content = await pw_page.content()
        if "Acesso temporariamente bloqueado" in content:
            return True
        if "ERR_TUNNEL_CONNECTION_FAILED" in content:
            logger.warning("Tunnel connection failed")
            return True
        if "Service unavailable" in content:
            logger.warning("Service unavailable")
            return True
        if "ERR_EMPTY_RESPONSE" in content:
            logger.warning("Empty response")
            return True
        if "ERR_HTTP2_SERVER_REFUSED_STREAM" in content:
            logger.warning("HTTP2 server refused stream")
            return True
        if "ERROR" in content:
            logger.error("Error")
            return True
        return False

    async def _connect_vpn(self, pw_page: Page):
        """Connect to VPN using the extension"""

        # check if premium popup appears and skip it
        try:
            skip_btn = pw_page.locator("button.premium-banner__skip.btn")
            await skip_btn.wait_for(state="visible", timeout=5000)
            if self.verbose:
                logger.info("Found premium popup, skipping it")
            await skip_btn.click()
            await asyncio.sleep(1)
        except PlaywrightTimeoutError:
            pass

        # check if dialog appears and close it
        try:
            close_btn = pw_page.locator("button.rate-us-modal__close")
            await close_btn.wait_for(state="visible", timeout=5000)
            if self.verbose:
                logger.info("Found rate us dialog, closing it")
            await close_btn.click()
            await asyncio.sleep(1)
        except PlaywrightTimeoutError:
            pass

        connect_button_selector = (
            "button.connect-button[aria-label='connection button']"
        )

        # check if already connected and if so, disconnect
        content = await pw_page.content()
        if "VPN is ON" in content:
            disconnect_btn = pw_page.locator(connect_button_selector)
            await disconnect_btn.wait_for(state="visible", timeout=10000)
            await disconnect_btn.click()
            await asyncio.sleep(3)

        # pass through the initial page, if it appears
        try:
            continue_btn = pw_page.locator(".intro-steps__btn")
            await continue_btn.wait_for(state="visible", timeout=10000)
            await continue_btn.click()
            await asyncio.sleep(1)

            continue_btn = pw_page.locator(".intro-steps__btn")
            await continue_btn.wait_for(state="visible", timeout=10000)
            await continue_btn.click()
            await asyncio.sleep(1)
        except PlaywrightTimeoutError:
            pass

        # randomly select a country
        select_country_btn = pw_page.locator(
            "button.connect-region__location[type='button']"
        )
        await select_country_btn.wait_for(state="visible", timeout=10000)
        await select_country_btn.click()

        country_list = pw_page.locator("ul.locations-view__country-list")
        await country_list.wait_for(state="visible", timeout=10000)
        countries = await country_list.locator("li.locations-view__country-item").all()

        # avoid russia and singapore because of latency
        while True:
            country = random.choice(countries)
            country_text = await country.text_content()
            if (
                "russia" not in country_text.lower()
                and "singapore" not in country_text.lower()
            ):
                break

        # some countries have sublocations, choose randomly one of them
        sublocations = await country.locator(".location-country__wrap").all()
        if sublocations:
            await country.click()
            await asyncio.sleep(1)

            try:
                sublocation_elements = await country.locator(".location-region").all()
                sublocation = random.choice(sublocation_elements)
                await sublocation.wait_for(state="visible", timeout=10000)
                await sublocation.click()
                await asyncio.sleep(1)
            except Exception as e:
                logger.error(f"Error selecting sublocation: {e}")
        else:
            await country.click()

        # Click connect button
        connect_btn = pw_page.locator(connect_button_selector)
        await connect_btn.wait_for(state="visible", timeout=10000)
        await connect_btn.click()

        # Wait for VPN to connect
        status_locator = pw_page.locator(".main-view__status")
        await status_locator.wait_for(timeout=30000)
        # Poll until "VPN is ON" appears
        for _ in range(60):
            text = await status_locator.text_content()
            if text and "VPN is ON" in text:
                return
            await asyncio.sleep(0.5)
        raise TimeoutError("VPN did not connect within timeout")

    async def _change_vpn_connection(self, pw_page: Page):
        """Change VPN connection using extension in a new page."""
        context = pw_page.context

        # close extra pages, keep the first one
        pages = context.pages
        if len(pages) > 1:
            for extra_page in pages[1:]:
                await extra_page.close()

        # open new page for extension popup
        vpn_page = await context.new_page()
        await vpn_page.goto(self.vpn_extension_page)
        await asyncio.sleep(3)

        @retry(
            stop=stop_after_attempt(3),
            wait=wait_exponential(multiplier=2, min=5, max=15),
            reraise=True,
        )
        async def _try_connect():
            await self._connect_vpn(vpn_page)

        try:
            await _try_connect()
        except Exception as e:
            logger.error(f"Failed to connect VPN after retries: {e}")

        # close the extension page
        await vpn_page.close()

    async def _handle_blocked_access(self, pw_page: Page):
        """Check if access is blocked and change vpn"""
        access_blocked = await self._is_access_blocked(pw_page)
        while access_blocked:
            try:
                await self._change_vpn_connection(pw_page)
                await asyncio.sleep(1)
                await pw_page.reload()
                access_blocked = await self._is_access_blocked(pw_page)
            except Exception as e:
                logger.error(f"Error handling blocked access: {e}")
                await asyncio.sleep(1)
                continue

    async def _fill_search_form(self, year: int, norm_type_id: int, pw_page: Page):
        """Fill the search form with the given year and norm type"""
        content = await pw_page.content()
        if "Your connection was interrupted" in content:
            logger.warning("Connection interrupted")
            await pw_page.reload()
            await asyncio.sleep(3)

        # fill the year
        year_input = pw_page.locator("#anoInicialAtoTema")
        await year_input.clear()
        await year_input.fill(str(year))

        # check the checkbox for the norm type
        norm_type_checkboxes = await pw_page.locator("#tiposAtoTema").all()
        norm_type_checkbox = None
        for checkbox in norm_type_checkboxes:
            val = await checkbox.get_attribute("value")
            if val == str(norm_type_id):
                norm_type_checkbox = checkbox
                break

        if norm_type_checkbox:
            await norm_type_checkbox.click()
            await asyncio.sleep(5)
        else:
            logger.warning(f"Norm type checkbox not found for {norm_type_id}")
            return

        # click on the search button
        search_button = pw_page.locator("#btPesquisar3")
        await search_button.click()
        await asyncio.sleep(5)

        # wait until the page is loaded
        content = await pw_page.content()
        soup = BeautifulSoup(content, "html.parser")

        while (
            not soup.find("table", id="list_tabela")
            and not soup.find(
                "td", class_="msg_sucesso", text="Nenhum registro encontrado."
            )
            and not soup.find(
                "td", class_="msg_erro", text="Ocorreram problemas na listagem"
            )
        ):
            blocked_access = await self._is_access_blocked(pw_page)
            if blocked_access:
                return
            await asyncio.sleep(5)
            content = await pw_page.content()
            soup = BeautifulSoup(content, "html.parser")

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=2, min=2, max=15),
        reraise=True,
    )
    async def _search_norms(
        self, url: str, year: int, norm_type_id: int, pw_page: Page
    ) -> str:
        """Search for norms in the given year and norm type"""
        retries = 6
        page_loaded = False
        while not page_loaded and retries > 0:
            try:
                await pw_page.goto(url, wait_until="domcontentloaded")
                page_loaded = True
            except Exception as e:
                logger.error(f"Error getting url: {e}")
                await asyncio.sleep(5)
                retries -= 1

        if retries == 0:
            logger.error("Failed to load page after 6 retries")
            return

        if not page_loaded:
            retries = 6
            await self._change_vpn_connection(pw_page)
            while not page_loaded and retries > 0:
                try:
                    await pw_page.goto(url, wait_until="domcontentloaded")
                    page_loaded = True
                except Exception as e:
                    logger.error(f"Error getting url: {e}")
                    await asyncio.sleep(5)
                    retries -= 1

        await self._handle_blocked_access(pw_page)
        await self._fill_search_form(year, norm_type_id, pw_page)

        await asyncio.sleep(5)
        return await pw_page.content()

    async def _get_docs_links(
        self, url: str, year: int, norm_type_id: int, page_num: int
    ) -> list:
        """Get documents html links from given page."""

        pw_page = await self._get_available_page()

        if page_num > 1:
            current_url = pw_page.url
            while f"indice={page_num}" not in current_url:
                await self._search_norms(url, year, norm_type_id, pw_page)
                await self._selenium_click_page(page_num, pw_page)

                await self._handle_blocked_access(pw_page)
                await asyncio.sleep(5)
                current_url = pw_page.url
        else:
            current_url = pw_page.url
            while "#resultado" not in current_url:
                await self._search_norms(url, year, norm_type_id, pw_page)
                current_url = pw_page.url

        content = await pw_page.content()
        soup = BeautifulSoup(content, "html.parser")

        docs = []

        table = soup.find("table", id="list_tabela")
        items = table.find_all("tr", class_=self._regex_list_items)

        for item in items:
            tds = item.find_all("td")

            id = tds[0].find("a", href=True)
            id = id["href"].split("'")[1]
            if not id:
                logger.warning("ID not found")

            title = tds[1].text.strip()
            summary = tds[2].text.strip()
            date = tds[3].text.strip()

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

        await self._release_page(pw_page)

        return docs

    def _infer_invalid_situation(self, soup: BeautifulSoup) -> str:
        """Infer invalid situation from document text"""
        text = soup.get_text()
        match = self._regex_invalid_situations.search(text)
        if match:
            return DEFAULT_INVALID_SITUATION

        return None

    async def _get_doc_data(self, doc_info: dict) -> dict:
        """Get document data from given doc info"""
        html_link = doc_info.pop("html_link")

        if self._is_already_scraped(html_link, doc_info.get("title", "")):
            return None

        pw_page = await self._get_available_page()

        norm_text_tag = None
        while not norm_text_tag:
            await self._handle_blocked_access(pw_page)
            soup = await self._browser_get_soup(html_link, pw_page)

            if "ERR_TIMED_OUT" in soup.prettify():
                logger.warning("Connection timed out, refreshing page")
                await pw_page.reload()
                await asyncio.sleep(3)

            if "ERR_EMPTY_RESPONSE" in soup.prettify():
                logger.warning("Empty response, refreshing page")
                await pw_page.reload()
                await asyncio.sleep(3)

            norm_text_tag = soup.find("form", attrs={"name": "pesquisarAtoForm"})

            await asyncio.sleep(5)

        table = norm_text_tag.find("table", id="list_tabela")
        if table:
            table.decompose()

        html_string = norm_text_tag.prettify().replace("\n ANEXOS:", "").strip()
        html_string = html_string.replace("javascript:listarAssinaturas();", "")

        situation = self._infer_invalid_situation(soup)
        if not situation:
            situation = DEFAULT_VALID_SITUATION

        await self._release_page(pw_page)

        # since we're getting the form tag, need to add the html and body tags to make it a valid html for markitdown
        html_string = f"<html><body>{html_string}</body></html>"

        # get text markdown
        text_markdown = (await self._get_markdown(html_content=html_string)).strip()

        doc_info["html_string"] = html_string
        doc_info["text_markdown"] = text_markdown
        doc_info["document_url"] = html_link
        doc_info["situation"] = situation
        doc_info["_raw_content"] = html_string.encode("utf-8")
        doc_info["_content_extension"] = ".html"

        saved = await self._save_doc_result(doc_info)
        if saved is not None:
            doc_info = saved

        return doc_info

    async def _scrape_type(
        self, norm_type: str, norm_type_id: int, year: int
    ) -> list[dict]:
        """Scrape norms for a specific type and year"""
        pw_page = await self._get_available_page()
        url = self._format_search_url(norm_type_id, year)
        await asyncio.sleep(5)
        page_html = await self._search_norms(url, year, norm_type_id, pw_page)
        soup = BeautifulSoup(page_html, "html.parser")

        await self._release_page(pw_page)

        # get total pages
        total_pages = self._regex_total_pages.search(soup.get_text())
        if total_pages:
            total_pages = int(total_pages.group(1))
        else:
            return []

        # Get documents html links
        tasks = [
            self._get_docs_links(
                url,
                year,
                norm_type_id,
                page,
            )
            for page in range(1, total_pages + 1)
        ]
        valid_results = await self._gather_results(
            tasks,
            context={"year": year, "type": norm_type, "situation": "N/A"},
            desc=f"PARANA | {norm_type} | get_docs_links",
        )
        documents = []
        for result in valid_results:
            documents.extend(result)

        # get all norms
        tasks = [self._get_doc_data(doc_info) for doc_info in documents]
        valid_results = await self._gather_results(
            tasks,
            context={"year": year, "type": norm_type, "situation": "N/A"},
            desc=f"PARANA | {norm_type}",
        )
        results = []
        for result in valid_results:
            queue_item = {"year": year, "type": norm_type, **result}
            results.append(queue_item)

        if self.verbose:
            logger.info(
                f"Finished scraping for Year: {year} | Type: {norm_type} | Results: {len(results)} | Total: {self.count}"
            )

        return results

    # _scrape_year uses default from BaseScraper
