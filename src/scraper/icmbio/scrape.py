import random
import asyncio
from loguru import logger
from typing import Optional
from playwright.async_api import (
    Page,
    TimeoutError as PlaywrightTimeoutError,
    Error as PlaywrightError,
)
from tenacity import (
    AsyncRetrying,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
)
from src.scraper.base.scraper import BaseScraper

TYPES = {
    "Instrução Normativa": "INSTRUÇÕES NORMATIVAS",
    "Portaria": "PORTARIAS",
    "Outros Atos": "OUTROS ATOS",
}

VALID_SITUATIONS = [
    "em vigência",
    "em vigência com alteração",
    "não consta",  # obs: for this we need to match (Blank) or (Em branco) in the powerbi filter
]
INVALID_SITUATIONS = ["revogado", "vencido", "sem efeito"]

SITUATIONS = VALID_SITUATIONS + INVALID_SITUATIONS


class ICMBioScraper(BaseScraper):
    """Webscraper for ICMBio (Instituto Chico Mendes de Conservação da Biodiversidade) PowerBI dashboard

    PowerBI URL: https://app.powerbi.com/view?r=eyJrIjoiMGQ0ODRhY2QtYThmNy00NmYwLWFkOGYtOWJmZDU0ODZlZWUzIiwidCI6ImMxNGUyYjU2LWM1YmMtNDNiZC1hZDljLTQwOGNmNmNjMzU2MCJ9
    Year of oldest document: 2000
    """

    def __init__(
        self,
        base_url: str = "https://app.powerbi.com/view?r=eyJrIjoiMGQ0ODRhY2QtYThmNy00NmYwLWFkOGYtOWJmZDU0ODZlZWUzIiwidCI6ImMxNGUyYjU2LWM1YmMtNDNiZC1hZDljLTQwOGNmNmNjMzU2MCJ9",
        **kwargs,
    ):
        super().__init__(
            base_url,
            name="ICMBIO",
            types=TYPES,
            situations=SITUATIONS,
            use_browser=True,
            multiple_pages=True,
            **kwargs,
        )

    async def _wait_and_sort_table(
        self, page: Page, sort_ascending: bool = False
    ) -> bool:
        """Wait for the table to load and sort 'Data Publicação' in the desired direction.

        Args:
            sort_ascending: If True, sort oldest-first (ascending). If False, sort newest-first (descending).
        """
        try:
            # Wait for any column header to be present to ensure the table structure is loaded
            await page.wait_for_selector('[role="columnheader"]')

            headers = await page.locator('[role="columnheader"]').all()
            date_header = None
            for header in headers:
                text = await header.inner_text()
                if (
                    "publicacao" in text.strip().lower()
                    or "data publicação" in text.strip().lower()
                ):
                    date_header = header
                    break

            if date_header is None:
                logger.error("Could not find 'Data Publicação' column header")
                return False

            # Read the current sort state from the aria-sort attribute
            current_sort = await date_header.get_attribute("aria-sort")
            desired_sort = "ascending" if sort_ascending else "descending"

            if current_sort != desired_sort:
                # Click to toggle sort direction (PowerBI toggles between ascending/descending)
                await date_header.click()
                await asyncio.sleep(3)  # Wait for the table to re-render

                # Verify the sort changed
                new_sort = await date_header.get_attribute("aria-sort")
                if new_sort != desired_sort:
                    logger.warning(
                        f"Sort did not change to {desired_sort} (got {new_sort}), clicking again"
                    )
                    await date_header.click()
                    await asyncio.sleep(3)

            return True

        except PlaywrightTimeoutError:
            logger.warning("Timeout waiting for table headers to load")
            return False
        except Exception as e:
            logger.error(f"Error while trying to sort the table: {e}")
            return False

    async def _select_situation(self, page: Page, situation: str) -> bool:
        """Interact with the PowerBI 'Situação' slicer to filter by the target situation."""
        try:
            # The visual-modern container that houses the ato_situacao slicer
            # (includes both the header with "Clear selections" and the dropdown)
            slicer_container = page.locator(
                'visual-modern:has(div[aria-label="condicao"])'
            )
            await slicer_container.wait_for(state="visible")

            # The dropdown element inside the slicer. Hover to expose the "Clear selections" button and click it to reset filter
            slicer_visual = slicer_container.locator('div[aria-label="condicao"]')
            await slicer_container.hover()
            clear_button = slicer_container.locator(
                'span[aria-label="Clear selections"]'
            )
            if await clear_button.is_visible():
                await clear_button.click()
                await asyncio.sleep(3)  # Wait for table to reset

            await slicer_visual.click()
            await asyncio.sleep(3)

            # Checkbox identifier mapping
            # "não consta" will map to "(Blank)" or "(Em branco)" depending on locale. Let's try "(Blank)" first as seen in the subagent.
            target_title = situation
            if situation == "não consta":
                target_title = "(Blank)"

            # Try to click the exact checkbox
            checkbox_locator = page.locator(
                f'div.slicerItemContainer[title="{target_title}"]'
            )
            if await checkbox_locator.count() == 0 and situation == "não consta":
                # Fallback to Portuguese empty text
                target_title = "(Em branco)"
                checkbox_locator = page.locator(
                    f'div.slicerItemContainer[title="{target_title}"]'
                )

            if await checkbox_locator.count() > 0:
                await checkbox_locator.first.click()
            else:
                logger.warning(f"Could not find checkbox for {target_title} in slicer.")
                # close dropdown and abort
                await slicer_visual.hover()
                return False

            # Click outside to close the dropdown
            # We can click anywhere outside, e.g. the visual header
            # Wait for table to reload its elements based on the filter
            await page.mouse.click(10, 10)
            await asyncio.sleep(3)

            return True

        except PlaywrightTimeoutError:
            logger.warning("Timeout interacting with the 'Situação' slicer.")
            return False
        except Exception as e:
            logger.error(f"Error while trying to select situation {situation}: {e}")
            return False

    async def _scrape_table_rows(
        self,
        scrollable_container,
        target_year: int,
        sort_ascending: bool = False,
    ) -> list:
        """Scrape rows from the PowerBI table using virtual scrolling for the target_year.

        Args:
            sort_ascending: If True, table is sorted oldest-first so we skip rows
                            with year < target and stop when year > target.
                            If False, table is newest-first (default).
        """
        scraped_rows_set = set()
        final_data_list = []
        stagnation_counter = 0
        reached_boundary = False

        while stagnation_counter < 3 and not reached_boundary:
            rows = await scrollable_container.locator('[role="row"]').all()
            rows_before_iteration = len(scraped_rows_set)

            for row in rows:
                try:
                    cells = await row.locator('[role="gridcell"]').all()
                    if len(cells) < 12:
                        logger.warning(
                            f"Row with insufficient cells found (expected at least 12, got {len(cells)}). Skipping row."
                        )
                        continue

                    # 0: Row Selection
                    # 1: Situação
                    # 2: Ato (Title)
                    # 3: Ementa (Summary)
                    # 4: Condicionador
                    # 5: Assunto
                    # 6: Objeto
                    # 7: UORG
                    # 8: publicacao (Date)
                    # 9: instrumento
                    # 10: Processo SEI
                    # 11: Link DOU

                    situation_text = await cells[1].inner_text()
                    situation_text = situation_text.strip()
                    if not situation_text or situation_text in [
                        "(Em branco)",
                        "(Blank)",
                    ]:
                        situation_text = "não consta"

                    date_text = await cells[8].inner_text()
                    subject_text = await cells[5].inner_text()
                    summary_text = await cells[3].inner_text()
                    title_text = await cells[2].inner_text()

                    row_key = f"{date_text}|{title_text}"

                    if row_key in scraped_rows_set:
                        continue

                    scraped_rows_set.add(row_key)

                    try:
                        # Extract year from the end of the date string (e.g. 1/29/2026 -> 2026)
                        row_year = int(date_text.strip().split("/")[-1])
                    except ValueError:
                        continue

                    # Skip/stop logic depends on sort direction:
                    #   Descending (newest-first): skip year > target, stop at year < target
                    #   Ascending  (oldest-first): skip year < target, stop at year > target
                    if sort_ascending:
                        skip_year = row_year < target_year
                        stop_year = row_year > target_year
                    else:
                        skip_year = row_year > target_year
                        stop_year = row_year < target_year

                    if skip_year:
                        continue
                    elif stop_year:
                        reached_boundary = True
                        break

                    link_element = cells[11].locator("a")
                    if await link_element.count() > 0:
                        document_url = await link_element.get_attribute("href")
                    else:
                        document_url = await cells[11].inner_text()

                    # Also try checking text from the HTML fallback since innerHTML can sometimes contain the raw URL text.
                    # As sometimes the url is printed directly as text instead of an anchor tag.
                    if not document_url or "http" not in document_url:
                        document_url = await cells[11].inner_text()
                        document_url = document_url.strip()

                    if not document_url:
                        logger.warning(
                            f"No link found in row: {row_key}. Skipping document."
                        )
                        continue

                    if (
                        "in.gov.br/web/dou" not in document_url
                        and "in.gov.br/en/web/dou" not in document_url
                    ):
                        continue

                    # Determine type from Title
                    lower_title = title_text.lower()
                    if "instrução normativa" in lower_title or lower_title.startswith(
                        "in "
                    ):
                        doc_type = "Instrução Normativa"
                    elif "portaria" in lower_title:
                        doc_type = "Portaria"
                    else:
                        doc_type = "Outros Atos"

                    row_data = {
                        "year": str(row_year),
                        "title": title_text,
                        "summary": summary_text,
                        "type": doc_type,
                        "document_url": document_url,
                        "situation": situation_text,
                        "subject": subject_text,
                    }
                    final_data_list.append(row_data)
                except Exception as e:
                    logger.error(f"Error processing row: {e}")
                    continue

            # Scroll to load more rows
            if not reached_boundary and rows:
                last_row = rows[-1]
                await last_row.scroll_into_view_if_needed()
                await asyncio.sleep(0.5)

                rows_after_iteration = len(scraped_rows_set)
                if rows_after_iteration == rows_before_iteration:
                    stagnation_counter += 1
                else:
                    stagnation_counter = 0
            else:
                break

        return final_data_list

    async def _scrape_situation(self, year: int, situation: str) -> list:
        """Get document links from PowerBI virtual scroll table for a specific year and situation.

        Acquires a dedicated page from the pool, performs all browser interactions
        on that page, then releases it — enabling safe concurrent calls.
        """
        if not self.browser_service:
            raise RuntimeError("Browser service is not initialized.")

        page = await self.browser_service.get_available_page()
        await page.set_viewport_size({"width": 4000, "height": 1080})
        try:
            # Stagger startup to avoid overwhelming PowerBI and the local Chromium instance
            await asyncio.sleep(random.uniform(0.5, 5.0))
            await page.goto(self.base_url, wait_until="load", timeout=60000)

            # Wait for the scrollable container
            try:
                await page.wait_for_selector("div.mid-viewport")
                scrollable_container = page.locator("div.mid-viewport")
            except PlaywrightTimeoutError:
                logger.error("Timed out waiting for the table container to load.")
                return []

            if not await self._select_situation(page, situation):
                logger.error(
                    f"Failed to select situation {situation}. Aborting extraction for this page."
                )
                return []

            # Sort ascending for older years (target near the top) and
            # descending for newer years (target near the top)
            midpoint = (self.year_start + self.year_end) // 2
            sort_ascending = year <= midpoint

            if not await self._wait_and_sort_table(page, sort_ascending=sort_ascending):
                logger.error("Failed to wait and sort table.")
                return []

            # Scrape the table rows
            final_data_list = await self._scrape_table_rows(
                scrollable_container, year, sort_ascending=sort_ascending
            )

            # Assign situation explicitly to fallback just in case the extraction failed
            for item in final_data_list:
                if not item.get("situation"):
                    item["situation"] = situation

            return final_data_list
        except Exception as e:
            logger.error(f"Error scraping year {year} situation {situation}: {e}")
            return []
        finally:
            self.browser_service.release_page(page)

    async def _get_doc_data(self, doc_info: dict) -> Optional[dict]:
        """Get document data from norm dict. The document_url is already in the doc_info."""

        document_url = doc_info.get("document_url")
        doc_title = doc_info.get("title", "Sem título")
        year = doc_info.get("year", "")
        situation = doc_info.get("situation", "")
        doc_type = doc_info.get("type", "")

        if not document_url:
            logger.warning(
                f"Invalid document URL for document titled '{doc_title}', skipping document."
            )
            await self._save_doc_error(
                title=doc_title,
                year=year,
                situation=situation,
                norm_type=doc_type,
                html_link="",
                error_message="Invalid or missing document URL",
            )
            return None

        # Use browser to fetch DOU pages (avoids Azion CDN 403 rate-limiting)
        # Retry up to 3 times if div.texto-dou is not found (transient 403 / page load issue)
        soup = None
        text_div = None

        try:
            async for attempt in AsyncRetrying(
                retry=retry_if_exception_type(
                    (ValueError, PlaywrightError, PlaywrightTimeoutError)
                ),
                wait=wait_exponential(multiplier=5, min=5, max=30),
                stop=stop_after_attempt(6),
                reraise=True,
            ):
                with attempt:
                    page = await self.browser_service.get_available_page()
                    try:
                        soup = await self.browser_service.get_soup(
                            document_url, page=page
                        )
                    finally:
                        self.browser_service.release_page(page)

                    text_div = soup.find("div", class_="texto-dou") if soup else None
                    if not text_div:
                        raise ValueError(f"div.texto-dou not found for {document_url}")
        except (ValueError, PlaywrightError, PlaywrightTimeoutError) as e:
            logger.warning(f"Exhausted retries fetching {document_url}: {e}")
            pass  # Exhausted retries, fall through to error handler

        if not text_div:
            logger.warning(
                f"Failed to find main text content in document at {document_url}, skipping document."
            )
            await self._save_doc_error(
                title=doc_title,
                year=year,
                situation=situation,
                norm_type=doc_type,
                html_link=document_url,
                error_message="Could not find div.texto-dou in page",
                soup=soup.prettify(encoding="utf-8", formatter="html").decode("utf-8"),
            )
            return None

        html_string = text_div.prettify()
        html_string = f"<html><body>{html_string}</body></html>"

        text_markdown = await self._get_markdown(html_content=html_string)
        if not text_markdown or not text_markdown.strip():
            logger.warning(
                f"Failed to extract text from document at {document_url}, skipping document."
            )
            await self._save_doc_error(
                title=doc_title,
                year=year,
                situation=situation,
                norm_type=doc_type,
                html_link=document_url,
                error_message="Empty markdown after HTML conversion",
            )
            return None

        doc_info["html_string"] = html_string

        # Check for error messages
        error_msg = "The requested URL was not found on this server"
        if error_msg.lower() in text_markdown.lower():
            logger.warning(
                f"Document at {document_url} returned a 'not found' message, skipping document."
            )
            await self._save_doc_error(
                title=doc_title,
                year=year,
                situation=situation,
                norm_type=doc_type,
                html_link=document_url,
                error_message="Document URL returned 'not found' message",
            )
            return None

        return {**doc_info, "text_markdown": text_markdown}

    async def _scrape_year(self, year: str):
        """Scrape norms for a specific year from PowerBI interface"""
        year_int = int(year)

        # Concurrently gather all links for each situation
        situation_tasks = [
            self._scrape_situation(year_int, situation) for situation in self.situations
        ]
        gather_results = await self._gather_results(
            situation_tasks,
            context={"year": year, "type": "NA", "situation": "NA"},
            desc=f"ICMBIO | Gathering situations for {year}",
        )

        # Flatten the list of lists
        norms = []
        for res_list in gather_results:
            if isinstance(res_list, list):
                norms.extend(res_list)

        if not norms:
            logger.warning(f"No documents found for year {year} across all situations.")
            return []

        # Concurrently process document HTML -> Markdown
        tasks = [self._get_doc_data(norm) for norm in norms]
        valid_results = await self._gather_results(
            tasks,
            context={"year": year, "type": "NA", "situation": "NA"},
            desc=f"ICMBIO | Processing docs for {year}",
        )

        results = [r for r in valid_results if r is not None]

        if self.verbose:
            logger.info(f"Finished scraping for Year: {year} | Results: {len(results)}")

        return results
