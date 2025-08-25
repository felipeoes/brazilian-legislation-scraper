import time
import re

from io import BytesIO
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
from typing import Optional
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from src.scraper.base.scraper import BaseScaper
from threading import Lock

lock = Lock()


TYPES = {
    "Instrução Normativa": "INSTRUÇÕES NORMATIVAS",
    "Portaria": "PORTARIAS",
    "Outros Atos": "OUTROS ATOS",
}

# Cant filter by situation in the powerbi interface, will get situation from the table
VALID_SITUATIONS = []
INVALID_SITUATIONS = []

SITUATIONS = VALID_SITUATIONS + INVALID_SITUATIONS


class ICMBioScraper(BaseScaper):
    """Webscraper for ICMBio (Instituto Chico Mendes de Conservação da Biodiversidade) PowerBI dashboard

    PowerBI URL: https://app.powerbi.com/view?r=eyJrIjoiOWJlYjU0OWQtMTEwZC00NTEwLWI4NGYtYWY4MzJmMzM0NTQ1IiwidCI6IjM5NTdhMzY3LTZkMzgtNGMxZi1hNGJhLTMzZThmM2M1NTBlNyJ9
    """

    def __init__(
        self,
        base_url: str = "https://app.powerbi.com/view?r=eyJrIjoiOWJlYjU0OWQtMTEwZC00NTEwLWI4NGYtYWY4MzJmMzM0NTQ1IiwidCI6IjM5NTdhMzY3LTZkMzgtNGMxZi1hNGJhLTMzZThmM2M1NTBlNyJ9",
        **kwargs,
    ):
        super().__init__(base_url, types=TYPES, situations=SITUATIONS, **kwargs)
        self.docs_save_dir = self.docs_save_dir / "ICMBIO"
        self._initialize_saver()

    def _extract_retry_delay(self, error_message: str) -> int:
        """Extract retry delay from error message"""
        try:
            # Look for retryDelay pattern like "42s"
            match = re.search(r"'retryDelay': '(\d+)s'", error_message)
            if match:
                return int(match.group(1))
        except:
            pass
        return 60  # Default to 60 seconds if can't parse

    def _llm_query_with_retry(
        self, full_pdf_content: str, title: str, max_retries: int = 3
    ) -> Optional[str]:
        """Query LLM with retry logic for rate limits"""
        for attempt in range(max_retries):
            try:
                llm_response = self.llm_client.chat.completions.create(
                    model=self.llm_model,
                    reasoning_effort="medium",
                    messages=[
                        {
                            "role": "user",
                            "content": [
                                {
                                    "type": "text",
                                    "text": f"""PDF Content:
                                    {full_pdf_content}
                                    
                                    Extraia todo o texto referente à norma: {title}. Retorne somente o texto, sem nenhuma outra informação.""",
                                }
                            ],
                        }
                    ],
                )
                return llm_response.choices[0].message.content

            except Exception as e:
                error_str = str(e)
                if "429" in error_str and "rate" in error_str.lower():
                    if attempt < max_retries - 1:  # Don't wait on the last attempt
                        retry_delay = self._extract_retry_delay(error_str)
                        if self.verbose:
                            print(
                                f"Rate limit hit (attempt {attempt + 1}/{max_retries}). Waiting {retry_delay} seconds..."
                            )
                        time.sleep(retry_delay)
                        continue
                    else:
                        if self.verbose:
                            print(f"Max retries reached for LLM query. Error: {e}")
                        return None
                else:
                    # Non-rate limit error, don't retry
                    if self.verbose:
                        print(f"LLM query failed with non-rate limit error: {e}")
                    return None
        return None

    def _setup_powerbi_filters(self, wait: WebDriverWait, scrollable_container) -> bool:
        """Setup PowerBI filters for ÓRGÃO. Returns True if successful."""
        try:
            orgao_dropdown = self.driver.find_element(
                By.CSS_SELECTOR, 'div[aria-label="ÓRGÃO"]'
            )
            orgao_dropdown.click()
            time.sleep(2)

            # Unselect all first
            select_all = self.driver.find_element(
                By.XPATH, '//div[@role="option"]//span[text()="Select all"]'
            )
            select_all.click()

            # Select ICMBio option
            icmbio_option = self.driver.find_element(
                By.XPATH, '//div[@role="option"]//span[text()="ICMBio"]'
            )
            icmbio_option.click()
            time.sleep(2)

            scrollable_container.click()
            time.sleep(2)
            return True
        except Exception as e:
            if self.verbose:
                print(f"Error selecting ICMBio filter: {e}")
            return False

    def _select_year_with_scroll(self, year: int, scrollable_container) -> bool:
        """Select year in ANO filter with scrolling support. Returns True if successful."""
        try:
            year_dropdown = self.driver.find_element(
                By.CSS_SELECTOR, 'div[aria-label="ANO"]'
            )
            year_dropdown.click()
            time.sleep(2)

            # Try to find the year option, scroll if necessary
            year_found = False
            max_scroll_attempts = 10
            scroll_attempts = 0

            while not year_found and scroll_attempts < max_scroll_attempts:
                try:
                    # Try multiple selectors for the year option
                    year_option = None
                    selectors = [
                        f'//div[@role="option"]//span[text()="{year}"]',
                        f'//span[@class="slicerText" and text()="{year}"]',
                        f'//div[contains(@class, "slicerItemContainer")]//span[text()="{year}"]'
                    ]
                    
                    for selector in selectors:
                        try:
                            year_option = self.driver.find_element(By.XPATH, selector)
                            break
                        except NoSuchElementException:
                            continue
                    
                    if year_option:
                        # Wait for element to be clickable and scroll it into view
                        self.driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", year_option)
                        time.sleep(0.5)
                        # Use JavaScript click to avoid interception
                        self.driver.execute_script("arguments[0].click();", year_option)
                        year_found = True
                        if self.verbose:
                            print(f"✅ Found and selected year {year}")
                    else:
                        raise NoSuchElementException(f"Year {year} not found with any selector")
                        
                except NoSuchElementException:
                    if not self._scroll_ano_dropdown():
                        break
                    scroll_attempts += 1

            if not year_found:
                if self.verbose:
                    print(
                        f"❌ Could not find year {year} in dropdown after {max_scroll_attempts} scroll attempts"
                    )
                return False

            time.sleep(2)
            scrollable_container.click()
            time.sleep(2)
            return True
        except Exception as e:
            if self.verbose:
                print(f"Error selecting year {year} filter: {e}")
            return False

    def _scroll_ano_dropdown(self) -> bool:
        """Scroll the ANO dropdown. Returns True if scroll was successful."""
        try:
            # First try to find the specific scrollable content within the ANO dropdown
            ano_slicer = self.driver.find_element(
                By.CSS_SELECTOR, '.slicerBody[aria-label="ANO"]'
            )
            
            # Try different scroll containers in order of preference
            scroll_containers = [
                ".scroll-content",
                ".scrollRegion", 
                ".slicerItemsContainer",
                ".slicer-content-wrapper"
            ]
            
            for container_class in scroll_containers:
                try:
                    scroll_element = ano_slicer.find_element(By.CSS_SELECTOR, container_class)
                    # Scroll down within the container
                    self.driver.execute_script("arguments[0].scrollTop += 150;", scroll_element)
                    time.sleep(1)
                    if self.verbose:
                        print(f"✅ Scrolled using {container_class}")
                    return True
                except NoSuchElementException:
                    continue
            
            # If no specific container found, scroll the slicer body itself
            self.driver.execute_script("arguments[0].scrollTop += 150;", ano_slicer)
            time.sleep(1)
            if self.verbose:
                print("✅ Scrolled using slicer body")
            return True
            
        except NoSuchElementException:
            # Fallback to window scroll
            self.driver.execute_script("window.scrollBy(0, 150);")
            time.sleep(1)
            if self.verbose:
                print("✅ Scrolled using window fallback")
            return True
        except Exception as e:
            if self.verbose:
                print(f"❌ Error during scroll: {e}")
            return False

    def _select_document_type(
        self, norm_type_value: str, wait: WebDriverWait, scrollable_container
    ) -> bool:
        """Select document type in DOCUMENTO filter. Returns True if successful."""
        try:
            time.sleep(3)

            ato_dropdown = wait.until(
                EC.element_to_be_clickable(
                    (By.CSS_SELECTOR, 'div[aria-label="DOCUMENTO (grupos)"]')
                )
            )

            self.driver.execute_script(
                "arguments[0].scrollIntoView(true);", ato_dropdown
            )
            time.sleep(1)
            ato_dropdown.click()
            time.sleep(3)

            # Try targeted approach first
            try:
                documento_slicer = self.driver.find_element(
                    By.CSS_SELECTOR, '.slicerBody[aria-label="DOCUMENTO (grupos)"]'
                )

                wait.until(
                    EC.presence_of_element_located(
                        (By.XPATH, './/div[@role="option"]//span[text()="Select all"]')
                    )
                )

                select_all_ato = documento_slicer.find_element(
                    By.XPATH, './/div[@role="option"]//span[text()="Select all"]'
                )
                select_all_ato.click()
                time.sleep(2)

                norm_type_option = documento_slicer.find_element(
                    By.XPATH,
                    f'.//div[@role="option"]//span[text()="{norm_type_value}"]',
                )
                norm_type_option.click()

            except NoSuchElementException:
                # Fallback to global search
                if self.verbose:
                    print("Trying fallback approach for dropdown options...")

                wait.until(
                    EC.presence_of_element_located(
                        (By.XPATH, '//div[@role="option"]//span[text()="Select all"]')
                    )
                )

                select_all_ato = self.driver.find_element(
                    By.XPATH, '//div[@role="option"]//span[text()="Select all"]'
                )
                select_all_ato.click()
                time.sleep(2)

                norm_type_option = wait.until(
                    EC.element_to_be_clickable(
                        (
                            By.XPATH,
                            f'//div[@role="option"]//span[text()="{norm_type_value}"]',
                        )
                    )
                )
                norm_type_option.click()

            time.sleep(3)
            scrollable_container.click()
            time.sleep(2)
            return True

        except TimeoutException:
            if self.verbose:
                print(
                    f"Timeout waiting for document type {norm_type_value} dropdown to become interactive"
                )
            return False
        except Exception as e:
            if self.verbose:
                print(f"Error selecting document type {norm_type_value} filter: {e}")
            return False

    def _scrape_table_rows(self, scrollable_container) -> list:
        """Scrape rows from the PowerBI table using virtual scrolling."""
        scraped_rows_set = set()
        final_data_list = []
        stagnation_counter = 0

        while stagnation_counter < 3:
            rows = scrollable_container.find_elements(
                By.CSS_SELECTOR, 'div[role="row"]'
            )
            rows_before_iteration = len(scraped_rows_set)

            for row in rows:
                try:
                    cells = row.find_elements(By.CSS_SELECTOR, 'div[role="gridcell"]')
                    if len(cells) < 7:
                        continue

                    year_text = cells[1].text
                    title_text = cells[2].text
                    summary_text = cells[3].text
                    row_key = f"{year_text}|{title_text}"

                    if row_key not in scraped_rows_set:
                        scraped_rows_set.add(row_key)

                        try:
                            link_element = cells[4].find_element(By.TAG_NAME, "a")
                            document_url = link_element.get_attribute("href")
                        except NoSuchElementException:
                            if self.verbose:
                                print(
                                    f"No link found in row: {row_key}. Skipping document."
                                )
                            continue

                        row_data = {
                            "year": year_text,
                            "title": title_text,
                            "summary": summary_text,
                            "document_url": document_url,
                            "situation": cells[5].text,
                            "subject": cells[6].text,
                        }
                        final_data_list.append(row_data)
                except Exception as e:
                    if self.verbose:
                        print(f"Error processing row: {e}")
                    continue

            # Scroll to load more rows
            if rows:
                last_row = rows[-1]
                self.driver.execute_script(
                    "arguments[0].scrollIntoView(true);", last_row
                )
                if self.verbose:
                    print(
                        f"Scrolled last row into view. Total unique rows found: {len(final_data_list)}"
                    )
            else:
                if self.verbose:
                    print("No rows found to scroll to. Stopping.")
                break

            time.sleep(2)

            rows_after_iteration = len(scraped_rows_set)
            if rows_after_iteration == rows_before_iteration:
                stagnation_counter += 1
                if self.verbose:
                    print(
                        f"  -> No new rows found. Stagnation counter: {stagnation_counter}/3"
                    )
            else:
                stagnation_counter = 0

        return final_data_list

    def _get_docs_links(self, year: int, norm_type_value: str) -> list:
        """Get documents links from PowerBI virtual scroll table for a specific year and document type.
        Returns a list of dicts with keys 'title', 'year', 'summary', 'document_url', 'situation', and 'subject'
        """

        if not self.driver:
            raise RuntimeError("Selenium driver is not initialized.")

        with lock:
            self.driver.get(self.base_url)

            # Wait for the scrollable container
            scrollable_container_selector = (By.CSS_SELECTOR, "div.mid-viewport")
            try:
                wait = WebDriverWait(self.driver, 30)
                scrollable_container = wait.until(
                    EC.presence_of_element_located(scrollable_container_selector)
                )
                if self.verbose:
                    print("✅ Scrollable table container found.")
            except TimeoutException:
                if self.verbose:
                    print("❌ Timed out waiting for the table container to load.")
                return []

            # Setup filters step by step
            if not self._setup_powerbi_filters(wait, scrollable_container):
                return []

            if not self._select_year_with_scroll(year, scrollable_container):
                return []

            if not self._select_document_type(
                norm_type_value, wait, scrollable_container
            ):
                return []

            # Scrape the table rows
            final_data_list = self._scrape_table_rows(scrollable_container)

            if self.verbose:
                print(
                    f"Scraping complete. Found {len(final_data_list)} documents for year {year} and type {norm_type_value}."
                )

            return final_data_list

    def _fetch_pdf_pages(self, document_url: str, doc_title: str) -> Optional[str]:
        """Fetch and process PDF pages from DOU links"""
        url = "https://pesquisa.in.gov.br/imprensa/servlet/INPDFViewer?"
        journal = document_url.split("jornal=")[1].split("&")[0]
        page = int(document_url.split("pagina=")[1].split("&")[0])
        date = document_url.split("data=")[1].split("&")[0]
        captchafield = "firstAccess"
        full_pdf_content = ""

        for index in range(3):
            try:
                constructed_url = f"{url}jornal={journal}&pagina={page + index}&data={date}&captchafield={captchafield}"
                txt_markdown = self._get_markdown(constructed_url)
                if not txt_markdown or not txt_markdown.strip():
                    if self.verbose:
                        print(
                            f"Failed to extract text from page {page + index} of document at {constructed_url}."
                        )
                    continue
                full_pdf_content += "\n" + txt_markdown
            except Exception as e:
                if self.verbose:
                    print(f"Exception fetching page {page + index} of document: {e}")
                break

        if not full_pdf_content.strip():
            if self.verbose:
                print(
                    f"No valid pages fetched for document titled '{doc_title}', skipping document."
                )
            return None

        return full_pdf_content

    def _get_doc_data(self, doc_info: dict) -> Optional[dict]:
        """Get document data from norm dict. The document_url is already in the doc_info."""

        document_url = doc_info.get("document_url")
        if not document_url:
            if self.verbose:
                print(
                    f"Invalid document URL for document titled '{doc_info.get('title')}', skipping document."
                )
            return None

        # Handle DOU PDF links with LLM processing
        if "pesquisa.in.gov.br/imprensa/" in document_url:
            doc_title = doc_info.get("title", "Sem título")
            full_pdf_content = self._fetch_pdf_pages(document_url, doc_title)
            if not full_pdf_content:
                return None

            # Use retry logic for LLM query
            text_markdown = self._llm_query_with_retry(full_pdf_content, doc_title)
            if not text_markdown:
                if self.verbose:
                    print(
                        f"Failed to get LLM response for document '{doc_title}', skipping document."
                    )
                return None
        elif "in.gov.br/web/dou" in document_url:
            # need to extract html content directly to avoid unnecessary headers, footers, and other noise
            soup = self._get_soup(document_url)

            text_div = soup.find("div", class_="texto-dou")
            if not text_div:
                if self.verbose:
                    print(
                        f"Failed to find main text content in document at {document_url}, skipping document."
                    )
                return None

            html_string = text_div.prettify()

            # enclose in html tags to convert to markdown
            html_string = f"<html><body>{html_string}</body></html>"

            buffer = BytesIO()
            buffer.write(html_string.encode())
            buffer.seek(0)

            text_markdown = self._get_markdown(stream=buffer)
            if not text_markdown or not text_markdown.strip():
                if self.verbose:
                    print(
                        f"Failed to extract text from document at {document_url}, skipping document."
                    )
                return None

            doc_info["html_string"] = html_string

        else:
            # Standard document processing
            if self.verbose:
                print(
                    f"Fetching document text from {document_url} using standard method..."
                )
            text_markdown = self._get_markdown(document_url)

            if text_markdown is None or not text_markdown.strip():
                if self.verbose:
                    print(
                        f"Failed to extract text from document at {document_url}, skipping document."
                    )
                return None

            # Check for error messages
            error_msg = "The requested URL was not found on this server"
            if error_msg.lower() in text_markdown.lower():
                if self.verbose:
                    print(
                        f"Document at {document_url} returned a 'not found' message, skipping document."
                    )
                return None

        return {**doc_info, "text_markdown": text_markdown}

    def _scrape_year(self, year: str):
        """Scrape norms for a specific year from PowerBI interface"""
        year_int = int(year)

        # Ensure types is a dict
        if not isinstance(self.types, dict):
            raise ValueError("Types must be a dictionary for ICMBio scraper")

        for norm_type, norm_type_value in tqdm(
            self.types.items(),
            desc=f"ICMBIO | Year: {year} | Types",
            total=len(self.types),
            disable=not self.verbose,
        ):
            if self.verbose:
                print(
                    f"Scraping ICMBio documents for year {year} and type {norm_type}..."
                )

            # Get all documents for this year and type from PowerBI
            norms = self._get_docs_links(year_int, norm_type_value)

            if not norms:
                if self.verbose:
                    print(f"No documents found for year {year} and type {norm_type}")
                continue

            # Get all norms data
            results = []

            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                futures = [executor.submit(self._get_doc_data, norm) for norm in norms]

                for future in tqdm(
                    as_completed(futures),
                    desc="ICMBIO | Get document data",
                    total=len(norms),
                    disable=not self.verbose,
                ):
                    try:
                        result = future.result()
                        if result is None:
                            continue

                        # Save to queue
                        queue_item = {
                            "year": year,
                            "type": norm_type,
                            **result,
                        }

                        self.queue.put(queue_item)
                        results.append(queue_item)
                    except Exception as e:
                        print(f"Error processing document: {e}")
                        continue

                self.results.extend(results)
                self.count += len(results)

                if self.verbose:
                    print(
                        f"Finished scraping for Year: {year} | Type: {norm_type} | Results: {len(results)} | Total: {self.count}"
                    )
