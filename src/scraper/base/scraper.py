import os
import requests
import time
import fitz
import warnings

from typing import Dict, List, Optional, Union
from PIL import Image
from openai import OpenAI
from io import BytesIO
from os import environ
from datetime import datetime
from bs4 import BeautifulSoup
from selenium.webdriver import Chrome
from selenium.webdriver.chrome.options import Options

from markitdown import MarkItDown, UnsupportedFormatException, FileConversionException
from tqdm.auto import tqdm
from concurrent.futures import ThreadPoolExecutor
from multiprocessing import Queue
from pathlib import Path
from dotenv import load_dotenv
from src.database.saver import OneDriveSaver
from src.utils.openvpn import OpenVPNManager


# supress markitdown warnings
warnings.filterwarnings("ignore", category=UserWarning, module="markitdown")

load_dotenv()

YEAR_START = 1808  # CHECK IF NECESSARY LATER
DEFAULT_VALID_SITUATION = "Não consta revogação expressa"
DEFAULT_INVALID_SITUATION = "Revogada"

ONEDRIVE_STATE_LEGISLATION_SAVE_DIR = (
    rf"{environ.get('ONEDRIVE_STATE_LEGISLATION_SAVE_DIR')}"
)


# retry decorator with exponential backoff
def retry(max_retries: int, base_delay: int = 3):
    def decorator(func):
        def wrapper(*args, **kwargs):
            retries = 0
            while retries < max_retries:
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    print(
                        f"Error: {e}. Retrying in {base_delay * (2 ** retries)} seconds..."
                    )
                    time.sleep(base_delay * (2**retries))
                    retries += 1

                    if retries == max_retries:
                        raise e

        return wrapper

    return decorator


def safe_pdf_open(pdf_content: bytes) -> Optional[fitz.Document]:
    """
    Safely open a PDF document with error handling for common PyMuPDF issues.
    
    Args:
        pdf_content (bytes): PDF content as bytes
        
    Returns:
        fitz.Document or None: PyMuPDF Document object or None if failed
    """
    try:
        return fitz.open("pdf", pdf_content)
    except Exception as e:
        error_msg = str(e).lower()
        if "invalid float value" in error_msg or "gray stroke color" in error_msg:
            print(f"PDF contains invalid color definitions: {e}")
        elif "damaged" in error_msg or "corrupt" in error_msg:
            print(f"PDF appears to be corrupted: {e}")
        else:
            print(f"Failed to open PDF: {e}")
        return None


class BaseScaper:
    """Base class for state legislation scrapers"""

    def __init__(
        self,
        base_url: str,
        types: Union[List, Dict],
        situations: Union[list, dict],
        year_start: int = YEAR_START,
        year_end: int = datetime.now().year,
        docs_save_dir: Path = Path(ONEDRIVE_STATE_LEGISLATION_SAVE_DIR),
        llm_client: Optional[OpenAI] = None,
        llm_model: Optional[str] = None,
        llm_prompt: str = "Extraia todo  o conteúdo da imagem. Não inclua nenhum outro texto ou explicação. Retorne somente o conteúdo extraído.",
        use_selenium: bool = False,
        multiple_drivers: bool = False,
        use_selenium_vpn: bool = False,
        vpn_extension_path: Optional[str] = None,
        vpn_extension_page: Optional[str] = None,
        use_requests_session: bool = False,
        use_openvpn: bool = False,
        config_files: Optional[list] = None,
        openvpn_credentials_map: Optional[dict] = None,
        proxies: Optional[dict] = None,
        max_workers: int = 16,
        verbose: bool = False,
    ):
        self.base_url = base_url
        self.types = types
        self.situations = situations
        self.year_start = year_start
        self.year_end = year_end
        self.docs_save_dir = Path(docs_save_dir)
        self.llm_client = llm_client
        self.llm_model = llm_model
        self.llm_prompt = llm_prompt
        self.use_selenium = use_selenium
        self.multiple_drivers = multiple_drivers
        self.use_requests_session = use_requests_session
        self.use_openvpn = use_openvpn
        self.use_selenium_vpn = use_selenium_vpn
        self.vpn_extension_path = vpn_extension_path
        self.vpn_extension_page = vpn_extension_page
        self.config_files = config_files
        self.openvpn_credentials_map = openvpn_credentials_map
        self.verbose = verbose
        self.proxies = proxies
        self.max_workers = max_workers
        self.years = list(range(self.year_start, self.year_end + 1))
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36"
        }
        self.llm_md_header = "\n# Description:\n"
        self.queue = Queue()
        self.error_queue = Queue()
        self.results = []
        self.count = 0  # keep track of number of results
        self.md = MarkItDown(llm_client=llm_client, llm_model=llm_model)
        self.soup = None
        self.session: requests.Session = requests.Session()
        self.driver: Optional[Chrome] = None
        self.drivers: list[Chrome] = []
        self.saver: Optional[OneDriveSaver] = None
        self.openvpn_manager: Optional[OpenVPNManager] = None
        self.initialize_selenium()
        self.initialize_requests_session()
        self.initialize_openvpn_manager()

    def initialize_requests_session(self):
        """Initialize requests session"""
        if self.use_requests_session:
            self.session.headers.update(self.headers)

    def initialize_selenium(self):
        """Initialize selenium driver"""
        options = Options()

        if self.use_selenium and self.use_selenium_vpn and self.vpn_extension_path:
            # load the extension
            extension_abs_path = os.path.abspath(self.vpn_extension_path)
            options.add_extension(extension_abs_path)
            print(f"Attempting to load packed extension from: {extension_abs_path}")

        if self.use_selenium and not self.multiple_drivers:
            options.add_argument("--no-sandbox")
            options.add_argument("--disable-dev-shm-usage")
            options.add_argument("--disable-gpu")
            options.add_argument("--enable-javascript")
            self.driver = Chrome(options=options)

        elif self.multiple_drivers:
            # create a new instance of the Chrome driver for each worker
            for driver_id, _ in enumerate(range(self.max_workers)):
                driver = Chrome(options=options)
                driver.id = driver_id
                driver.available = True
                self.drivers.append(driver)
                print(f"Driver {driver.id} initialized")

    def _get_available_driver(self):
        """Get available driver from the list of drivers"""
        while True:
            for driver in self.drivers:
                if driver.available:
                    driver.available = False
                    return driver
            time.sleep(1)  # wait for a driver to become available

    def _release_driver(self, driver: Chrome):
        """Release driver back to the pool"""
        driver.available = True

    def _initialize_saver(self):
        """Initialize saver class. The child class should call this method in its __init__ method, after setting the docs_save_dir attribute."""
        self.saver = OneDriveSaver(self.queue, self.error_queue, self.docs_save_dir)

    def initialize_openvpn_manager(self):
        """Initialize openvpn manager"""
        if self.use_openvpn:
            self.openvpn_manager = OpenVPNManager(
                config_files=self.config_files if self.config_files is not None else [],
                credentials_map=self.openvpn_credentials_map,
            )

    def _get_request(self, url: str, **kwargs) -> requests.Response:
        """Get request from given url"""
        if self.use_requests_session:
            response = self.session.get(url, proxies=self.proxies, **kwargs)
        else:
            response = requests.get(url, proxies=self.proxies, **kwargs)

        return response

    def _post_request(self, url: str, json: dict, **kwargs) -> requests.Response:
        """Post request to given url"""
        if self.use_requests_session:
            response = self.session.post(url, json=json, proxies=self.proxies, **kwargs)
        else:
            response = requests.post(url, json=json, proxies=self.proxies, **kwargs)

        return response

    def _make_request(
        self,
        url: str,
        method: str = "GET",
        json: dict = {},
        payload: list | dict = {},
        *args,
        **kwargs,
    ) -> Optional[requests.Response]:
        """Make request to given url"""
        retries = 3
        for attempt in range(retries):
            try:

                if method == "POST":
                    response = self._post_request(
                        url,
                        json=json,
                        data=payload,  # payload will be used for form data in POST requests, useful when have files or duplicate keys
                        headers=self.headers,
                        verify=False,
                        *args,
                        **kwargs,
                    )
                else:
                    response = self._get_request(
                        url,
                        headers=self.headers,
                        verify=False,
                        *args,
                        **kwargs,
                    )

                # check  "O servidor encontrou um erro interno, ou está sobrecarregado" error
                if (
                    "O servidor encontrou um erro interno, ou está sobrecarregado"
                    in response.text
                ):
                    print("Server error, retrying...")
                    time.sleep(5 ** attempt)
                    continue

                # check for 429, 502 503 status code (right now useful for mato grosso scraper)
                if response.status_code in [429, 502, 503]:
                    # print(f"Status code {response.status_code}, retrying...")
                    time.sleep(5 ** attempt)
                    continue

                return response
            except Exception as e:
                print(f"Error getting response from url: {url}")
                print(e)
                time.sleep(5 ** attempt)

        return None

    def _change_vpn_connection(self, *args, **kwargs):
        """Change VPN connection. Currently the supported VPN is ProtonVPN and the way to reconnect is by killing the process and starting it again."""
        if not self.use_openvpn:
            print("OpenVPN is not enabled, skipping VPN connection change")
            return

        if self.openvpn_manager is None:
            print("OpenVPN manager is not initialized, skipping VPN connection change")
            return

        self.openvpn_manager.change_vpn_connection()

    def _get_soup(self, url: Union[str, requests.Response],
                  method: str = "GET", *args, **kwargs
                  ) -> Optional[BeautifulSoup]:
        """Get BeautifulSoup object from given url"""

        if isinstance(url, requests.Response):
            return BeautifulSoup(url.content, "html.parser")

        res = self._make_request(url, method=method, *args, **kwargs)

        if res is None:
            return None

        return BeautifulSoup(res.content, "html.parser")

    def _selenium_get_soup(
        self, url: str, driver: Optional[Chrome] = None
    ) -> BeautifulSoup:
        """Get BeautifulSoup object from given url using selenium"""
        retries = 3
        while retries > 0:
            try:
                if driver:
                    driver.get(url)
                elif self.driver:
                    self.driver.get(url)
                if self.use_openvpn:
                    self._handle_blocked_access()

                time.sleep(1)
                break
            except Exception as e:
                print(f"Error: {e}")
                retries -= 1

        if driver:
            return BeautifulSoup(driver.page_source, "html.parser")

        if self.driver is not None:
            return BeautifulSoup(self.driver.page_source, "html.parser")
        else:
            raise RuntimeError("Selenium driver is not initialized.")

    def _pdf_to_images(self, doc: fitz.Document) -> list[Image.Image]:
        """
        Converts a PDF document to a list of images, one image per page.

        Args:
            doc (fitz.Document): PyMuPDF Document object.

        Returns:
            list: List of PIL Image objects, one for each page in the PDF.
        """
        image_list = []

        for page_num in range(doc.page_count):
            try:
                page = doc.load_page(page_num)
                pix = page.get_pixmap(
                    matrix=fitz.Identity,
                    dpi=None,
                    colorspace=fitz.csRGB,
                    clip=None,
                    annots=True,
                )

                # Convert PyMuPDF Pixmap to PIL Image
                img = Image.frombytes("RGB", [pix.width, pix.height], pix.samples)
                image_list.append(img)
            except Exception as e:
                # Handle errors like "Cannot set gray stroke color because /'P14' is an invalid float value"
                print(f"Error processing page {page_num}: {e}")
                # Try alternative approach with different settings
                try:
                    page = doc.load_page(page_num)
                    # Use different pixmap settings to handle corrupted color spaces
                    pix = page.get_pixmap(
                        matrix=fitz.Identity,
                        dpi=150,  # Lower DPI might help with problematic content
                        colorspace=fitz.csGRAY,  # Use grayscale to avoid color issues
                        clip=None,
                        annots=False,  # Disable annotations which might contain invalid colors
                    )
                    img = Image.frombytes("L", [pix.width, pix.height], pix.samples)
                    # Convert grayscale to RGB for consistency
                    img = img.convert("RGB")
                    image_list.append(img)
                except Exception as e2:
                    print(f"Failed to process page {page_num} even with fallback method: {e2}")
                    continue

        return image_list

    def _get_pdf_image_markdown(self, pdf_content: bytes) -> str:
        """Get markdown response from given pdf content"""
        pdf_file_stream = BytesIO(pdf_content)
        text_markdown_raw = self._get_markdown(stream=pdf_file_stream)
        if text_markdown_raw and len(text_markdown_raw) > 200:
            print("Text extracted from pdf")
            return text_markdown_raw

        # get images from pdf
        pdf = safe_pdf_open(pdf_content)
        if pdf is None:
            return ""
        
        try:
            images = self._pdf_to_images(pdf)
        except Exception as e:
            print(f"Error converting PDF to images: {e}")
            pdf.close()
            return ""
        finally:
            # Always close the PDF document to free memory
            try:
                pdf.close()
            except:
                pass

        # paralllel processing
        text_markdown_img = ""
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = []
            for img in images:
                try:
                    # Convert PIL Image to BytesIO stream
                    buffer = BytesIO()
                    img.save(buffer, format="PNG")
                    img = BytesIO(buffer.getvalue())
                except Exception as e:
                    print(f"Error converting image to BytesIO: {e}")
                    continue
                
                future = executor.submit(self._get_markdown, stream=img)
                futures.append(future)

            for future in tqdm(
                futures,  # not using as_completed because we want the results in order
                desc="Converting images to markdown",
                total=len(futures),
                disable=not self.verbose,
            ):
                try:
                    md_content = future.result()
                    text_markdown_img += md_content + "\n\n"
                except Exception as e:
                    print(f"Error converting image to markdown: {e}")
                    continue

        if not text_markdown_img and not text_markdown_raw:
            print("No text extracted from PDF or images")
            return ""

        text_markdown = text_markdown_raw + text_markdown_img
        return text_markdown

    def _get_markdown(
        self,
        url: Optional[str] = None,
        response: Optional[requests.Response] = None,
        stream: Optional[BytesIO] = None,
        retries: int = 3,
    ) -> str:
        """Get markdown response from given url"""
        md_content = ""
        while retries > 0:
            try:
                if stream is not None:
                    md_content = self.md.convert_stream(
                        stream, llm_prompt=self.llm_prompt
                    ).text_content

                    if (
                        not md_content
                    ):  # for images, sometimes the mllm struggles to process, try again
                        retries -= 1
                        continue

                    return md_content.replace(self.llm_md_header, "").strip()

                if response is None and url:
                    response = self._make_request(url)

                if response is not None:
                    md_content = self.md.convert(response).text_content
                else:
                    md_content = ""

            except FileConversionException as e:
                print(f"Error converting to markdown: {e}")
                md_content = ""

            except UnsupportedFormatException as e:
                print(f"Error converting to markdown: {e}")
                md_content = ""

            except Exception as e:
                print(f"Error getting markdown from url: {url} | Error: {e}")
                md_content = ""

            if md_content:
                break

            retries -= 1

        return md_content.replace(self.llm_md_header, "").strip()

    def _handle_blocked_access(self, *args, **kwargs):
        pass

    def _format_search_url(self, *args, **kwargs) -> str:
        """Format search URL for the given parameters"""
        raise NotImplementedError(
            "This method should be implemented in the child class."
        )

    def _get_docs_links(self, *args, **kwargs) -> Optional[List[Dict]]:
        """Get document links from the given parameters"""
        raise NotImplementedError(
            "This method should be implemented in the child class."
        )

    def _get_doc_data(self, *args, **kwargs) -> Optional[Union[Dict, List[Dict]]]:
        """Get document data from the given parameters"""
        raise NotImplementedError(
            "This method should be implemented in the child class."
        )

    def _scrape_year(self, year: int, *args, **kwargs) -> None:
        """Scrape norms for a specific year"""
        raise NotImplementedError(
            "This method should be implemented in the child class."
        )

    def scrape(self) -> list:
        """Scrape data from all years"""

        # start saver thread
        if not self.saver:
            raise RuntimeError(
                "Saver is not initialized. Call _initialize_saver() in the child class __init__ method."
            )

        self.saver.start()

        # check if can resume from last scrapped year
        resume_from = self.year_start  # 1808
        forced_resume = self.year_start != YEAR_START
        if self.saver.last_year is not None and not forced_resume:
            print(f"Resuming from {self.saver.last_year}")
            resume_from = int(self.saver.last_year)
        else:
            print(f"Starting from {resume_from}")

        # # scrape data from all years
        for year in tqdm(
            self.years, desc=f"{self.__class__.__name__} | Years", total=len(self.years)
        ):
            if year < resume_from:
                continue

            self._scrape_year(year)

        # stop saver thread
        self.saver.stop()

        # wait for saver thread to finish
        self.saver.join()

        return self.results
