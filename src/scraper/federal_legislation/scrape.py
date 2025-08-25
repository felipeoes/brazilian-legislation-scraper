from io import BytesIO
import time
import json
import re
from typing import Optional
from pathlib import Path
from urllib.parse import urljoin
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
from src.scraper.base.scraper import BaseScaper
from src.database.saver import ONEDRIVE_SAVE_DIR

VALID_SITUATIONS = [
    "Não%20consta%20revogação%20expressa",
    "Não%20Informado",  # since there is no explicit information about it's not valid, we consider it valid
    "Convertida%20em%20Lei",
    "Reeditada",
    "Reeditada%20com%20alteração",
]  # only norms with these situations (are actually valid norms)

INVALID_SITUATIONS = [
    "Arquivada",
    "Rejeitada",
    "Revogada",
    "Sem%20Eficácia",
]  # norms with these situations are invalid norms (no longer have legal effect)

# the reason to have invalid situations is in case we need to train a classifier to predict if a norm is valid or something else similar
SITUATIONS = VALID_SITUATIONS + INVALID_SITUATIONS

# OBS: empty string means all (Toda legislação). OPTIONS: 'Legislação+Interna' 'OR Legislação+Federal'
COVERAGE = [""]

TYPES = [
    "Alvará",
    "Ato",
    "Carta%20Régia",
    "Carta+Imperial",
    "Constitui%C3%A7%C3%A3o",
    "Decisão",
    "Decreto",
    "Decreto+Sem+N%C3%BAmero",
    "Decreto-Lei",
    "Emenda+Constitucional",
    "Instrução",
    "Lei",
    "Manifesto",
    "Mensagem",
    "Pacto",
    "Proclamação",
    "Protocolo",
    "Medida+Provis%C3%B3ria",
    "Ordem+de+Serviço",
    "Portaria",
    "Regulamento",
    "Resolu%C3%A7%C3%A3o+da+Assembl%C3%A9ia+Nacional+Constituinte",
    "Resolu%C3%A7%C3%A3o+da+C%C3%A2mara+dos+Deputados",
    "Resolução+da+Mesa",
    "Resolu%C3%A7%C3%A3o+do+Congresso+Nacional",
    "Resolu%C3%A7%C3%A3o+do+Senado+Federal",
]
ORDERING = "data%3AASC"
YEAR_START = 1808  # CHECK IF NECESSARY LATER


class CamaraDepScraper(BaseScaper):
    """Webscraper for Camara dos Deputados website (https://www.camara.leg.br/legislacao/)

    Example search request url: https://www.camara.leg.br/legislacao/busca?geral=&ano=&situacao=&abrangencia=&tipo=Decreto%2CDecreto+Legislativo%2CDecreto-Lei%2CEmenda+Constitucional%2CLei+Complementar%2CLei+Ordin%C3%A1ria%2CMedida+Provis%C3%B3ria%2CResolu%C3%A7%C3%A3o+da+C%C3%A2mara+dos+Deputados%2CConstitui%C3%A7%C3%A3o%2CLei%2CLei+Constitucional%2CPortaria%2CRegulamento%2CResolu%C3%A7%C3%A3o+da+Assembl%C3%A9ia+Nacional+Constituinte%2CResolu%C3%A7%C3%A3o+do+Congresso+Nacional%2CResolu%C3%A7%C3%A3o+do+Senado+Federal&origem=&numero=&ordenacao=data%3AASC
    """

    def __init__(
        self,
        base_url: str = "https://www.camara.leg.br/legislacao/",
        docs_save_dir: str = ONEDRIVE_SAVE_DIR.resolve().as_posix(),
        **kwargs,
    ):
        super().__init__(base_url, types=TYPES, situations=SITUATIONS, **kwargs)
        self.base_url = base_url
        self.coverage = kwargs.get("coverage", COVERAGE)
        self.ordering = kwargs.get("ordering", ORDERING)
        self.docs_save_dir = Path(docs_save_dir) / "LEGISLACAO_FEDERAL"
        self.params = {
            "abrangencia": "",
            "geral": "",
            "ano": "",
            "situacao": "",
            "origem": "",
            "numero": "",
            "ordenacao": "",
        }
        self._initialize_saver()

    def _format_search_url(self, year: str, situation: str, type: str) -> str:
        """Format search url with given year"""
        self.params["ano"] = year
        self.params["abrangencia"] = self.coverage[0]
        self.params["ordenacao"] = self.ordering
        self.params["situacao"] = situation
        self.params["tipo"] = type

        url = (
            self.base_url
            + "busca?"
            + "&".join([f"{key}={value}" for key, value in self.params.items()])
        )

        return url

    def _get_documents_html_links(self, url: str) -> "list[dict]":
        """Get html links from given url. Returns a list of dictionaries in the format {
            "title": str,
            "summary": str,
            "html_link": str
        }"""
        retries = 5
        documents_html_links_info = []
        for attempt in range(retries):
            try:
                # Get soup from url
                soup = self._get_soup(url)

                if soup is None:
                    return documents_html_links_info

                # Get all documents html links from page
                documents = soup.find_all("li", class_="busca-resultados__item")
                for document in documents:
                    a_tag = document.find(
                        "h3", class_="busca-resultados__cabecalho"
                    ).find("a")
                    document_html_link = a_tag["href"]
                    title = a_tag.text.strip()
                    summary = document.find(
                        "p", class_="busca-resultados__descricao js-fade-read-more"
                    ).text.strip()
                    documents_html_links_info.append(
                        {
                            "title": title,
                            "summary": summary,
                            "html_link": document_html_link,
                        }
                    )
                if not documents_html_links_info:
                    print(f"No documents found for url: {url}")
                    return documents_html_links_info
                 
                break  # Exit the retry loop if successful

            except Exception as e:
                if attempt < retries - 1:
                    print(f"Error fetching {url}: {e}. Retrying...")
                    time.sleep(2 ** (attempt + 1))
                else:
                    print(f"Failed to fetch {url} after {retries} attempts.")
                    return []

        return documents_html_links_info

    def _get_document_text_link(
        self, document_html_link: str, title: str, summary: str
    ) -> Optional[dict]:
        """Get proper document text link from given document html link"""

        soup = self._get_soup(document_html_link)
        if soup is None:
            print(f"Could not get soup for document: {title}")
            error_data = {
                "title": title,
                "year": self.params["ano"],
                "situation": self.params["situacao"],
                "type": self.params["tipo"],
                "summary": summary,
                "html_link": document_html_link,
            }
            self.error_queue.put(error_data)
            return None
        
        # check if not found (text not available)
        not_found = soup.find("h1", text="Not Found")
        if not_found:
            print(f"Document not found: {title}")
            # error_data = {
            #     "title": title,
            #     "year": self.params["ano"],
            #     "situation": self.params["situacao"],
            #     "type": self.params["tipo"],
            #     "summary": summary,
            #     "html_link": document_html_link,
            # }
            # self.error_queue.put(error_data)
            return None

        document_text_links = soup.find("div", class_="sessao")
        if not document_text_links:
            # probably link doesn't exist (error in website)
            print(f"Could not find text link for document: {title}")
            error_data = {
                "title": title,
                "year": self.params["ano"],
                "situation": self.params["situacao"],
                "type": self.params["tipo"],
                "summary": summary,
                "html_link": document_html_link,
                "soup": str(soup.prettify()),  # include soup for debugging
            }
            self.error_queue.put(error_data)
            json.dump(
                error_data, open("error_log.json", "a"), ensure_ascii=False, indent=2
            )
            return None

        # document_text_links_list = []
        # if document_text_links and hasattr(document_text_links, "find_all"):
        #     try:
        #         document_text_links_list = document_text_links.find_all("a")  # type: ignore
        #     except AttributeError:
        #         document_text_links_list = []

        document_text_link = None
        
        if document_text_links and hasattr(document_text_links, "find_all"):
            original_doc_text_links = []
            repub_doc_text_links = []
            doc_text_links_list = []
            
            for link in document_text_links.find_all("a"):
                link_text = link.text.strip().lower()
                if "texto - publicação original" in link_text:
                    original_doc_text_links.append(link)
                elif "texto - republicação" in link_text:
                    repub_doc_text_links.append(link)
                elif "texto -" in link_text:
                    doc_text_links_list.append(link)

            if original_doc_text_links:
                # if there is a link with "texto - publicação original" text, use it
                document_text_link = original_doc_text_links[-1]["href"]
                document_text_link = urljoin(document_html_link, document_text_link)
            elif repub_doc_text_links:
                # if there is a link with "texto - republicação" text, use it
                document_text_link = repub_doc_text_links[-1]["href"]
                document_text_link = urljoin(document_html_link, document_text_link)
            elif doc_text_links_list:
                # if there is a link with "texto -" text, use it
                document_text_link = doc_text_links_list[-1]["href"]
                document_text_link = urljoin(document_html_link, document_text_link)
                
        if document_text_link is None:
            print(f"Could not find text link for document: {title}")
            error_data = {
                "title": title,
                "year": self.params["ano"],
                "situation": self.params["situacao"],
                "type": self.params["tipo"],
                "summary": summary,
                "html_link": document_html_link,
                "soup": str(soup.prettify()),  # include soup for debugging
            }
            self.error_queue.put(error_data)
            return None

        return {"title": title, "summary": summary, "html_link": document_text_link}

    def _get_document_data(
        self, document_text_link: str, title: str, summary: str
    ) -> Optional[dict]:
        """Get data from given document text link . Data will be in the format {
            "title": str,
            "summary": str,
            "html_string": str,
            "text_markdown": str,
            "document_url": str
        }"""
        soup = self._get_soup(document_text_link)

        if soup is None:
            print(f"Could not get soup for document: {title}")
            error_data = {
                "title": title,
                "year": self.params["ano"],
                "situation": self.params["situacao"],
                "type": self.params["tipo"],
                "summary": summary,
                "html_link": document_text_link,
            }
            self.error_queue.put(error_data)
            return None

        try:
            # get html string
            texto_norma = soup.find("div", class_="textoNorma")
            if texto_norma is None:
                raise Exception("Could not find textoNorma div")
            
            # check if the text is empty
            if not texto_norma.text.strip():
                print(f"Document text is empty: {title}")
                error_data = {
                    "title": title,
                    "year": self.params["ano"],
                    "situation": self.params["situacao"],
                    "type": self.params["tipo"],
                    "summary": summary,
                    "html_link": document_text_link,
                }
                self.error_queue.put(error_data)
                return None

            html_string = f"<html>{texto_norma.prettify()}</html>"  # type: ignore

            buffer = BytesIO()
            buffer.write(html_string.encode())
            buffer.seek(0)

            # get text markdown
            text_markdown = self._get_markdown(stream=buffer).strip()
            return {
                "title": title,
                "summary": summary,
                "html_string": html_string,
                "text_markdown": text_markdown,
                "document_url": document_text_link,
            }
        except Exception as e:
            print(f"Error getting html string for document: {title}")
            print(e)
            error_data = {
                "title": title,
                "year": self.params["ano"],
                "situation": self.params["situacao"],
                "type": self.params["tipo"],
                "summary": summary,
                "html_link": document_text_link,
            }
            self.error_queue.put(error_data)
            return None

    def _scrape_year(self, year: int) -> list:
        """Scrape data from given year"""
        for situation in tqdm(
            self.situations,
            desc="CamaraDEP | Situations",
            total=len(self.situations),
            disable=not self.verbose,
        ):
            for type in self.types:
                results = []
                
                url = self._format_search_url(str(year), situation, type)
                # Each page has 20 results, find the total and calculate the number of pages
                per_page = 20
                self.soup = self._get_soup(url)

                if self.soup is None:
                    print(f"Could not get soup for url: {url}")
                    continue

                total_element = self.soup.find(
                    "div",
                    class_="busca-info__resultado busca-info__resultado--informado",
                )

                if total_element is None:
                    print(f"Could not find total element for url: {url}")
                    continue

                total = total_element.text
                total = int(total.strip().split()[-1])

                if total == 0:
                    # if self.verbose:
                    #     print(
                    #         f"No results for Year: {year} | Situation: {situation} | Type: {type}"
                    #     )
                    continue
                pages = total // per_page + 1

                # Get documents html links from all pages using ThreadPoolExecutor
                with ThreadPoolExecutor() as executor:
                    documents_html_links_info = []
                    futures = [
                        executor.submit(
                            self._get_documents_html_links, url + f"&pagina={page}"
                        )
                        for page in range(1, pages + 1)
                    ]
                    for future in tqdm(
                        as_completed(futures),
                        desc="CamaraDEP | Pages",
                        disable=not self.verbose,
                        total=len(futures),
                    ):
                        result = future.result()
                        if result is not None:
                            # filter out None results
                            if isinstance(result, list):
                                documents_html_links_info.extend(result)
                            else:
                                print(f"Unexpected result type: {type(result)}")
                                print(f"Result: {result}")
                # Get proper document text link from each document html link
                with ThreadPoolExecutor() as executor:
                    futures = []
                    documents_text_links = []
                    futures.extend(
                        [
                            executor.submit(
                                self._get_document_text_link,
                                document_html_link.get("html_link"),
                                document_html_link.get("title"),
                                document_html_link.get("summary"),
                            )
                            for document_html_link in documents_html_links_info
                            if document_html_link is not None
                        ]
                    )

                    for future in tqdm(
                        as_completed(futures),
                        desc="CamaraDEP | Text link",
                        total=len(futures),
                        disable=not self.verbose,
                    ):
                        documents_text_links.append(future.result())

                # Get data from all  documents text links using ThreadPoolExecutor
                with ThreadPoolExecutor() as executor:
                    futures = [
                        executor.submit(
                            self._get_document_data,
                            document_text_link.get("html_link"),
                            document_text_link.get("title"),
                            document_text_link.get("summary"),
                        )
                        for document_text_link in documents_text_links
                        if document_text_link is not None
                    ]

                    for future in tqdm(
                        as_completed(futures),
                        desc="CamaraDEP |Documents text",
                        total=len(futures),
                        disable=not self.verbose,
                    ):
                        result = future.result()

                        if result is None:
                            continue

                        # save to onedrive
                        queue_item = {
                            "year": year,
                            "situation": situation,
                            "type": type,
                            **result,
                        }
                        self.queue.put(queue_item)
                        results.append(queue_item)

                self.results.extend(results)
                self.count += len(results)

                print(
                    f"Finished scraping for Year: {year} | Situation: {situation} | Type: {type} | Results: {len(results)} | Total: {self.count}"
                )

        return self.results
