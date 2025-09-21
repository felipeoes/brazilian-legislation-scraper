import re
from io import BytesIO
from typing import Optional, List, Dict
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
from src.scraper.base.scraper import BaseScaper
from src.database.saver import FileSaver
from urllib.parse import urlencode, urljoin

TYPES = {
    "Constituição Estadual": 2,
    "Decisão": 16,
    "Decreto": 4,
    "Decreto-Lei": 5,
    "Deliberação": 6,
    "Emenda Constitucional": 7,
    "Lei": 9,
    "Lei Complementar": 10,
    "Lei Constitucional": 11,
    "Lei Delegada": 12,
    "Ordem de Serviço": 13,
    "Portaria": 14,
    "Resolução": 15,
}


# OBS:  not using situation because it is not working properly, situation will be inferred from the document text

VALID_SITUATIONS = [
    "Não consta revogação expressa"
]  # Almg does not have a situation field, invalid norms will have an indication in the document text

INVALID_SITUATIONS = []
# norms with these situations are invalid norms

# the reason to have invalid situations is in case we need to train a classifier to predict if a norm is valid or something else similar
SITUATIONS = VALID_SITUATIONS + INVALID_SITUATIONS


class MGAlmgScraper(BaseScaper):
    """Webscraper for Minas Gerais state legislation website (https://www.almg.gov.br)

    Example search request: https://www.almg.gov.br/atividade-parlamentar/leis/legislacao-mineira/?pagina=2&aba=pesquisa&q=&ano=1989&dataFim=&num=&grupo=4&ordem=0&pesquisou=true&dataInicio=&sit=1
    """

    def __init__(
        self,
        base_url: str = "https://www.almg.gov.br",
        **kwargs,
    ):
        super().__init__(base_url, types=TYPES, situations=SITUATIONS, **kwargs)
        self.docs_save_dir = self.docs_save_dir / "MINAS_GERAIS"
        self.params = {
            "pagina": "",
            "aba": "pesquisa",
            "q": "",
            "ano": "",
            "dataFim": "",
            "num": "",
            "grupo": "",
            "ordem": "0",
            "pesquisou": "true",
            "dataInicio": "",  # not using situation parameter because it is not working properly
        }
        self.reached_end_page = False
        # Initialize the FileSaver
        self.saver = FileSaver(self.docs_save_dir)

    def _format_search_url(self, norm_type_id: str, year: int, page: int) -> str:
        """Format url for search request"""
        self.params["grupo"] = norm_type_id
        self.params["ano"] = str(year)
        self.params["pagina"] = str(page)
        return f"{self.base_url}/atividade-parlamentar/leis/legislacao-mineira?{urlencode(self.params)}"

    def _get_docs_links(self, url: str) -> list:
        """Get documents html links from given page.
        Returns a list of dicts with keys 'title', 'summary', 'html_link'
        """
        soup = self._get_soup(url)

        if soup is None:
            return []

        docs = []

        items = soup.find_all("article") if soup else []
        # check if the page is empty
        if len(items) == 0:
            self.reached_end_page = True
            return []

        for item in items:
            title = item.find("a").text.strip()
            html_link = item.find("a")["href"]
            summary = item.find("div").next_sibling.text.strip()
            docs.append({"title": title, "summary": summary, "html_link": html_link})

        return docs

    def _get_doc_data(self, doc_info: dict) -> Optional[dict]:
        """Get document data from given document dict"""
        # remove html_link from doc_info
        html_link = doc_info.pop("html_link")
        url = urljoin(self.base_url, html_link)

        soup_data = self._get_soup(url)

        if soup_data is None:
            return None

        origin = soup_data.find("span", text="Origem")
        origin_text = ""
        if origin and origin.next_sibling and hasattr(origin.next_sibling, "text"):
            origin_text = origin.next_sibling.text.strip()
        else:  # may have multiple origens
            origin = soup_data.find("span", text="Origens")
            if origin:
                # <h2 class="d-none">PL&nbsp;PROJETO DE LEI&nbsp;1191/1964</h2>
                h2s = origin.find_all_next("h2", class_="d-none")
                if h2s:
                    origin_text = ", ".join([h2.text.strip() for h2 in h2s])

        situation = soup_data.find("span", text="Situação")
        situation_text = ""
        if (
            situation
            and situation.next_sibling
            and hasattr(situation.next_sibling, "text")
        ):
            situation_text = situation.next_sibling.text.strip().capitalize()

        publication = soup_data.find("span", text="Fonte")
        publication_text = ""
        if publication:
            pub_div = publication.find_next("div")
            if pub_div and hasattr(pub_div, "text"):
                publication_text = pub_div.text.strip()

        tags = soup_data.find("span", text="Resumo")
        tags_text = ""
        if tags and tags.next_sibling and hasattr(tags.next_sibling, "text"):
            tags_text = tags.next_sibling.text.strip()

        subject = soup_data.find("span", text="Assunto Geral")
        subject_text = ""
        if subject and subject.next_sibling and hasattr(subject.next_sibling, "text"):
            subject_text = subject.next_sibling.text.strip()

        # get link for real html (first look for Text atualizado, if not found, look for Texto original)
        html_link_text = None

        # Look for texts that could contain the link
        for elem in soup_data.find_all(
            text=re.compile("|".join(["Texto atualizado", "Texto original"]))
        ):
            parent = elem.parent
            if parent:
                # Find the nearest a tag that has an href attribute
                a_tag = parent.find("a") or parent
                if a_tag and a_tag.has_attr("href"):
                    html_link_text = a_tag["href"]
                    break

        if not html_link_text:
            return None

        html_link = urljoin(self.base_url, html_link_text)
        if (
            html_link == self.base_url
        ):  # norm is invalid because it does not have a link to the document text
            return None

        soup = self._get_soup(html_link)
        if soup is None:
            return None

        text_norm_span = soup.find("span", class_="textNorma")
        if text_norm_span is None:
            return None

        data = {
            **doc_info,
            "origin": origin_text,
            "situation": situation_text,
            "publication": publication_text,
            "tags": tags_text,
            "subject": subject_text,
        }

        # <p>OBSERVAÇÃO: A imagem da lei está disponível em:</p>
        # <a href="https://mediaserver.almg.gov.br/acervo/191/945/1191945.pdf">https://mediaserver.almg.gov.br/acervo/191/945/1191945.pdf</a>

        # check if "A imagem da lei está disponível em" is in the text, in that case get link to pdf
        a_tag = text_norm_span.find("a", href=re.compile("mediaserver"))
        if a_tag:
            print(
                f"Document {data.get('title', '')} is an image PDF, extracting text from image. URL: {html_link}"
            )
            pdf_link = a_tag["href"]
            pdf_content = self._make_request(pdf_link).content
            text_markdown = self._get_pdf_image_markdown(pdf_content)

            if not text_markdown.replace(".", "").strip():
                return None

            return {
                **data,
                "html_string": "",
                "text_markdown": text_markdown,
                "document_url": pdf_link,
            }

        # Use str() if prettify() is not available
        # Use string representation for all elements to avoid prettify issues
        norm_text = str(text_norm_span)

        # remove Data da última atualização: 14/09/2007 from text
        norm_text = re.sub(
            r"Data da última atualização: \d{2}/\d{2}/\d{4}", "", norm_text
        )

        if not norm_text:  # some documents are not available, so we skip them
            return None

        html_string = f"<html><body>{norm_text}</body></html>"

        buffer = BytesIO()
        buffer.write(html_string.encode())
        buffer.seek(0)

        text_markdown = self._get_markdown(stream=buffer)

        # some invalid documents have only a '.' as text
        if not text_markdown.replace(".", "").strip():
            return None

        return {
            **data,
            "html_string": html_string,
            "text_markdown": text_markdown,
            "document_url": html_link,
        }

    def _scrape_year(self, year: int) -> List[Dict]:
        """Scrape norms for a specific year"""
        all_results = []
        
        for situation in tqdm(
            self.situations,
            desc="MINAS GERAIS | Situations",
            total=len(self.situations),
            disable=not self.verbose,
        ):
            # Convert self.types to a dictionary if it's not already
            types_dict = (
                self.types
                if isinstance(self.types, dict)
                else {k: i for i, k in enumerate(self.types)}
            )

            for norm_type, norm_type_id in tqdm(
                types_dict.items(),
                desc=f"MINAS GERAIS | Year: {year} | Types",
                total=len(types_dict),
                disable=not self.verbose,
            ):

                # total pages info is not available, so we need to check if the page is empty. In order to make parallel calls, we will assume an initial number of pages and increase if needed. We will know that all the pages were scraped when we request a page and it shows a error message

                total_pages = 1  # just to start and avoid making a lot of requests for empty pages
                self.reached_end_page = False

                # Get documents html links
                documents = []
                while not self.reached_end_page:
                    start_page = 1

                    with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                        futures = [
                            executor.submit(
                                self._get_docs_links,
                                self._format_search_url(norm_type_id, year, page),
                            )
                            for page in range(start_page, total_pages + 1)
                        ]

                        for future in tqdm(
                            as_completed(futures),
                            total=total_pages - start_page + 1,
                            desc="MINAS GERAIS | Get document link",
                            disable=not self.verbose,
                        ):
                            docs = future.result()
                            if docs:
                                documents.extend(docs)

                    start_page += total_pages
                    total_pages += self.max_workers

                # Get document data
                results = []
                with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                    futures = [
                        executor.submit(self._get_doc_data, doc_info)
                        for doc_info in documents
                    ]
                    for future in tqdm(
                        as_completed(futures),
                        total=len(documents),
                        desc="MINAS GERAIS | Get document data",
                        disable=not self.verbose,
                    ):
                        result = future.result()
                        if result is None:
                            continue

                        # situation will appear only in invalid norms
                        if not result["situation"]:
                            result["situation"] = situation

                        # save to results
                        queue_item = {
                            "year": year,
                            "type": norm_type,
                            **result,
                        }

                        results.append(queue_item)

                all_results.extend(results)
                self.results.extend(results)
                self.count += len(results)

                if self.verbose:
                    print(
                        f"Finished scraping for Year: {year} | Situation: {situation} | Type: {norm_type} | Results: {len(results)} | Total: {self.count}"
                    )
        
        return all_results
