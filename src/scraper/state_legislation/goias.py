import re
from typing import Optional, List, Dict
from io import BytesIO
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm.auto import tqdm
from urllib.parse import urlencode
from src.scraper.base.scraper import BaseScaper
from src.database.saver import FileSaver

TYPES = {
    "Constituição Estadual": {"id": 12, "url_suffix": "constituicao-estadual"},
    "Emenda Constitucional": {"id": 13, "url_suffix": "emenda-constitucional"},
    "Lei Complementar": {"id": 1, "url_suffix": "lei-complementar"},
    "Lei Ordinária": {"id": 2, "url_suffix": "lei"},
    "Lei Delegada": {"id": 4, "url_suffix": "lei-delegada"},
    "Decreto Lei": {"id": 8, "url_suffix": "decreto-lei"},
    "Decreto Numerado": {"id": 3, "url_suffix": "decreto"},
    "Decreto Orçamentário": {"id": 5, "url_suffix": "decreto-orcamentario"},
    "Portaria Orçamentária": {"id": 6, "url_suffix": "portaria-orcamentaria"},
    "Resolução": {"id": 7, "url_suffix": "resolucao"},
}

# situations are gotten from doc data while scraping
VALID_SITUATIONS = []
INVALID_SITUATIONS = (
    []
)  # norms with these situations are invalid norms (no longer have legal effect)

# the reason to have invalid situations is in case we need to train a classifier to predict if a norm is valid or something else similar
SITUATIONS = VALID_SITUATIONS + INVALID_SITUATIONS


class LegislaGoias(BaseScaper):
    """Webscraper for Espirito Santo state legislation website (https://legisla.casacivil.go.gov.br)

    Example search request: https://legisla.casacivil.go.gov.br/api/v2/pesquisa/legislacoes?ano=1798&ordenarPor=data&page=1&qtd_por_pagina=10&tipo_legislacao=7
    """

    def __init__(
        self,
        base_url: str = "https://legisla.casacivil.go.gov.br/api/v2/pesquisa/legislacoes",
        **kwargs,
    ):
        super().__init__(base_url, types=TYPES, situations=SITUATIONS, **kwargs)
        self.params = {
            "ano": 1800,
            "ordenarPor": "data",
            "qtd_por_pagina": 100,
            "tipo_legislacao": "",
            "page": 1,
        }
        self.docs_save_dir = self.docs_save_dir / "GOIAS"
        self._special_regex = re.compile(r"[^a-zA-ZÀ-ÿ\s]")
        self._space_regex = re.compile(r"\s+")
        # Initialize the FileSaver
        self.saver = FileSaver(self.docs_save_dir)

    def _format_search_url(self, norm_type_id: str, year: int, page: int = 1) -> str:
        self.params["ano"] = year
        self.params["tipo_legislacao"] = norm_type_id
        self.params["page"] = page
        return f"{self.base_url}?{urlencode(self.params)}"

    def _clean_markdown(self, text_markdown: str) -> str:
        """Clean markdown text"""

        return text_markdown.replace("javascript:print()", "").strip()

    def _process_pdf_link(
        self, link: str, doc_id: str, doc_info: dict
    ) -> Optional[dict]:
        response = self._make_request(link)
        if not response:
            print(f"Error fetching PDF for doc ID: {doc_id} | Link: {link}")
            return None

        text_markdown = self._get_pdf_image_markdown(response.content)
        if text_markdown:
            doc_info["text_markdown"] = text_markdown
            if not doc_info.get("document_url"):
                doc_info["document_url"] = link
            else:
                doc_info["pdf_link"] = link
        else:
            print(f"Failed to extract text from PDF for doc ID: {doc_id}")
            return None

        doc_info["text_markdown"] = self._clean_markdown(doc_info["text_markdown"])

        return doc_info

    def _get_doc_info(self, doc: dict, norm_url_suffix: str) -> Optional[dict]:
        """Get document info from given doc data using API"""
        doc_id = doc["id"]

        # Use the API endpoint to get detailed document information
        api_url = (
            f"https://legisla.casacivil.go.gov.br/api/v2/pesquisa/legislacoes/{doc_id}"
        )
        response = self._make_request(api_url)

        if not response:
            print(f"Error getting detailed data for doc ID: {doc_id}")
            return {}

        doc_detail = response.json()

        doc_info = {
            "id": doc_detail["id"],
            "norm_number": doc_detail["numero"],
            "situation": doc_detail.get("estado_legislacao", {}).get("nome", ""),
            "date": doc_detail["data_legislacao"],
            "title": f'{doc_detail["tipo_legislacao"]["nome"]} {doc_detail["numero"]} de {doc_detail["ano"]}',
            "summary": doc_detail["ementa"].strip(),
        }

        # Check if we have formatted content (HTML)
        if doc_detail.get("conteudo"):
            html_content = doc_detail["conteudo"]

            # Parse HTML with BeautifulSoup to clean it up
            soup = BeautifulSoup(html_content, "html.parser")

            # check if "Clique no link abaixo para acessar a:" in soup and skip document
            if (
                "Clique no link abaixo para acessar a:".lower()
                in str(soup.prettify()).lower()
            ):
                return None

            # remove header table, if it contains GOVERNO DO ESTADO DE GOIÁS
            header_table = soup.find("table")
            if (
                header_table
                and "GOVERNO DO ESTADO DE GOIÁS".lower() in header_table.text.lower()
            ):
                header_table.decompose()

            pdf_link = ""

            # remove a tag if it has <img src="/assets/ver_lei.jpg">
            for a_tag in soup.find_all("a"):
                img = a_tag.find("img", src="/assets/ver_lei.jpg")
                if img:
                    # get pdf link because we will need it later if html content is insufficient
                    pdf_link = a_tag["href"]
                    a_tag.decompose()

            html_string = soup.prettify().strip()

            # Ensure we have a complete HTML document for markitdown
            if not html_string.startswith("<html"):
                html_string = f"<html><body>{html_string}</body></html>"

            doc_info["html_string"] = html_string

            # check if botao-baixar in soup
            baixar_div = soup.find("div", class_="botao-baixar")
            if baixar_div:
                a_tag = baixar_div.find("a", href=True)
                pdf_link = a_tag["href"]

                doc_info = self._process_pdf_link(pdf_link, doc_id, doc_info)

                if not doc_info:
                    return None

                return doc_info

            # Convert HTML to markdown using BytesIO buffer
            buffer = BytesIO()
            buffer.write(html_string.encode("utf-8"))
            buffer.seek(0)

            text_markdown = self._get_markdown(stream=buffer)
            if text_markdown:
                text_markdown = text_markdown.strip()
                doc_info["text_markdown"] = text_markdown

                new_text = self._special_regex.sub("", text_markdown.lower())
                new_text = self._space_regex.sub("", new_text).strip()
                compare_summary = self._special_regex.sub(
                    "", doc_info["summary"].lower()
                )
                compare_summary = self._space_regex.sub("", compare_summary).strip()

                new_text = new_text.replace(compare_summary, "").strip()
                if (
                    len(new_text) < 150
                ):  # threshold for substantial content (based on experimentation with goias norms)
                    # set text_markdown to None so that we can fall back to PDF fetching below
                    doc_info["text_markdown"] = None

            # Build the HTML link for reference
            if norm_url_suffix == "constituicao-estadual":
                html_link = f"https://legisla.casacivil.go.gov.br/pesquisa_legislacao/{doc_id}/{norm_url_suffix}"
            else:
                html_link = f'https://legisla.casacivil.go.gov.br/pesquisa_legislacao/{doc_id}/{norm_url_suffix}-{doc_detail["numero"]}'

            doc_info["document_url"] = html_link

        # If we don't have HTML content or markdown conversion failed, try PDF
        if not doc_info.get("text_markdown"):
            doc_info = self._process_pdf_link(pdf_link, doc_id, doc_info)
            if not doc_info:
                print(f"Failed to process PDF for doc ID: {doc_id}")
                return None

        # clean text_markdown (some docs may have the "javascript:print()" string at the end of the document)
        doc_info["text_markdown"] = self._clean_markdown(doc_info["text_markdown"])

        # check for error msg
        error_msg = "doesn't work properly without JavaScript enabled"
        if error_msg.lower() in doc_info["text_markdown"].lower():
            print(f"Invalid  doc ID: {doc_id}. Year: {doc_detail['ano']}")
            return None

        return doc_info

    def _get_doc_data(self, url: str, norm_url_suffix: str) -> list[dict]:
        """Get document data from given url"""
        response = self._make_request(url)

        if not response:
            print(f"Error getting data from URL: {url}")
            return []

        response = response.json()

        total_results = response["total_resultados"]
        if total_results == 0:
            return []

        data = response["resultados"]
        docs = []

        # concurrent processing

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = [
                executor.submit(self._get_doc_info, doc, norm_url_suffix)
                for doc in data
            ]

            for future in tqdm(
                as_completed(futures),
                desc="GOIAS | Get document info",
                total=len(futures),
                disable=not self.verbose,
            ):
                doc_info = future.result()
                if doc_info:
                    docs.append(doc_info)

        return docs

    def _scrape_year(self, year: int) -> List[Dict]:
        """Scrape norms for a specific year"""
        all_results = []

        for norm_type, norm_type_data in tqdm(
            self.types.items(),
            desc=f"GOIAS | Year: {year} | Types",
            total=len(self.types),
            disable=not self.verbose,
        ):
            norm_type_id = norm_type_data["id"]
            url = self._format_search_url(norm_type_id, year, 0)
            response = self._make_request(url)

            if not response:
                print(f"Error getting data for Year: {year} | Type: {norm_type}")
                continue

            data = response.json()
            total_results = data["total_resultados"]

            if total_results == 0:
                continue

            pages = total_results // 100 + 1

            # get all norms
            results = []
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                futures = [
                    executor.submit(
                        self._get_doc_data,
                        self._format_search_url(norm_type_id, year, page),
                        norm_type_data["url_suffix"],
                    )
                    for page in range(1, pages + 1)
                ]

                for future in tqdm(
                    as_completed(futures),
                    desc="GOIAS | Get document data",
                    total=len(futures),
                    disable=not self.verbose,
                ):

                    try:
                        norms = future.result()
                        if not norms:
                            continue

                        for norm in norms:
                            # save to results
                            queue_item = {
                                "year": year,
                                "type": norm_type,
                                **norm,
                            }
                            results.append(queue_item)

                    except Exception as e:
                        print(f"Error getting document data | Error: {e}")

            all_results.extend(results)
            self.results.extend(results)

        return all_results
