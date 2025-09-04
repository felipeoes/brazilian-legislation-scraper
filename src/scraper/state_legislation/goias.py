from typing import Optional, Union
from io import BytesIO
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
from urllib.parse import urlencode, urljoin
from src.scraper.base.scraper import BaseScaper

TYPES = {
    "Constituição Estadual": {"id": 12, "url_suffix": "constituicao-estadual"},
    "Emenda Constitucional": {"id": 13, "url_suffix": "emenda-constitucional"},
    "Lei Complementar": {"id": 1, "url_suffix": "lei-complementar"},
    "Lei Ordinária": {"id": 2, "url_suffix": "lei"},
    "Lei Delegada": {"id": 4, "url_suffix": "lei-delegada"},
    "Decreto Lei": {"id": 8, "url_suffix": "decreto-lei"},
    "Decreto Numerado": {"id": 3, "url_suffix": "decreto"},
    "Decreto Orçamentário": {"id": 5, "url_suffix": "decreto-orcamentario"},
    "Portaria Orçaentária": {"id": 6, "url_suffix": "portaria-orcamentaria"},
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
        self._initialize_saver()

    def _format_search_url(self, norm_type_id: str, year: int, page: int = 1) -> str:
        self.params["ano"] = year
        self.params["tipo_legislacao"] = norm_type_id
        self.params["page"] = page
        return f"{self.base_url}?{urlencode(self.params)}"

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
            "summary": doc_detail["ementa"],
        }

        # Check if we have formatted content (HTML)
        if doc_detail.get("conteudo"):
            html_content = doc_detail["conteudo"]

            # Parse HTML with BeautifulSoup to clean it up
            soup = BeautifulSoup(html_content, "html.parser")

            # remove header table, if it contains GOVERNO DO ESTADO DE GOIÁS
            header_table = soup.find("table")
            if (
                header_table
                and "GOVERNO DO ESTADO DE GOIÁS".lower() in header_table.text.lower()
            ):
                header_table.decompose()

            pdf_link = ""

            # remove a tag it it has <img src="/assets/ver_lei.jpg">
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

            # Convert HTML to markdown using BytesIO buffer
            buffer = BytesIO()
            buffer.write(html_string.encode("utf-8"))
            buffer.seek(0)

            text_markdown = self._get_markdown(stream=buffer)
            if text_markdown:
                doc_info["text_markdown"] = text_markdown

                # check if text_markdown has substantial content after removing summary and possible error msg, if not, fetch text from PDF

                error_msg = "We're sorry but legisla\\_publico\\_vue doesn't work properly without JavaScript enabled. Please enable it to continue."

                new_text = (
                    text_markdown.lower().replace(error_msg.lower(), "")
                ).strip()
                if len(new_text) < 50:  # threshold for substantial content
                    print(f"Invalid  doc ID: {doc_id}. Year: {doc_detail['ano']}")
                    return None

                new_text = (
                    text_markdown.lower()
                    .replace(doc_info["summary"].lower(), "")
                    .strip()
                )
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
            print(
                f"Falling back to PDF for doc ID: {doc_id} | Year: {doc_detail['ano']}"
            )
            response = self._make_request(pdf_link)
            text_markdown = self._get_pdf_image_markdown(response.content)
            if text_markdown:
                doc_info["text_markdown"] = text_markdown
                if not doc_info.get("document_url"):
                    doc_info["document_url"] = pdf_link
                else:
                    doc_info["pdf_link"] = pdf_link
            else:
                print(f"Failed to extract text from PDF for doc ID: {doc_id}")
                return None

        # clean text_markdown (some docs may have the "javascript:print()" string at the end of the document)
        doc_info["text_markdown"] = (
            doc_info["text_markdown"].replace("javascript:print()", "").strip()
        )

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

    def _scrape_year(self, year: int):
        """Scrape norms for a specific year"""
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
                            # save to one drive
                            queue_item = {
                                "year": year,
                                "type": norm_type,
                                **norm,
                            }

                            self.queue.put(queue_item)
                            results.append(queue_item)

                    except Exception as e:
                        print(f"Error getting document data | Error: {e}")

            self.results.extend(results)
            self.count += len(results)

            if self.verbose:
                print(
                    f"Finished scraping for Year: {year} | Type: {norm_type} | Results: {len(results)} | Total: {self.count}"
                )
