import re
from io import BytesIO
from loguru import logger
from src.scraper.base.scraper import StateScraper
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


class MGAlmgScraper(StateScraper):
    """Webscraper for Minas Gerais state legislation website (https://www.almg.gov.br)

    Example search request: https://www.almg.gov.br/atividade-parlamentar/leis/legislacao-mineira/?pagina=2&aba=pesquisa&q=&ano=1989&dataFim=&num=&grupo=4&ordem=0&pesquisou=true&dataInicio=&sit=1
    """

    def __init__(
        self,
        base_url: str = "https://www.almg.gov.br",
        **kwargs,
    ):
        super().__init__(
            base_url, types=TYPES, situations=SITUATIONS, name="MINAS_GERAIS", **kwargs
        )

    def _build_search_url(self, norm_type_id: str, year: int, page: int) -> str:
        """Build search URL from arguments (no shared state mutation)."""
        params = {
            "pagina": str(page),
            "aba": "pesquisa",
            "q": "",
            "ano": str(year),
            "dataFim": "",
            "num": "",
            "grupo": norm_type_id,
            "ordem": "0",
            "pesquisou": "true",
            "dataInicio": "",
        }
        return f"{self.base_url}/atividade-parlamentar/leis/legislacao-mineira?{urlencode(params)}"

    async def _get_docs_links(self, url: str) -> tuple[list, bool]:
        """Get documents html links from given page.
        Returns (docs, reached_end) where docs is a list of dicts and
        reached_end indicates there are no more pages.
        """
        soup = await self.request_service.get_soup(url)

        if soup is None:
            return [], False

        docs = []

        items = soup.find_all("article") if soup else []
        # check if the page is empty
        if len(items) == 0:
            return [], True

        for item in items:
            title = item.find("a").text.strip()
            html_link = item.find("a")["href"]
            summary = item.find("div").next_sibling.text.strip()
            docs.append({"title": title, "summary": summary, "html_link": html_link})

        return docs, False

    async def _get_doc_data(self, doc_info: dict) -> dict | None:
        """Get document data from given document dict"""
        # remove html_link from doc_info
        html_link = doc_info.pop("html_link")
        url = urljoin(self.base_url, html_link)

        soup_data = await self.request_service.get_soup(url)

        if soup_data is None:
            await self._save_doc_error(
                title=doc_info.get("title", "Unknown"),
                year="",
                situation="",
                norm_type="",
                html_link=url,
                error_message="Failed to fetch document page (soup is None)",
            )
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
            await self._save_doc_error(
                title=doc_info.get("title", "Unknown"),
                year="",
                situation="",
                norm_type="",
                html_link=url,
                error_message="No text link found (Texto atualizado/original)",
            )
            return None

        html_link = urljoin(self.base_url, html_link_text)

        if self._is_already_scraped(html_link, doc_info.get("title", "")):
            return None

        if (
            html_link == self.base_url
        ):  # norm is invalid because it does not have a link to the document text
            await self._save_doc_error(
                title=doc_info.get("title", "Unknown"),
                year="",
                situation="",
                norm_type="",
                html_link=url,
                error_message="Document link resolves to base URL (no document text available)",
            )
            return None

        soup = await self.request_service.get_soup(html_link)
        if soup is None:
            await self._save_doc_error(
                title=doc_info.get("title", "Unknown"),
                year="",
                situation="",
                norm_type="",
                html_link=html_link,
                error_message="Failed to fetch document text page (soup is None)",
            )
            return None

        text_norm_span = soup.find("span", class_="textNorma")
        if text_norm_span is None:
            await self._save_doc_error(
                title=doc_info.get("title", "Unknown"),
                year="",
                situation="",
                norm_type="",
                html_link=html_link,
                error_message="Could not find span.textNorma in document page",
            )
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
            logger.info(
                f"Document {data.get('title', '')} is an image PDF, extracting text from image. URL: {html_link}"
            )
            pdf_link = a_tag["href"]

            if self._is_already_scraped(pdf_link, doc_info.get("title", "")):
                return None

            pdf_response = await self.request_service.make_request(pdf_link)
            if pdf_response is None:
                await self._save_doc_error(
                    title=data.get("title", "Unknown"),
                    year="",
                    situation="",
                    norm_type="",
                    html_link=pdf_link,
                    error_message="Failed to download image PDF",
                )
                return None
            pdf_content = await pdf_response.read()
            text_markdown = await self._get_markdown(stream=BytesIO(pdf_content))

            if not text_markdown.replace(".", "").strip():
                await self._save_doc_error(
                    title=data.get("title", "Unknown"),
                    year="",
                    situation="",
                    norm_type="",
                    html_link=pdf_link,
                    error_message="PDF image contains only dots (invalid content)",
                )
                return None

            return {
                **data,
                "html_string": "",
                "text_markdown": text_markdown,
                "document_url": pdf_link,
                "_raw_content": pdf_content,
                "_content_extension": ".pdf",
            }

        # Use str() if prettify() is not available
        # Use string representation for all elements to avoid prettify issues
        norm_text = str(text_norm_span)

        # remove Data da última atualização: 14/09/2007 from text
        norm_text = re.sub(
            r"Data da última atualização: \d{2}/\d{2}/\d{4}", "", norm_text
        )

        if not norm_text:  # some documents are not available, so we skip them
            await self._save_doc_error(
                title=doc_info.get("title", "Unknown"),
                year="",
                situation="",
                norm_type="",
                html_link=html_link,
                error_message="Norm text is empty after extraction",
            )
            return None

        html_string = f"<html><body>{norm_text}</body></html>"

        text_markdown = await self._get_markdown(html_content=html_string)

        # some invalid documents have only a '.' as text
        if not text_markdown.replace(".", "").strip():
            await self._save_doc_error(
                title=doc_info.get("title", "Unknown"),
                year="",
                situation="",
                norm_type="",
                html_link=html_link,
                error_message="Markdown text contains only dots (invalid content)",
            )
            return None

        return {
            **data,
            "html_string": html_string,
            "text_markdown": text_markdown,
            "document_url": html_link,
            "_raw_content": html_string.encode("utf-8"),
            "_content_extension": ".html",
        }

    async def _scrape_situation_type(
        self, situation: str, norm_type: str, norm_type_id: str, year: int
    ) -> list[dict]:
        """Scrape norms for a specific situation and type"""
        # total pages info is not available, so we need to check if the page is empty. In order to make parallel calls, we will assume an initial number of pages and increase if needed. We will know that all the pages were scraped when we request a page and it shows a error message

        total_pages = (
            1  # just to start and avoid making a lot of requests for empty pages
        )
        reached_end_page = False

        # Get documents html links
        documents = []
        while not reached_end_page:
            start_page = 1

            tasks = [
                self._get_docs_links(self._build_search_url(norm_type_id, year, page))
                for page in range(start_page, total_pages + 1)
            ]
            valid_results = await self._gather_results(
                tasks,
                context={
                    "year": year,
                    "type": norm_type,
                    "situation": situation,
                },
                desc=f"MINAS GERAIS | {norm_type} | get_docs_links",
            )
            for result in valid_results:
                docs, ended = result
                if ended:
                    reached_end_page = True
                if docs:
                    documents.extend(docs)

            start_page += total_pages
            total_pages += self.max_workers

        # Get document data
        results = []
        tasks = [self._get_doc_data(doc_info) for doc_info in documents]
        valid_results = await self._gather_results(
            tasks,
            context={"year": year, "type": norm_type, "situation": situation},
            desc=f"MINAS GERAIS | {norm_type}",
        )
        for result in valid_results:
            if not result["situation"]:
                result["situation"] = situation
            queue_item = {"year": year, "type": norm_type, **result}
            await self._save_doc_result(queue_item)
            results.append(queue_item)

        if self.verbose:
            logger.info(
                f"Finished scraping for Year: {year} | Situation: {situation} | Type: {norm_type} | Results: {len(results)}"
            )

        return results

    async def _scrape_year(self, year: int) -> list[dict]:
        """Scrape norms for a specific year"""
        types_dict = (
            self.types
            if isinstance(self.types, dict)
            else {k: i for i, k in enumerate(self.types)}
        )
        tasks = [
            self._scrape_situation_type(sit, nt, nt_id, year)
            for sit in self.situations
            for nt, nt_id in types_dict.items()
        ]
        valid = await self._gather_results(
            tasks,
            context={"year": year, "type": "N/A", "situation": "N/A"},
            desc=f"{self.name} | Year {year}",
        )
        return [
            item
            for result in valid
            for item in (result if isinstance(result, list) else [result])
        ]
