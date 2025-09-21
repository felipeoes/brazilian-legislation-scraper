import warnings
import re

from datetime import datetime
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
from src.scraper.base.scraper import BaseScaper
from src.database.saver import FileSaver

warnings.filterwarnings("ignore")

TYPES = {
    "Lei Ordinária": "lei_ordinarias",
    "Lei Complementar": "lei_complementares",
    "Constituição Estadual": "detalhar_constituicao",  # texto completo, modificar a lógica no scraper
    "Decreto": "lei_decretos",
}

VALID_SITUATIONS = [
    "Não consta revogação expressa",
]  # Legis - Acre only publishes norms that are currently valid (no explicit revocation)

INVALID_SITUATIONS = (
    []
)  # norms with these situations are invalid norms (no longer have legal effect)

# the reason to have invalid situations is in case we need to train a classifier to predict if a norm is valid or something else similar
SITUATIONS = VALID_SITUATIONS + INVALID_SITUATIONS


class AcreLegisScraper(BaseScaper):
    """Webscraper for Legis - Acre website (https://legis.ac.gov.br)

    Example search request: https://legis.ac.gov.br/principal/1
    """

    def __init__(
        self,
        base_url: str = "https://legis.ac.gov.br/principal",
        **kwargs,
    ):
        super().__init__(base_url, types=TYPES, situations=SITUATIONS, **kwargs)
        self.docs_save_dir = self.docs_save_dir / "ACRE"
        self.year_regex = re.compile(r"\d{4}")
        self.remove_markdown_header = """\n\n# Reportar Erro\n\nNome\n\nEmail\n\nDescrição\n\nEnviar\nCancelar\n\n# Informações de Contato\n\nNome\n\nEmail\n\nAssunto\n\nDescrição\n\nEnviar\nCancelar\n\n[![](https://legis.ac.gov.br/assets/img/logo.svg)](https://legis.ac.gov.br/)\n\n* ac.gov.br\n* Diário Oficial\n* Notícias\n* Sobre\n* [Normas Covid-19](https://legis.ac.gov.br/covid19)\n\n* LEIS ORDINÁRIAS\n* LEIS COMPLEMENTARES\n* [CONSTITUIÇÃO ESTADUAL](https://legis.ac.gov.br/detalhar_constituicao)\n* DECRETOS\n\nTodos\nDecretos\nLei Complementar\nLei Ordinária\nConstituição Estadual\nEmendas Constitucionais\n\nEscolha a autoria\nAssembleia Legislativa do Estado do Acre\nDefensoria Pública do Estado do Acre\nMinistério Público do Estado do Acre\nPoder Executivo do Estado do Acre\nTribunal de Contas do Estado do Acre\nTribunal de Justiça do Estado do Acre\n\n**PESQUISAR**\n\n![](https://legis.ac.gov.br/assets/img/logo2.svg)\n\n* [INÍCIO](https://legis.ac.gov.br/)\n* LEIS ORDINÁRIAS\n* LEIS COMPLEMENTARES\n* [CONSTITUIÇÃO ESTADUAL](https://legis.ac.gov.br/detalhar_constituicao)\n* DECRETOS\n* [NORMAS COVID-19](https://legis.ac.gov.br/covid19)\n\n* PDF\n* INFORMAÇÃO\n\n"""
        self.remove_markdown_footer = """\n\n| NOME DO ARQUIVO | LINK PARA DOWNLOAD |\n| --- | --- |\n\n#### Informações sobre a legislação\n\n# Relacionados\n\n* [Governo do Estado do Acre](https://www.ac.gov.br/)\n* [Secretaria de Estado da Casa Civil](https://www.casacivil.ac.gov.br/)\n* [Diário Oficial do Estado do Acre](diario.ac.gov.br/)\n* [Assembleia Legislativa do Estado do Acre](http://www.al.ac.leg.br/)\n\n# Serviços\n\n* [Perguntas Frequentes](https://legis.ac.gov.br/perguntas_frequentes)\n* Reporte um erro\n* Fale Conosco\n* [Mapa do Site](https://legis.ac.gov.br/mapa_site)\n\n# Links Externos\n\n* [Procuradoria Geral do Estado do Acre](http://www.pge.ac.gov.br/)\n* [Ministério Público do Estado do Acre](https://www.mpac.mp.br/)\n* [Defensoria Pública do Estado do Acre](http://defensoria.ac.gov.br/)\n* [Ministério Público de Contas do Acre](http://mpc.tce.ac.gov.br/)\n* [Tribunal de Contas do Estado do Acre](http://www.tce.ac.gov.br/)\n\nSecretaria de Estado da Casa Civil | CASA CIVIL\nAv. Brasil, 307-447 - Centro, Rio Branco - AC\n\n2025 Governo do Estado do Acre\nCopyright Todos os direitos reservados\n\nSecretaria de Estado da Casa Civil\nDiretoria de Modernização\n\n"""
        self.saver = FileSaver(self.docs_save_dir)

    def _format_search_url(self, norm_type_id: str) -> str:
        """Format url for search request"""
        return f"{self.base_url}/{norm_type_id}"

    def _get_docs_links(self, soup: BeautifulSoup, norm_type_id: str) -> list:
        """Get documents html links from soup object.
        Returns a list of dicts with keys 'title', 'year', 'summary' and 'html_link'
        """

        # get all tr's from table that is within the div with id == norm_type_id
        trs = (
            soup.find("div", id=norm_type_id)
            .find("table")
            .find_all("tr", {"class": "visaoQuadrosTr"})
        )

        # get all html links
        html_links = []
        for tr in trs:
            a = tr.find("a")
            title = a.text.strip()
            html_link = a["href"]
            summary = tr.find("td").find_next("td").text.strip()
            year = self.year_regex.search(title.split(",")[-1]).group()
            html_links.append(
                {
                    "title": title,
                    "year": year,
                    "summary": summary,
                    "html_link": html_link,
                }
            )

        return html_links

    def _get_doc_data(self, doc_info: dict) -> dict:
        """Get document data from given html link"""
        doc_html_link = doc_info["html_link"]
        doc_title = doc_info["title"]
        doc_year = doc_info["year"]
        doc_summary = doc_info["summary"]

        response = self._make_request(doc_html_link)
        if response is None:
            return None

        soup = BeautifulSoup(response.text, "html.parser")
        html_string = soup.find("div", id="body-law")
        if not html_string:
            soup.find("div", id="exportacao")

        html_string = html_string.prettify() if html_string else ""

        # get text markdown
        text_markdown = self._get_markdown(response=response)

        if text_markdown is None:
            return None
        else:
            text_markdown = text_markdown.replace(
                self.remove_markdown_header, ""
            ).replace(self.remove_markdown_footer, "")

        return {
            "title": doc_title,
            "year": doc_year,
            "summary": doc_summary,
            "html_string": html_string,
            "text_markdown": text_markdown,
            "document_url": doc_html_link,
        }

    def _get_state_constitution(self, norm_type_id: str) -> dict:
        """Get state constitution data"""
        document_url = f"{self.base_url.replace('/principal', '')}/{norm_type_id}"
        response = self._make_request(document_url)

        soup = BeautifulSoup(response.text, "html.parser")
        html_string = soup.find("div", id="exportacao").prettify()

        # get text markdown
        text_markdown = self._get_markdown(response=response)

        text_markdown = text_markdown.replace(self.remove_markdown_header, "").replace(
            self.remove_markdown_footer, ""
        )

        return {
            "title": "Constituição Estadual",
            "year": datetime.now().year,
            "summary": "Constituição Estadual do Estado do Acre",
            "html_string": html_string,
            "text_markdown": text_markdown,
            "document_url": document_url,
        }

    def scrape(self):
        """Scrape norms"""

        for situation in tqdm(
            self.situations,
            desc="ACRE | Situations",
            total=len(self.situations),
            disable=not self.verbose,
        ):

            # all the norms and types are in the same page, so we just need to make one request to get html links

            url = self._format_search_url(1)
            soup = self._get_soup(url)
            if soup is None:
                continue

            for norm_type, norm_type_id in self.types.items():

                # if it's state constitution, we need to change logic. All the text is within div class="exportacao"
                if norm_type == "Constituição Estadual":
                    doc_info = self._get_state_constitution(norm_type_id)
                    doc_info["situation"] = situation
                    doc_info["type"] = norm_type

                    all_results.append(doc_info)
                    self.results.append(doc_info)
                    self.count += 1
                    continue

                html_links = self._get_docs_links(soup, norm_type_id)
                results = []

                # Get data from all  documents text links using ThreadPoolExecutor
                with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                    futures = [
                        executor.submit(self._get_doc_data, doc) for doc in html_links
                    ]

                    for future in tqdm(
                        as_completed(futures),
                        desc=f"ACRE | Type: {norm_type}",
                        total=len(html_links),
                        disable=not self.verbose,
                    ):
                        result = future.result()

                        if result is None:
                            continue

                        # prepare item for saving
                        queue_item = {
                            # "year": year, # getting year from document title because Legis does not have a search by year
                            # website only shows documents without any revocation
                            "situation": situation,
                            "type": norm_type,
                            **result,
                        }

                        results.append(queue_item)

                if results:
                    self.saver.save(results)
                    self.results.extend(results)
                    self.count += len(results)

                if self.verbose:
                    print(
                        f"Type: {norm_type} | Situation: {situation} | Total: {len(results)}"
                    )

        return self.results
