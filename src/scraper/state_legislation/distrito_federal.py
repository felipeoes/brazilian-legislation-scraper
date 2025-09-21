from io import BytesIO
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
from src.scraper.base.scraper import BaseScaper
from src.database.saver import FileSaver

TYPES = {
    "Ato da Mesa Diretora": 17000000,
    "Ato Declaratório": 18000000,
    "Ato Declaratório Interpretativo": "7c5da8af85dd43b8973acaf39043a3d2",
    "Ato do Presidente": "18e34c5d799c445ab47df54cf6f1d2b9",
    "Ato Regimental": 20000000,
    "Decisão": 23000000,
    "Decreto": 27000000,
    "Decreto Executivo": 28000000,
    "Decreto Legislativo": 29000000,
    "Deliberação": "c870f54826864e6889ec08c7f3d9d8c2",
    "Despacho": 31000000,
    "Determinação": "b67f52a2c5a5471299f5ea2cc6c2aad5",
    "Emenda Regimental": 38000000,
    "Estatuto": 39000000,
    "Instrução": 41000000,
    "Instrução de Serviço": 43000000,
    "Instrução Normativa": 45000000,
    "Lei": 46000000,
    "Lei Complementar": 47000000,
    "Norma Técnica": 52000000,
    "Portaria": 59000000,
    "Ordem de Serviço": 53000000,
    "Ordem de Serviço Conjunta": 54000000,
    "Parecer Normativo": 57000000,
    "Parecer Referencial": "877d20147e02451e929fcfa80ae76de3",
    "Plano": 58000000,
    "Portaria": 59000000,
    "Portaria Conjunta": 60000000,
    "Portaria Normativa": 61000000,
    "Recomendação": 65000000,
    "Regimento": 66000000,
    "Regimento Interno": 67000000,
    "Regulamento": 68000000,
    "Resolução": 71000000,
    "Resolução Administrativa": 72000000,
    "Resolução Normativa": 75000000,
    "Resolução Ordinária": "037f6f0fc7a04d69834cf60007bba07d",
    "Súmula": 76000000,
    "Súmula Administrativa": "d74996b4f496432fa09fea831f4f72be",
}

VALID_SITUATIONS = {
    "Sem Revogação Expressa": "semrevogacaoexpressa",
    "Ajuizado": "ajuizado",
    "Alterado": "alterado",
    "Julgado Procedente": "julgadoprocedente",
    "Não conhecida": "naoconhecida",
}

INVALID_SITUATIONS = {
    "Anulado": "anulado",
    "Cancelado": "cancelado",
    "Cessar os efeitos": "cessarosefeitos",
    "Extinta": "extinta",
    "Inconstitucional": "inconstitucional",
    "Prejudicada": "prejudicada",
    "Revogado": "revogado",
    "Suspenso": "suspenso",
    "Sustado(a)": "sustado",
    "Tornado sem efeito": "tornadosemefeito",
}  # norms with these situations are invalid norms (no longer have legal effect)

# the reason to have invalid situations is in case we need to train a classifier to predict if a norm is valid or something else similar
SITUATIONS = VALID_SITUATIONS | INVALID_SITUATIONS


class DFSinjScraper(BaseScaper):
    """Webscraper for Distrito Federal state legislation website (https://www.sinj.df.gov.br/sinj/)

    Example search request: https://www.sinj.df.gov.br/sinj/ashx/Datatable/ResultadoDePesquisaNormaDatatable.ashx

    payload: {
        "bbusca": "sinj_norma",
        "iColumns": 9,
        "sColumns": ",,,,,,,,",
        "iDisplayStart": 0,
        "iDisplayLength": 100,
        "mDataProp_0": "_score",
        "sSearch_0": "",
        "bRegex_0": False,
        "bSearchable_0": True,
        "bSortable_0": False,
        "mDataProp_1": "_score",
        "sSearch_1": "",
        "bRegex_1": False,
        "bSearchable_1": True,
        "bSortable_1": True,
        "mDataProp_2": "nm_tipo_norma",
        "sSearch_2": "",
        "bRegex_2": False,
        "bSearchable_2": True,
        "bSortable_2": True,
        "mDataProp_3": "dt_assinatura",
        "sSearch_3": "",
        "bRegex_3": False,
        "bSearchable_3": True,
        "bSortable_3": True,
        "mDataProp_4": "origens",
        "sSearch_4": "",
        "bRegex_4": False,
        "bSearchable_4": True,
        "bSortable_4": False,
        "mDataProp_5": "ds_ementa",
        "sSearch_5": "",
        "bRegex_5": False,
        "bSearchable_5": True,
        "bSortable_5": False,
        "mDataProp_6": "nm_situacao",
        "sSearch_6": "",
        "bRegex_6": False,
        "bSearchable_6": True,
        "bSortable_6": True,
        "mDataProp_7": 7,
        "sSearch_7": "",
        "bRegex_7": False,
        "bSearchable_7": True,
        "bSortable_7": False,
        "mDataProp_8": 8,
        "sSearch_8": "",
        "bRegex_8": False,
        "bSearchable_8": True,
        "bSortable_8": False,
        "sSearch": "",
        "bRegex": False,
        "iSortCol_0": 1,
        "sSortDir_0": "desc",
        "iSortingCols": 1,
        "tipo_pesquisa": "avancada",
        "argumento": "autocomplete#ch_situacao#Situação#igual#igual a#semrevogacaoexpressa#Sem Revogação Expressa#E",
        "argumento": "number#ano_assinatura#Ano de Assinatura#igual#igual a#1960#1960#E",
        "ch_tipo_norma": 27000000,
    }
    """

    def __init__(
        self,
        base_url: str = "https://www.sinj.df.gov.br/sinj",
        **kwargs,
    ):
        super().__init__(base_url, types=TYPES, situations=SITUATIONS, **kwargs)
        self.docs_save_dir = self.docs_save_dir / "DISTRITO_FEDERAL"
        self.params = {
            "bbusca": "sinj_norma",
            "iColumns": 9,
            "sColumns": ",,,,,,,,",
            "iDisplayStart": 0,
            "iDisplayLength": 100,
            "mDataProp_0": "_score",
            "sSearch_0": "",
            "bRegex_0": False,
            "bSearchable_0": True,
            "bSortable_0": False,
            "mDataProp_1": "_score",
            "sSearch_1": "",
            "bRegex_1": False,
            "bSearchable_1": True,
            "bSortable_1": True,
            "mDataProp_2": "nm_tipo_norma",
            "sSearch_2": "",
            "bRegex_2": False,
            "bSearchable_2": True,
            "bSortable_2": True,
            "mDataProp_3": "dt_assinatura",
            "sSearch_3": "",
            "bRegex_3": False,
            "bSearchable_3": True,
            "bSortable_3": True,
            "mDataProp_4": "origens",
            "sSearch_4": "",
            "bRegex_4": False,
            "bSearchable_4": True,
            "bSortable_4": False,
            "mDataProp_5": "ds_ementa",
            "sSearch_5": "",
            "bRegex_5": False,
            "bSearchable_5": True,
            "bSortable_5": False,
            "mDataProp_6": "nm_situacao",
            "sSearch_6": "",
            "bRegex_6": False,
            "bSearchable_6": True,
            "bSortable_6": True,
            "mDataProp_7": 7,
            "sSearch_7": "",
            "bRegex_7": False,
            "bSearchable_7": True,
            "bSortable_7": False,
            "mDataProp_8": 8,
            "sSearch_8": "",
            "bRegex_8": False,
            "bSearchable_8": True,
            "bSortable_8": False,
            "sSearch": "",
            "bRegex": False,
            "iSortCol_0": 1,
            "sSortDir_0": "desc",
            "iSortingCols": 1,
            "tipo_pesquisa": "avancada",
            "argumento": "autocomplete#ch_situacao#Situação#igual#igual a#semrevogacaoexpressa#Sem Revogação Expressa#E",
            "argumento_situation": "number#ano_assinatura#Ano de Assinatura#igual#igual a#1960#1960#E",
            "ch_tipo_norma": 27000000,
        }
        self.total_pages_url = "https://www.sinj.df.gov.br/sinj/ashx/Consulta/TotalConsulta.ashx?bbusca=sinj_norma"
        self.session_id_created = False
        # Initialize the FileSaver
        self.saver = FileSaver(self.docs_save_dir)

    def _format_search_url(
        self,
        situation: str,
        situation_id: str,
        norm_type_id: str,
        year: int,
        page: int = 1,
    ) -> str:
        """Format url for search request"""
        self.params["argumento"] = (
            f"number#ano_assinatura#Ano de Assinatura#igual#igual a#{year}#{year}#E"
        )
        self.params["argumento_situation"] = (
            f"autocomplete#ch_situacao#Situação#igual#igual a#{situation_id}#{situation}#E"
        )
        self.params["ch_tipo_norma"] = norm_type_id

        self.params["iDisplayLength"] = 100
        self.params["iDisplayStart"] = (page - 1) * self.params["iDisplayLength"]

        return f"{self.base_url}/ashx/Datatable/ResultadoDePesquisaNormaDatatable.ashx"

    def _get_docs_links(self, url: str) -> list:
        """Get document links from search request. Returns a list of dicts with keys 'title', 'summary', 'date', 'html_link'"""
        payload = [
            (key, value) for key, value in self.params.items() if key != "argumento"
        ]
        payload.append(("argumento", self.params["argumento"]))
        payload.append(("argumento", self.params["argumento_situation"]))

        response = self._make_request(
            url,
            method="POST",
            payload=payload,
        )
        if response is None:
            return []

        def transform_norm_type(norm_type: str) -> str:
            # change all special characters to _
            new_chars = []
            for char in norm_type:
                if char.isalnum():
                    new_chars.append(char)
                else:
                    new_chars.append("_")

            return "".join(new_chars)

        data = response.json()

        docs = []
        for item in data["aaData"]:
            item_info = item["_source"]
            title = f"{item_info['nm_tipo_norma']} {item_info['nr_norma']} de {item_info['dt_assinatura']}"
            norm_number = item_info["nr_norma"]
            ch_norma = item_info["ch_norma"]
            norm_type = item_info["nm_tipo_norma"]
            dt_assinatura = item_info["dt_assinatura"]

            transformed_tipo_norma = transform_norm_type(norm_type)

            html_link = f"{self.base_url}/Norma/{ch_norma}/{transformed_tipo_norma}_{norm_number}_{dt_assinatura.replace('/', '_')}.html"
            docs.append(
                {
                    "title": title,
                    "summary": item_info["ds_ementa"],
                    "date": dt_assinatura,
                    "html_link": html_link,
                }
            )

        return docs

    def _get_doc_data(self, doc_info: dict) -> dict:
        """Get document data from html link"""

        try:
            # remove html link from doc_info
            html_link = doc_info.pop("html_link")
            response = self._make_request(html_link)

            soup = BeautifulSoup(response.content, "html.parser")

            # get id="div_texto"
            norm_text_tag = soup.find("div", id="div_texto")
            text_markdown = None
            if not norm_text_tag:
                # it may be a pdf file, try to get text markdown instead (without using LLM for image extraction)
                text_markdown = self._get_markdown(response=response)

                if not text_markdown:
                    return False  # invalid norm, not applying image extraction for distrito federal for now
            else:
                html_string = f"<html>{norm_text_tag.prettify()}</html>"

                buffer = BytesIO()
                buffer.write(html_string.encode())
                buffer.seek(0)

                # get markdown text
                text_markdown = (
                    self._get_markdown(stream=buffer)
                    if not text_markdown
                    else text_markdown
                )

                doc_info["html_string"] = html_string

            doc_info["text_markdown"] = text_markdown
            doc_info["document_url"] = html_link

            return doc_info
        except Exception as e:
            print(f"Error getting document data: {e}")
            return False

    def _scrape_year(self, year: int) -> list:
        """Scrape norms for a specific year"""
        all_results = []
        
        for situation, situation_id in tqdm(
            self.situations.items(),
            desc="DISTRITO FEDERAL | Situations",
            total=len(self.situations),
            disable=not self.verbose,
        ):
            for norm_type, norm_type_id in tqdm(
                self.types.items(),
                desc=f"DISTRITO FEDERAL | Year: {year} | Types",
                total=len(self.types),
                disable=not self.verbose,
            ):
                # need to make a get request first to create the session ID ( will be used in all subsequent requests)
                if not self.session_id_created:
                    get_url = (
                        self.base_url
                        + "/ashx/Cadastro/HistoricoDePesquisaIncluir.ashx?tipo_pesquisa=avancada&argumento=autocomplete%23ch_situacao%23Situa%C3%A7%C3%A3o%23igual%23igual+a%23semrevogacaoexpressa%23Sem+Revoga%C3%A7%C3%A3o+Expressa%23E&ch_tipo_norma=46000000&consulta=tipo_pesquisa=avancada&consulta=argumento=autocomplete%23ch_situacao%23Situa%C3%A7%C3%A3o%23igual%23igual+a%23semrevogacaoexpressa%23Sem+Revoga%C3%A7%C3%A3o+Expressa%23E&consulta=ch_tipo_norma=46000000&chave=6c31e2b0c76d4aa227cd6804bc4fc59f&total={%22nm_base%22:%22sinj_norma%22,%22ds_base%22:%22Normas%22,%22nr_total%22:6008}&_=1741738478078"
                    )
                    self._make_request(get_url)
                    self.session_id_created = True

                # try using payload tuples
                total_pages_request_params = [
                    ("tipo_pesquisa", "avancada"),
                    (
                        "argumento",
                        f"number#ano_assinatura#Ano de Assinatura#igual#igual a#{year}#{year}#E",
                    ),
                    (
                        "argumento",
                        f"autocomplete#ch_situacao#Situação#igual#igual a#{situation_id}#{situation}#E",
                    ),
                    ("ch_tipo_norma", norm_type_id),
                ]

                response = self._make_request(
                    self.total_pages_url,
                    method="POST",
                    payload=total_pages_request_params,
                )

                data = response.json()

                total_norms = data["counts"][0]["count"]
                # if count is 0, skip
                if total_norms == 0:
                    continue

                pages = total_norms // self.params["iDisplayLength"]
                if total_norms % self.params["iDisplayLength"]:
                    pages += 1

                norms = []

                # get all norms
                with ThreadPoolExecutor() as executor:
                    futures = [
                        executor.submit(
                            self._get_docs_links,
                            self._format_search_url(
                                situation, situation_id, norm_type_id, year, page
                            ),
                        )
                        for page in range(1, pages + 1)
                    ]

                    for future in tqdm(
                        as_completed(futures),
                        desc="DISTRITO FEDERAL | Get document links",
                        total=len(futures),
                        disable=not self.verbose,
                    ):
                        result = future.result()
                        norms.extend(result)

                results = []

                # get all norm data
                with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                    futures = [
                        executor.submit(self._get_doc_data, norm) for norm in norms
                    ]

                    for future in tqdm(
                        as_completed(futures),
                        desc="DISTRITO FEDERAL | Get document data",
                        total=len(norms),
                        disable=not self.verbose,
                    ):
                        result = future.result()

                        if result:

                            # prepare item for saving
                            queue_item = {
                                "year": year,
                                "type": norm_type,
                                "situation": situation,
                                **result,
                            }

                            results.append(queue_item)

                all_results.extend(results)
                self.count += len(results)

                if self.verbose:
                    print(
                        f"Finished scraping for Year: {year} | Situation: {situation} | Type: {norm_type} | Results: {len(results)} | Total: {self.count}"
                    )

        return all_results
