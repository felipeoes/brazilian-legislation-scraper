from bs4 import BeautifulSoup
from loguru import logger
from src.scraper.base.scraper import StateScraper

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


class DFSinjScraper(StateScraper):
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
        super().__init__(
            base_url,
            name="DISTRITO_FEDERAL",
            types=TYPES,
            situations=SITUATIONS,
            **kwargs,
        )
        self._base_params = {
            "bbusca": "sinj_norma",
            "iColumns": 9,
            "sColumns": ",,,,,,,,",
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
        }
        self._display_length = 100
        self.total_pages_url = "https://www.sinj.df.gov.br/sinj/ashx/Consulta/TotalConsulta.ashx?bbusca=sinj_norma"
        self.session_id_created = False

    def _build_payload(
        self,
        situation: str,
        situation_id: str,
        norm_type_id: str,
        year: int,
        page: int = 1,
    ) -> list[tuple]:
        """Build a fresh POST payload for a specific query (no shared state mutation)."""
        display_start = (page - 1) * self._display_length
        # Start with base params (immutable keys)
        payload = [(key, value) for key, value in self._base_params.items()]
        # Add mutable fields
        payload.append(("iDisplayStart", display_start))
        payload.append(("iDisplayLength", self._display_length))
        payload.append(("ch_tipo_norma", norm_type_id))
        payload.append(
            (
                "argumento",
                f"number#ano_assinatura#Ano de Assinatura#igual#igual a#{year}#{year}#E",
            )
        )
        payload.append(
            (
                "argumento",
                f"autocomplete#ch_situacao#Situ\u00e7\u00e3o#igual#igual a#{situation_id}#{situation}#E",
            )
        )
        return payload

    async def _get_docs_links(self, url: str, payload: list[tuple]) -> list:
        """Get document links from search request. Returns a list of dicts with keys 'title', 'summary', 'date', 'html_link'"""
        response = await self.request_service.make_request(
            url,
            method="POST",
            payload=payload,
        )
        if not response:
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

        data = await response.json()

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

    async def _get_doc_data(self, doc_info: dict) -> dict:
        """Get document data from html link"""

        try:
            # remove html link from doc_info
            html_link = doc_info.pop("html_link")

            if self._is_already_scraped(html_link, doc_info.get("title", "")):
                return None

            response = await self.request_service.make_request(html_link)
            if not response:
                raise RuntimeError(f"No response for {html_link}")

            body = await response.read()
            soup = BeautifulSoup(body, "html.parser")

            # get id="div_texto"
            norm_text_tag = soup.find("div", id="div_texto")
            text_markdown = None
            raw_content = None
            content_ext = None
            if not norm_text_tag:
                # it may be a pdf file, try to get text markdown instead (without using LLM for image extraction)
                text_markdown = await self._get_markdown(response=response)
                raw_content = body
                content_ext = ".pdf"

                if not text_markdown:
                    await self._save_doc_error(
                        title=doc_info.get("title", "Unknown"),
                        year="",
                        situation="",
                        norm_type="",
                        html_link=html_link,
                        error_message="Could not find div_texto and markdown extraction failed",
                    )
                    return None
            else:
                # Remove the "Este texto não substitui..." footer disclaimer
                for tag in norm_text_tag.find_all(
                    "p", style=lambda s: s and "text-align:right" in s
                ):
                    if tag.find("a", href=lambda h: h and "BaixarArquivoDiario" in h):
                        tag.decompose()

                html_string = f"<html>{norm_text_tag.prettify()}</html>"

                # get markdown text
                text_markdown = (
                    await self._get_markdown(html_content=html_string)
                    if not text_markdown
                    else text_markdown
                )

                raw_content = html_string.encode("utf-8")
                content_ext = ".html"

            doc_info["text_markdown"] = text_markdown
            doc_info["document_url"] = html_link
            if raw_content is not None:
                doc_info["_raw_content"] = raw_content
                doc_info["_content_extension"] = content_ext

            return doc_info
        except Exception as e:
            logger.error(f"Error getting document data: {e}")
            await self._save_doc_error(
                title=doc_info.get("title", "Unknown"),
                year="",
                situation="",
                norm_type="",
                html_link=doc_info.get("html_link", ""),
                error_message=str(e),
            )
            return None

    async def _scrape_situation_type(
        self,
        situation: str,
        situation_id: str,
        norm_type: str,
        norm_type_id: str,
        year: int,
    ) -> list:
        """Scrape norms for a specific situation and type"""
        # need to make a get request first to create the session ID ( will be used in all subsequent requests)
        if not self.session_id_created:
            get_url = (
                self.base_url
                + "/ashx/Cadastro/HistoricoDePesquisaIncluir.ashx?tipo_pesquisa=avancada&argumento=autocomplete%23ch_situacao%23Situa%C3%A7%C3%A3o%23igual%23igual+a%23semrevogacaoexpressa%23Sem+Revoga%C3%A7%C3%A3o+Expressa%23E&ch_tipo_norma=46000000&consulta=tipo_pesquisa=avancada&consulta=argumento=autocomplete%23ch_situacao%23Situa%C3%A7%C3%A3o%23igual%23igual+a%23semrevogacaoexpressa%23Sem+Revoga%C3%A7%C3%A3o+Expressa%23E&consulta=ch_tipo_norma=46000000&chave=6c31e2b0c76d4aa227cd6804bc4fc59f&total={%22nm_base%22:%22sinj_norma%22,%22ds_base%22:%22Normas%22,%22nr_total%22:6008}&_=1741738478078"
            )
            await self.request_service.make_request(get_url)
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

        response = await self.request_service.make_request(
            self.total_pages_url,
            method="POST",
            payload=total_pages_request_params,
        )
        if not response:
            return []

        data = await response.json()

        total_norms = data["counts"][0]["count"]
        # if count is 0, skip
        if total_norms == 0:
            return []

        pages = total_norms // self._display_length
        if total_norms % self._display_length:
            pages += 1

        norms = []
        search_url = (
            f"{self.base_url}/ashx/Datatable/ResultadoDePesquisaNormaDatatable.ashx"
        )

        # get all norms
        tasks = [
            self._get_docs_links(
                search_url,
                self._build_payload(situation, situation_id, norm_type_id, year, page),
            )
            for page in range(1, pages + 1)
        ]
        valid_results = await self._gather_results(
            tasks,
            context={"year": year, "type": norm_type, "situation": situation},
            desc=f"DISTRITO FEDERAL | {norm_type} | get_docs_links",
        )
        for result in valid_results:
            norms.extend(result)

        # get all norm data
        ctx = {"year": year, "type": norm_type, "situation": situation}
        tasks = [self._with_save(self._get_doc_data(norm), ctx) for norm in norms]
        results = await self._gather_results(
            tasks,
            context=ctx,
            desc=f"DISTRITO FEDERAL | {norm_type}",
        )

        if self.verbose:
            logger.info(
                f"Finished scraping for Year: {year} | Situation: {situation} | Type: {norm_type} | Results: {len(results)}"
            )

        return results

    async def _scrape_year(self, year: int) -> list:
        """Scrape norms for a specific year"""
        tasks = [
            self._scrape_situation_type(sit, sit_id, nt, nt_id, year)
            for sit, sit_id in self.situations.items()
            for nt, nt_id in self.types.items()
        ]
        valid = await self._gather_results(
            tasks,
            context={"year": year, "type": "NA", "situation": "NA"},
            desc=f"{self.name} | Year {year}",
        )
        return self._flatten_results(valid)
