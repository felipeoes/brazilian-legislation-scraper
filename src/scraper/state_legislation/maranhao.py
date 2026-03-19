from __future__ import annotations

import re
from dataclasses import dataclass
from typing import TYPE_CHECKING
from urllib.parse import urljoin
from xml.etree import ElementTree as ET

from bs4 import BeautifulSoup, Tag
from loguru import logger

from src.scraper.base.converter import calc_pages, valid_markdown
from src.scraper.base.scraper import DEFAULT_VALID_SITUATION, StateScraper

if TYPE_CHECKING:
    from src.scraper.base.schemas import ScrapedDocument

TYPES = {
    "Constituição Estadual": "constituicao-estadual/detalhe.html?dswid=-4293",
    "Lei": {
        "id": 1,
        "subtypes": {
            "Lei Ordinária": 2,
            "Lei Complementar": 3,
        },
    },
    "Emenda Constitucional": 5,
    "Decreto Legislativo": 6,
    "Resolução Legislativa": 7,
    "Resolução Administrativa": 8,
}

SITUATIONS = {"Não consta": "Não consta"}

_CONSTITUTION_YEAR = 1989


@dataclass(slots=True)
class JSFFormState:
    action_url: str
    fields: dict[str, str]
    search_button_name: str


class MaranhaoAlemaScraper(StateScraper):
    """Webscraper for Maranhao state legislation website (https://legislacao.al.ma.leg.br)

    Year start (earliest on source): 1948

    The search flow is a JSF form:
    - GET `/ged/busca.html` to obtain a fresh session and dynamic form state
    - optional AJAX POST on `in_tipo_doc` to load Lei subtypes
    - POST the search form for the target year/type
    - JSF partial POSTs on `table_resultados` to paginate additional rows
    """

    _TOTAL_RESULTS_RE = re.compile(r"(\d+)\s+registro\(s\)\s+encontrado\(s\)")
    _USER_INPUT_FIELDS = {
        "in_tipo_doc_focus",
        "in_tipo_doc_input",
        "in_nro_doc",
        "in_ano_doc",
        "ementa",
        "in_nro_proj_lei",
        "in_ano_proj_lei",
        "in_ini_public_input",
        "in_fim_public_input",
    }
    _STATE_FIELDS = {
        "javax.faces.ViewState",
        "javax.faces.ClientWindow",
        "table_resultados_rowExpansionState",
    }

    def __init__(
        self,
        base_url: str = "https://legislacao.al.ma.leg.br",
        **kwargs,
    ):
        super().__init__(
            base_url,
            name="MARANHAO",
            types=TYPES,
            situations=SITUATIONS,
            **kwargs,
        )
        self._rows_per_page = 10
        self._scraped_constitution: bool = False

    def _build_search_url(self) -> str:
        return f"{self.base_url}/ged/busca.html"

    @staticmethod
    def _string_attr(tag: Tag, attr_name: str, default: str = "") -> str:
        value = tag.attrs.get(attr_name, default)
        return value if isinstance(value, str) else default

    @staticmethod
    def _get_search_button_name(soup: BeautifulSoup) -> str:
        for button in soup.find_all("button"):
            if "Consultar" in button.get_text(" ", strip=True):
                if not isinstance(button, Tag):
                    continue
                return (
                    MaranhaoAlemaScraper._string_attr(button, "name")
                    or MaranhaoAlemaScraper._string_attr(button, "id")
                    or ""
                )
        return ""

    def _extract_form_state(self, soup: BeautifulSoup) -> JSFFormState | None:
        form = soup.find("form", action=re.compile(r"/ged/busca\.html"))
        if not isinstance(form, Tag):
            return None

        fields: dict[str, str] = {}
        for tag in soup.select("input[name], textarea[name], select[name]"):
            if not isinstance(tag, Tag):
                continue

            name = str(self._string_attr(tag, "name"))
            if not name:
                continue

            if tag.name == "input":
                input_type = str(self._string_attr(tag, "type")).lower()
                if input_type in {
                    "checkbox",
                    "radio",
                    "submit",
                    "button",
                    "image",
                    "file",
                }:
                    continue
                fields[name] = self._string_attr(tag, "value")
            elif tag.name == "textarea":
                fields[name] = tag.get_text("", strip=False)
            else:
                selected = tag.find("option", selected=True)
                fields[name] = (
                    self._string_attr(selected, "value")
                    if isinstance(selected, Tag)
                    else ""
                )

        search_button_name = self._get_search_button_name(soup) or "j_idt72"
        return JSFFormState(
            action_url=urljoin(
                self.base_url,
                self._string_attr(form, "action", "/ged/busca.html"),
            ),
            fields=fields,
            search_button_name=search_button_name,
        )

    def _update_form_state(
        self,
        form_state: JSFFormState,
        updates: dict[str, str],
    ) -> JSFFormState:
        fields = dict(form_state.fields)
        for update_id, value in updates.items():
            if "ViewState" in update_id:
                fields["javax.faces.ViewState"] = value
            elif "ClientWindow" in update_id:
                fields["javax.faces.ClientWindow"] = value

        action_url = form_state.action_url
        client_window = fields.get("javax.faces.ClientWindow")
        if client_window and "dswid=" in action_url:
            action_url = re.sub(r"dswid=[^&#]*", f"dswid={client_window}", action_url)

        return JSFFormState(
            action_url=action_url,
            fields=fields,
            search_button_name=form_state.search_button_name,
        )

    def _parse_partial_response(self, xml_text: str) -> dict[str, str]:
        try:
            root = ET.fromstring(xml_text)
        except ET.ParseError as exc:
            logger.error(f"MARANHAO | Invalid JSF partial response: {exc}")
            return {}

        updates: dict[str, str] = {}
        for update in root.findall(".//update"):
            update_id = update.attrib.get("id")
            if update_id:
                updates[update_id] = "".join(update.itertext())
        return updates

    def _extract_subtypes(self, panel_html: str) -> tuple[str, dict[str, str]]:
        soup = BeautifulSoup(panel_html, "html.parser")
        subtype_field_name = ""
        options: dict[str, str] = {}

        for row in soup.find_all("tr"):
            checkbox = row.find("input", {"type": "checkbox"})
            label = row.find("label")
            if not isinstance(checkbox, Tag) or not isinstance(label, Tag):
                continue

            subtype_field_name = subtype_field_name or self._string_attr(
                checkbox, "name"
            )
            label_text = label.get_text(" ", strip=True)
            value = self._string_attr(checkbox, "value")
            if label_text and value:
                options[label_text] = value

        return subtype_field_name, options

    def _iter_support_fields(self, fields: dict[str, str]):
        for name, value in fields.items():
            if name in self._USER_INPUT_FIELDS or name in self._STATE_FIELDS:
                continue
            if name.startswith("table_resultados"):
                continue
            yield name, value

    def _build_query_fields(
        self,
        form_state: JSFFormState,
        norm_type_id: str | int,
        year: int | str,
        subtype_field_name: str = "",
        subtype_values: tuple[str, ...] = (),
    ) -> list[tuple[str, str]]:
        fields = form_state.fields
        payload = list(self._iter_support_fields(fields))
        payload.extend(
            [
                ("in_tipo_doc_focus", fields.get("in_tipo_doc_focus", "")),
                ("in_tipo_doc_input", str(norm_type_id)),
            ]
        )
        if subtype_field_name:
            for subtype_value in subtype_values:
                payload.append((subtype_field_name, str(subtype_value)))
        payload.extend(
            [
                ("in_nro_doc", fields.get("in_nro_doc", "")),
                ("in_ano_doc", str(year)),
                ("ementa", fields.get("ementa", "")),
                ("in_nro_proj_lei", fields.get("in_nro_proj_lei", "")),
                ("in_ano_proj_lei", fields.get("in_ano_proj_lei", "")),
                ("in_ini_public_input", fields.get("in_ini_public_input", "")),
                ("in_fim_public_input", fields.get("in_fim_public_input", "")),
            ]
        )
        return payload

    def _build_type_change_payload(
        self,
        form_state: JSFFormState,
        norm_type_id: str | int,
    ) -> list[tuple[str, str]]:
        payload = self._build_query_fields(form_state, norm_type_id, "")
        payload.extend(
            [
                (
                    "javax.faces.ViewState",
                    form_state.fields.get("javax.faces.ViewState", ""),
                ),
                (
                    "javax.faces.ClientWindow",
                    form_state.fields.get("javax.faces.ClientWindow", ""),
                ),
                ("javax.faces.source", "in_tipo_doc"),
                ("javax.faces.partial.event", "change"),
                ("javax.faces.partial.execute", "in_tipo_doc in_tipo_doc"),
                ("javax.faces.partial.render", "painel_tipo_doc"),
                ("javax.faces.behavior.event", "change"),
                ("javax.faces.partial.ajax", "true"),
            ]
        )
        return payload

    def _build_search_payload(
        self,
        form_state: JSFFormState,
        norm_type_id: str | int,
        year: int,
        subtype_field_name: str = "",
        subtype_values: tuple[str, ...] = (),
    ) -> list[tuple[str, str]]:
        payload = self._build_query_fields(
            form_state,
            norm_type_id,
            year,
            subtype_field_name=subtype_field_name,
            subtype_values=subtype_values,
        )
        payload.append((form_state.search_button_name, ""))
        payload.extend(
            [
                (
                    "javax.faces.ViewState",
                    form_state.fields.get("javax.faces.ViewState", ""),
                ),
                (
                    "javax.faces.ClientWindow",
                    form_state.fields.get("javax.faces.ClientWindow", ""),
                ),
            ]
        )
        return payload

    def _build_page_payload(
        self,
        form_state: JSFFormState,
        norm_type_id: str | int,
        year: int,
        page: int,
        subtype_field_name: str = "",
        subtype_values: tuple[str, ...] = (),
    ) -> list[tuple[str, str]]:
        payload = [
            ("javax.faces.partial.ajax", "true"),
            ("javax.faces.source", "table_resultados"),
            ("javax.faces.partial.execute", "table_resultados"),
            ("javax.faces.partial.render", "table_resultados"),
            ("javax.faces.behavior.event", "page"),
            ("javax.faces.partial.event", "page"),
            ("table_resultados_pagination", "true"),
            ("table_resultados_first", str((page - 1) * self._rows_per_page)),
            ("table_resultados_rows", str(self._rows_per_page)),
            ("table_resultados_skipChildren", "true"),
            ("table_resultados_encodeFeature", "true"),
        ]
        payload.extend(
            self._build_query_fields(
                form_state,
                norm_type_id,
                year,
                subtype_field_name=subtype_field_name,
                subtype_values=subtype_values,
            )
        )
        payload.append(
            (
                "table_resultados_rowExpansionState",
                form_state.fields.get("table_resultados_rowExpansionState", ""),
            )
        )
        payload.extend(
            [
                (
                    "javax.faces.ViewState",
                    form_state.fields.get("javax.faces.ViewState", ""),
                ),
                (
                    "javax.faces.ClientWindow",
                    form_state.fields.get("javax.faces.ClientWindow", ""),
                ),
            ]
        )
        return payload

    async def _get_search_form_state(self) -> JSFFormState | None:
        response = await self.request_service.make_request(self._build_search_url())
        if not response:
            logger.error("MARANHAO | Failed to fetch search page")
            return None

        soup = BeautifulSoup(await response.text(), "html.parser")
        form_state = self._extract_form_state(soup)
        if form_state is None:
            logger.error("MARANHAO | Failed to extract search form state")
        return form_state

    async def _load_lei_subtype_state(
        self,
        form_state: JSFFormState,
        norm_type_id: str | int,
    ) -> tuple[JSFFormState, str, dict[str, str]] | None:
        response = await self.request_service.make_request(
            form_state.action_url,
            method="POST",
            payload=self._build_type_change_payload(form_state, norm_type_id),
        )
        if not response:
            logger.error("MARANHAO | Failed to load Lei subtypes")
            return None

        updates = self._parse_partial_response(await response.text())
        panel_html = updates.get("painel_tipo_doc", "")
        if not panel_html:
            logger.error("MARANHAO | Missing Lei subtype panel in JSF response")
            return None

        subtype_field_name, subtypes = self._extract_subtypes(panel_html)
        if not subtype_field_name or not subtypes:
            logger.error("MARANHAO | Failed to parse Lei subtype options")
            return None

        return (
            self._update_form_state(form_state, updates),
            subtype_field_name,
            subtypes,
        )

    def _get_total_docs(self, soup: BeautifulSoup) -> int:
        text = soup.get_text(" ", strip=True)
        match = self._TOTAL_RESULTS_RE.search(text)
        return int(match.group(1)) if match else 0

    def _get_docs_links(
        self, source: str | BeautifulSoup, norm_type: str
    ) -> list[dict] | None:
        soup = (
            source
            if isinstance(source, BeautifulSoup)
            else BeautifulSoup(source, "html.parser")
        )
        docs: list[dict] = []

        for row in soup.select("tr.ui-widget-content"):
            labels = [
                label.get_text(" ", strip=True)
                for label in row.select("label.ui-outputlabel.ui-widget")
            ]
            if not labels:
                continue

            number = labels[0]
            row_type = labels[1] if len(labels) > 1 else norm_type
            project = labels[2] if len(labels) > 2 else ""
            publication = labels[3] if len(labels) > 3 else ""
            summary_tag = row.select_one("label.ementa")
            summary = (
                summary_tag.get_text(" ", strip=True)
                if summary_tag is not None
                else (labels[4] if len(labels) > 4 else "")
            )

            anchors = row.select("a[href]")
            if not anchors:
                continue

            docs.append(
                {
                    "title": f"{row_type or norm_type} - {number}",
                    "publication": publication,
                    "project": project,
                    "summary": summary,
                    "pdf_link": urljoin(
                        self.base_url,
                        self._string_attr(anchors[-1], "href"),
                    ),
                }
            )

        return docs

    async def _get_doc_data(self, doc_info: dict) -> ScrapedDocument | None:
        return await self._process_pdf_doc(doc_info)

    async def _search_norms(
        self,
        norm_type: str,
        norm_type_id: str,
        year: int,
        subtype: str | None = None,
        subtype_id: str | None = None,
    ) -> tuple[JSFFormState | None, BeautifulSoup | None, str, tuple[str, ...]]:
        form_state = await self._get_search_form_state()
        if form_state is None:
            return None, None, "", ()

        subtype_field_name = ""
        subtype_values: tuple[str, ...] = ()

        if subtype_id is not None:
            subtype_state = await self._load_lei_subtype_state(form_state, norm_type_id)
            if subtype_state is None:
                return None, None, "", ()

            form_state, subtype_field_name, subtype_options = subtype_state
            subtype_value = subtype_options.get(subtype or "")
            if not subtype_value:
                logger.error(
                    f"MARANHAO | Missing subtype value for {norm_type} | {subtype}"
                )
                return None, None, "", ()
            subtype_values = (subtype_value,)

        response = await self.request_service.make_request(
            form_state.action_url,
            method="POST",
            payload=self._build_search_payload(
                form_state,
                norm_type_id,
                year,
                subtype_field_name=subtype_field_name,
                subtype_values=subtype_values,
            ),
        )
        if not response:
            logger.error(
                f"MARANHAO | Failed search request | {norm_type} | {subtype or ''} | {year}"
            )
            return None, None, "", ()

        soup = BeautifulSoup(await response.text(), "html.parser")
        result_state = self._extract_form_state(soup)
        if result_state is None:
            logger.error(
                f"MARANHAO | Failed to extract search result state | {norm_type} | {subtype or ''} | {year}"
            )
            return None, None, "", ()

        return result_state, soup, subtype_field_name, subtype_values

    async def _fetch_docs_page(
        self,
        page: int,
        *,
        form_state: JSFFormState,
        norm_type_id: str,
        year: int,
        effective_type: str,
        subtype_field_name: str = "",
        subtype_values: tuple[str, ...] = (),
    ) -> list[dict]:
        response = await self.request_service.make_request(
            form_state.action_url,
            method="POST",
            payload=self._build_page_payload(
                form_state,
                norm_type_id,
                year,
                page,
                subtype_field_name=subtype_field_name,
                subtype_values=subtype_values,
            ),
        )
        if not response:
            logger.error(f"MARANHAO | Failed to fetch results page {page}")
            return []

        updates = self._parse_partial_response(await response.text())
        table_html = updates.get("table_resultados", "")
        if not table_html:
            logger.error(f"MARANHAO | Missing table_resultados in page {page} response")
            return []

        return self._get_docs_links(table_html, effective_type) or []

    async def _scrape_norms(
        self,
        norm_type: str,
        norm_type_id: str | int,
        year: int,
        situation: str,
        subtype: str | None = None,
        subtype_id: str | None = None,
    ) -> list[dict]:
        form_state, soup, subtype_field_name, subtype_values = await self._search_norms(
            norm_type,
            norm_type_id,
            year,
            subtype=subtype,
            subtype_id=subtype_id,
        )
        if form_state is None or soup is None:
            return []

        total_docs = self._get_total_docs(soup)
        if total_docs <= 0:
            return []

        effective_type = subtype or norm_type
        documents = self._get_docs_links(soup, effective_type) or []
        total_pages = calc_pages(total_docs, self._rows_per_page)

        if total_pages > 1:
            page_results = await self._gather_results(
                [
                    self._fetch_docs_page(
                        page_num,
                        form_state=form_state,
                        norm_type_id=norm_type_id,
                        year=year,
                        effective_type=effective_type,
                        subtype_field_name=subtype_field_name,
                        subtype_values=subtype_values,
                    )
                    for page_num in range(2, total_pages + 1)
                ],
                context={"year": year, "type": effective_type, "situation": situation},
                desc=f"{self.name} | {effective_type} | pages",
            )
            for page_docs in page_results:
                if isinstance(page_docs, list):
                    documents.extend(page_docs)

        return await self._process_documents(
            documents,
            year=year,
            norm_type=effective_type,
            situation=situation,
        )

    async def _scrape_constitution(
        self, norm_type: str, norm_type_id: str
    ) -> dict | None:
        url = urljoin(f"{self.base_url}/ged/", norm_type_id)
        soup = await self.request_service.get_soup(url)
        if not soup:
            logger.error("MARANHAO | Failed to fetch constitution page")
            return None

        object_tag = soup.find("object", {"class": "view-pdf-constituicao"})
        if not isinstance(object_tag, Tag):
            logger.error("MARANHAO | Constitution PDF object not found")
            return None

        pdf_link = self._string_attr(object_tag, "data")
        if not pdf_link:
            logger.error("MARANHAO | Constitution PDF link is empty")
            return None

        if self._is_already_scraped(pdf_link, "Constituição Estadual do Maranhão"):
            self._scraped_constitution = True
            return None

        text_markdown, raw_content, content_ext = await self._download_and_convert(
            pdf_link
        )
        valid, reason = valid_markdown(text_markdown)
        if not valid:
            logger.error(
                f"Failed to get markdown for Constitution | {pdf_link}: {reason}"
            )
            return None

        queue_item = {
            "year": _CONSTITUTION_YEAR,
            "situation": DEFAULT_VALID_SITUATION,
            "type": norm_type,
            "title": "Constituição Estadual do Maranhão",
            "summary": "",
            "text_markdown": text_markdown,
            "document_url": pdf_link,
            "_raw_content": raw_content,
            "_content_extension": content_ext,
        }

        await self._save_doc_result(queue_item)
        self._scraped_constitution = True
        return queue_item

    async def _scrape_situation_type(
        self, year: int, situation: str, situation_id, norm_type: str, norm_type_id
    ) -> list[dict]:
        results = []

        if norm_type == "Constituição Estadual":
            if self._scraped_constitution:
                return results
            result = await self._scrape_constitution(norm_type, norm_type_id)
            if result:
                results.append(result)
            return results

        if isinstance(norm_type_id, dict):
            subtypes = norm_type_id["subtypes"]
            norm_type_id = norm_type_id["id"]
            for subtype, subtype_id in subtypes.items():
                subtype_results = await self._scrape_norms(
                    norm_type,
                    norm_type_id,
                    year,
                    situation,
                    subtype=subtype,
                    subtype_id=subtype_id,
                )
                if isinstance(subtype_results, list):
                    results.extend(subtype_results)
        else:
            subtype_results = await self._scrape_norms(
                norm_type, norm_type_id, year, situation
            )
            results.extend(subtype_results)

        return results

    async def _scrape_year(self, year: int) -> list[dict]:
        results = []
        situation_items = (
            self.situations.items()
            if isinstance(self.situations, dict)
            else [(s, s) for s in self.situations]
        )
        type_items = (
            self.types.items()
            if isinstance(self.types, dict)
            else [(t, None) for t in self.types]
        )

        tasks = [
            self._scrape_situation_type(
                year, situation, situation_id, norm_type, norm_type_id
            )
            for situation, situation_id in situation_items
            for norm_type, norm_type_id in type_items
        ]
        gathered = await self._gather_results(
            tasks,
            context={"year": year},
            desc=f"MARANHAO | year {year}",
        )
        for item in gathered:
            if isinstance(item, list):
                results.extend(item)
        return results
