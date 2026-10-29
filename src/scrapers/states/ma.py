import re
from collections.abc import AsyncIterator, Iterator
from dataclasses import dataclass
from typing import Any
from urllib.parse import urljoin
from xml.etree import ElementTree as ET

from bs4 import BeautifulSoup, Tag
from loguru import logger

from src.core.convert import calc_pages
from src.core.document import Document
from src.core.scraper import DEFAULT_VALID_SITUATION, BaseScraper

TYPES = {
    "Constituição Estadual": "constituicao-estadual/detalhe.html?dswid=-4293",
    "Lei": {"id": 1, "subtypes": {"Lei Ordinária": 2, "Lei Complementar": 3}},
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


class MaranhaoAlemaScraper(BaseScraper):
    """Webscraper for Maranhao state legislation website (https://legislacao.al.ma.leg.br)

    Year start (earliest on source): 1948

    The search flow is a JSF form:
    - GET `/ged/busca.html` to obtain a fresh session and dynamic form state
    - optional AJAX POST on `in_tipo_doc` to load Lei subtypes
    - POST the search form for the target year/type
    - JSF partial POSTs on `table_resultados` to paginate additional rows
    """

    name = "MARANHAO"
    types = TYPES
    situations = SITUATIONS
    base_url = "https://legislacao.al.ma.leg.br"
    _TOTAL_RESULTS_RE = re.compile("(\\d+)\\s+registro\\(s\\)\\s+encontrado\\(s\\)")
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

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self._rows_per_page = 10

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
        form = soup.find("form", action=re.compile("/ged/busca\\.html"))
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
                self.base_url, self._string_attr(form, "action", "/ged/busca.html")
            ),
            fields=fields,
            search_button_name=search_button_name,
        )

    def _update_form_state(
        self, form_state: JSFFormState, updates: dict[str, str]
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
            action_url = re.sub("dswid=[^&#]*", f"dswid={client_window}", action_url)
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
        soup = BeautifulSoup(panel_html, "lxml")
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
        return (subtype_field_name, options)

    def _iter_support_fields(self, fields: dict[str, str]) -> Iterator[tuple[str, str]]:
        for name, value in fields.items():
            if name in self._USER_INPUT_FIELDS or name in self._STATE_FIELDS:
                continue
            if name.startswith("table_resultados"):
                continue
            yield (name, value)

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
        self, form_state: JSFFormState, norm_type_id: str | int
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
        response = await self.http.request(self._build_search_url())
        soup = BeautifulSoup(response.text, "lxml")
        form_state = self._extract_form_state(soup)
        if form_state is None:
            logger.error("MARANHAO | Failed to extract search form state")
        return form_state

    async def _load_lei_subtype_state(
        self, form_state: JSFFormState, norm_type_id: str | int
    ) -> tuple[JSFFormState, str, dict[str, str]] | None:
        response = await self.http.request(
            form_state.action_url,
            method="POST",
            data=self._build_type_change_payload(form_state, norm_type_id),
        )
        updates = self._parse_partial_response(response.text)
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
            else BeautifulSoup(source, "lxml")
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
                else labels[4]
                if len(labels) > 4
                else ""
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
                        self.base_url, self._string_attr(anchors[-1], "href")
                    ),
                }
            )
        return docs

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
            return (None, None, "", ())
        subtype_field_name = ""
        subtype_values: tuple[str, ...] = ()
        if subtype_id is not None:
            subtype_state = await self._load_lei_subtype_state(form_state, norm_type_id)
            if subtype_state is None:
                return (None, None, "", ())
            form_state, subtype_field_name, subtype_options = subtype_state
            subtype_value = subtype_options.get(subtype or "")
            if not subtype_value:
                logger.error(
                    f"MARANHAO | Missing subtype value for {norm_type} | {subtype}"
                )
                return (None, None, "", ())
            subtype_values = (subtype_value,)
        response = await self.http.request(
            form_state.action_url,
            method="POST",
            data=self._build_search_payload(
                form_state,
                norm_type_id,
                year,
                subtype_field_name=subtype_field_name,
                subtype_values=subtype_values,
            ),
        )
        soup = BeautifulSoup(response.text, "lxml")
        result_state = self._extract_form_state(soup)
        if result_state is None:
            logger.error(
                f"MARANHAO | Failed to extract search result state | {norm_type} | {subtype or ''} | {year}"
            )
            return (None, None, "", ())
        return (result_state, soup, subtype_field_name, subtype_values)

    async def list_documents(self, year: int) -> AsyncIterator[dict]:
        for norm_type, spec in self.types.items():
            if norm_type == "Constituição Estadual":
                if year == self.years.start:
                    yield {
                        "year": _CONSTITUTION_YEAR,
                        "type": norm_type,
                        "title": "Constituição Estadual do Maranhão",
                        "document_url": urljoin(f"{self.base_url}/ged/", spec),
                        "situation": DEFAULT_VALID_SITUATION,
                        "constitution": True,
                    }
                continue
            subtypes = (
                spec["subtypes"].items() if isinstance(spec, dict) else [(None, None)]
            )
            norm_id = spec["id"] if isinstance(spec, dict) else spec
            for subtype, subtype_id in subtypes:
                state, soup, field, values = await self._search_norms(
                    norm_type, norm_id, year, subtype, subtype_id
                )
                if state is None or soup is None:
                    raise ValueError("Maranhão search did not return form state")
                effective_type = subtype or norm_type
                pages = calc_pages(self._get_total_docs(soup), self._rows_per_page)
                for page in range(1, pages + 1):
                    if page > 1:
                        response = await self.http.request(
                            state.action_url,
                            method="POST",
                            data=self._build_page_payload(
                                state,
                                norm_id,
                                year,
                                page,
                                subtype_field_name=field,
                                subtype_values=values,
                            ),
                        )
                        updates = self._parse_partial_response(response.text)
                        state = self._update_form_state(state, updates)
                        if "table_resultados" not in updates:
                            raise ValueError("Maranhão pagination is missing results")
                        soup = BeautifulSoup(updates["table_resultados"], "lxml")
                    for ref in self._get_docs_links(soup, effective_type):
                        yield {
                            **ref,
                            "year": year,
                            "type": effective_type,
                            "situation": DEFAULT_VALID_SITUATION,
                            "document_url": ref["pdf_link"],
                        }

    async def fetch(self, ref: dict) -> Document:
        if ref.get("constitution"):
            soup = (await self.http.request(ref["document_url"])).soup()
            pdf = soup.find("object", class_="view-pdf-constituicao")
            if pdf is None or not pdf.get("data"):
                raise ValueError("Maranhão constitution has no PDF link")
            ref = {**ref, "pdf_link": urljoin(self.base_url, pdf["data"])}
        return await self.fetch_pdf(ref)
