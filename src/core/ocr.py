"""Shared PydanticAI OCR with bounded rendering and all-or-nothing batches."""

import asyncio
from collections import defaultdict
from collections.abc import Sequence
from contextvars import ContextVar
from typing import Any

import fitz
from pydantic import BaseModel
from pydantic_ai import Agent, BinaryContent
from pydantic_ai.exceptions import ModelHTTPError
from pydantic_ai.models.fallback import FallbackModel
from pydantic_ai.usage import RunUsage
from pydantic_core import to_jsonable_python
from tenacity import (
    AsyncRetrying,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

usage_scope: ContextVar[str] = ContextVar("ocr_usage_scope", default="global")


class OCRError(Exception):
    pass


class OCROutput(BaseModel):
    text_markdown: str


def clean_md_tag(md_content: str) -> str:
    if md_content.startswith("```markdown"):
        md_content = md_content.removeprefix("```markdown").strip()
    elif md_content.startswith("```md"):
        md_content = md_content.removeprefix("```md").strip()
    if md_content.endswith("```"):
        md_content = md_content.removesuffix("```").strip()
    return md_content


DEFAULT_PROMPT = """Você é um especialista de extração e formatação de textos jurídicos. O documento fornecido é uma norma jurídica. Extraia todo o conteúdo principal e formate-o em Markdown, seguindo rigorosamente estas regras:

*   **Fidelidade Absoluta (CRÍTICO):** Transcreva o texto exata e literalmente como aparece no documento. Não altere nenhuma palavra, não corrija gramática e não modifique a pontuação. Preservar a exatidão legal é essencial. Não introduza nenhuma palavra ou frase que não esteja presente no documento original.
*   **Estrutura Legal:** Respeite rigorosamente a numeração e a hierarquia legislativa: títulos, capítulos, seções, artigos (Art.), parágrafos (§), incisos (algarismos romanos: I, II, III) e alíneas (letras: a, b, c).
*   **Formatação Markdown:**
    * Use títulos Markdown (`##` ou `###`) para títulos, capítulos e seções.
    * Aplique **negrito** ou *itálico* exatamente onde o texto original estiver em destaque.
    * Caso haja tabelas, preserve a formatação tabular usando a sintaxe de tabelas do Markdown.
    * Se houver uma *ementa* (o bloco de texto que resume a norma, geralmente recuado à direita no topo), formate-a como citação (usando `>` antes do bloco).
*   **Continuidade:** O texto pode ser continuação de uma página anterior ou terminar de forma abrupta. Extraia desde a primeira palavra válida até a última, mesmo que comece ou termine no meio de uma frase.
*   **Limpeza e Exclusões (ATENÇÃO):** Ignore cabeçalhos (headers), rodapés (footers), números de página, datas de impressão ou marcas d'água. **Exclua obrigatoriamente qualquer nota editorial ou aviso legal que inicie com "Este texto não substitui..." ou "Esse texto não substitui..." ou outras notas e observações similares, independentemente de onde apareçam na página.**

Nota: o documento recebido pode estar em branco. Nesse caso, retorne text_markdown como uma string vazia ("").

Retorne o conteúdo extraído no campo text_markdown. Não inclua a tag ```markdown, saudações, introduções ou explicações adicionais nesse campo."""


class OCR:
    def __init__(
        self,
        models: str,
        prompt: str = DEFAULT_PROMPT,
        batch_size: int = 5,
        concurrency: int = 8,
        pdf_input: bool = False,
    ) -> None:
        names = [name.strip() for name in models.split(",") if name.strip()]
        if not names or batch_size < 1 or concurrency < 1:
            raise ValueError("OCR needs models and positive batch_size/concurrency")
        model = FallbackModel(*names) if len(names) > 1 else names[0]
        self.agent = Agent(model, instructions=prompt, output_type=OCROutput)
        self.sem = asyncio.Semaphore(concurrency)
        self.batch_size = batch_size
        self.concurrency = concurrency
        self.pdf_input = pdf_input
        self.usage: dict[str, RunUsage] = defaultdict(RunUsage)
        self._scoped_usage: dict[str, dict[str, RunUsage]] = defaultdict(
            lambda: defaultdict(RunUsage)
        )

    async def _run(self, parts: Sequence[BinaryContent]) -> str:
        async for attempt in AsyncRetrying(
            stop=stop_after_attempt(3),
            wait=wait_exponential(min=1, max=10),
            retry=retry_if_exception_type((ModelHTTPError, TimeoutError)),
            reraise=True,
        ):
            with attempt:
                result = await self.agent.run(["Transcreva o documento.", *parts])
                model = result.response.model_name or "unknown"
                usage = result.usage
                self.usage[model].incr(usage)
                self._scoped_usage[usage_scope.get()][model].incr(usage)
                return clean_md_tag(result.output.text_markdown)
        raise AssertionError("Retry loop did not execute")

    def usage_for(self, scope: str) -> dict[str, Any]:
        return {
            model: to_jsonable_python(usage)
            for model, usage in self._scoped_usage.get(scope, {}).items()
        }

    async def images(self, pngs: Sequence[bytes]) -> str:
        texts = []
        try:
            for offset in range(0, len(pngs), self.batch_size):
                async with self.sem:
                    texts.append(
                        await self._run(
                            [
                                BinaryContent(data=png, media_type="image/png")
                                for png in pngs[offset : offset + self.batch_size]
                            ]
                        )
                    )
        except Exception as exc:
            raise OCRError("Image OCR failed") from exc
        return "\n\n".join(texts)

    def _render_batch(self, data: bytes, start: int, end: int) -> list[BinaryContent]:
        with fitz.open(stream=data, filetype="pdf") as doc:
            if self.pdf_input:
                with fitz.open() as chunk:
                    chunk.insert_pdf(doc, from_page=start, to_page=end - 1)
                    return [
                        BinaryContent(
                            data=chunk.tobytes(), media_type="application/pdf"
                        )
                    ]
            return [
                BinaryContent(
                    data=doc[i].get_pixmap(dpi=150).tobytes("png"),
                    media_type="image/png",
                )
                for i in range(start, end)
            ]

    async def pdf(self, data: bytes) -> str:
        def pages() -> int:
            with fitz.open(stream=data, filetype="pdf") as doc:
                return doc.page_count

        try:
            count = await asyncio.to_thread(pages)
            texts = []

            async def batch(start: int) -> str:
                async with self.sem:
                    parts = await asyncio.to_thread(
                        self._render_batch,
                        data,
                        start,
                        min(start + self.batch_size, count),
                    )
                    return await self._run(parts)

            starts = range(0, count, self.batch_size)
            for offset in range(0, len(starts), self.concurrency):
                tasks = [
                    asyncio.create_task(batch(start))
                    for start in starts[offset : offset + self.concurrency]
                ]
                try:
                    texts.extend(await asyncio.gather(*tasks))
                finally:
                    for task in tasks:
                        if not task.done():
                            task.cancel()
                    await asyncio.gather(*tasks, return_exceptions=True)
            return "\n\n".join(texts)
        except Exception as exc:
            raise OCRError("PDF OCR failed; no partial document was returned") from exc
