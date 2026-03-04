def clean_md_tag(md_content: str) -> str:
    """Strip markdown code block wrappers (```markdown, ```md) if present."""
    if md_content.startswith("```markdown"):
        md_content = md_content.removeprefix("```markdown").strip()
    elif md_content.startswith("```md"):
        md_content = md_content.removeprefix("```md").strip()
    if md_content.endswith("```"):
        md_content = md_content.removesuffix("```").strip()
    return md_content
