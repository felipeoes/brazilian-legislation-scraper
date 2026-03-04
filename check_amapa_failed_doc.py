import httpx
from bs4 import BeautifulSoup
import urllib3

urllib3.disable_warnings()


def main():
    url = "https://al.ap.leg.br/ver_texto_lei.php?iddocumento=10907"
    print(f"Fetching {url}")

    response = httpx.get(url, verify=False, timeout=30.0)
    soup = BeautifulSoup(response.text, "html.parser")

    # Mirror scraper's logic
    for a in soup.find_all("a", class_="texto_noticia3"):
        tbl = a.find_parent("table")
        if tbl:
            tbl.decompose()

    for img in soup.find_all("img", src=lambda s: s and "brasao" in s.lower()):
        tbl = img.find_parent("table")
        if tbl:
            tbl.decompose()

    remaining_table = soup.find("table")
    if remaining_table:
        print("--- Remaining table ---")
        print(remaining_table.prettify())
    else:
        print("--- Entire body ---")
        print(soup.prettify())


if __name__ == "__main__":
    main()
