import httpx
from bs4 import BeautifulSoup
import urllib3

urllib3.disable_warnings()


def main():
    # 14 is Decreto Legislativo, 2025
    url = "https://al.ap.leg.br/pagina.php?pg=buscar_legislacao&aba=legislacao&submenu=listar_legislacao&especie_documento=14&ano=2025&pesquisa=&n_doeB=&n_leiB=&data_inicial=&data_final=&orgaoB=&autor=&legislaturaB=&pagina=1"
    print(f"Fetching {url}")

    response = httpx.get(url, verify=False, timeout=30.0)
    soup = BeautifulSoup(response.text, "html.parser")

    items = soup.find("tbody").find_all("tr") if soup.find("tbody") else []
    print(f"Found {len(items)} items in tbody.")

    for item in items:
        tds = item.find_all("td")
        if len(tds) != 6:
            continue

        title = tds[0].text.strip()

        # Check specifically for 1776
        if "1776" in title or "1775" in title:
            print(f"\n--- HTML for {title} ---")
            print(item.prettify())


if __name__ == "__main__":
    main()
