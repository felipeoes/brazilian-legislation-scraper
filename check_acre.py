import httpx
from bs4 import BeautifulSoup
import urllib3

urllib3.disable_warnings()


def main():
    url = "https://legis.ac.gov.br/principal/1"
    try:
        print(f"Fetching {url}...")
        response = httpx.get(url, verify=False, timeout=30.0)
        soup = BeautifulSoup(response.text, "html.parser")
        types = {
            "Lei Ordinária": "lei_ordinarias",
            "Lei Complementar": "lei_complementares",
            "Constituição Estadual": "detalhar_constituicao",
            "Decreto": "lei_decretos",
        }

        print("\n--- Expected counts from website ---")
        for name, div_id in types.items():
            if name == "Constituição Estadual":
                print(f"{name}: 1")
                continue

            div = soup.find("div", id=div_id)
            if div:
                table = div.find("table")
                if table:
                    trs = table.find_all("tr", {"class": "visaoQuadrosTr"})
                    print(f"{name}: {len(trs)}")
                else:
                    print(f"{name}: 0 (No table found)")
            else:
                print(f"{name}: DIV NOT FOUND")
    except Exception as e:
        print(f"Error fetching website: {e}")


if __name__ == "__main__":
    main()
