import httpx
import asyncio
from bs4 import BeautifulSoup
import re
import urllib3
from datetime import datetime
import time

urllib3.disable_warnings()

TYPES = {
    "Decreto Legislativo": 14,
    "Lei Complementar": 12,
    "Lei Ordinária": 13,
    "Resolução": 15,
    "Emenda Constitucional": 11,
}

async def fetch_year_count(client, name, norm_type_id, year):
    url = f"https://al.ap.leg.br/pagina.php?pg=buscar_legislacao&aba=legislacao&submenu=listar_legislacao&especie_documento={norm_type_id}&ano={year}&pesquisa=&n_doeB=&n_leiB=&data_inicial=&data_final=&orgaoB=&autor=&legislaturaB=&pagina=1"
    try:
        response = await client.get(url)
        soup = BeautifulSoup(response.text, "html.parser")
        
        # Check if empty results based on tbody
        items = soup.find("tbody").find_all("tr") if soup.find("tbody") else []
        if len(items) == 0:
            return 0
        
        for p in soup.find_all("p"):
            text = p.text.strip()
            if "Encontramos" in text:
                match = re.search(r"Encontramos\s+(\d+)\s+resutados", text)
                if match:
                    return int(match.group(1))
        # If no "Encontramos" but items exist, maybe it's less than 20? 
        return len(items)
        
    except Exception as e:
        print(f"Error fetching {name} {year}: {e}")
        return 0

async def main():
    start_year = 1991
    end_year = datetime.now().year
    
    print(f"Fetching expected counts for AMAPÁ (Years: {start_year}-{end_year})...")
    
    total_expected = {name: 0 for name in TYPES}
    
    async with httpx.AsyncClient(verify=False, timeout=30.0) as client:
        for name, norm_type_id in TYPES.items():
            tasks = []
            for year in range(start_year, end_year + 1):
                tasks.append(fetch_year_count(client, name, norm_type_id, year))
            
            counts = await asyncio.gather(*tasks)
            total_expected[name] = sum(counts)
            print(f"{name}: {total_expected[name]}")
            
    print("\n--- Total Expected ---")
    total = sum(total_expected.values())
    for k, v in total_expected.items():
        print(f"{k}: {v}")
    print(f"Grand Total: {total}")

if __name__ == "__main__":
    asyncio.run(main())
