from pathlib import Path

import requests

DATASET_ID = "empresas-habilitadas"


def get_csv_urls():
    url = f"https://dados.antt.gov.br/api/3/action/package_show?id={DATASET_ID}"
    res = requests.get(url).json()

    csv_resources = []
    for r in res["result"]["resources"]:
        if r["format"].lower() == "csv":
            # Cria um nome de arquivo seguro usando o nome do recurso vindo da API
            safe_name = "".join([c if c.isalnum() else "_" for c in r["name"]]).strip(
                "_"
            )
            csv_resources.append((f"{safe_name}.csv", r["url"]))

    if not csv_resources:
        raise Exception("Nenhum CSV encontrado")
    return csv_resources


def download_raw():
    Path("data/raw").mkdir(parents=True, exist_ok=True)

    # Limpa os arquivos antigos para evitar sujeira de execuções anteriores
    for old_file in Path("data/raw").glob("*.csv"):
        old_file.unlink()

    csvs = get_csv_urls()
    for filename, csv_url in csvs:
        print(f"Baixando dados de: {csv_url}")
        response = requests.get(csv_url)

        filepath = Path("data/raw") / filename
        with open(filepath, "wb") as f:
            f.write(response.content)
        print(f"Download concluído: {filepath}")


if __name__ == "__main__":
    download_raw()
