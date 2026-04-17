# CONTEXTO COMPLETO DO PROJETO

## ESTRUTURA DE PASTAS:
```text
.
├── notebooks
├── src
│   ├── aggregate.py
│   ├── extract.py
│   ├── transform.py
│   └── utils.py
├── .gitignore
├── main.py
├── pyproject.toml
└── README.md
```

## CONTEÚDO DOS ARQUIVOS:

### Arquivo: `.gitignore`
```text
# Python-generated files
__pycache__/
*.py[oc]
build/
dist/
wheels/
*.egg-info

# Virtual environments
.venv
```

### Arquivo: `main.py`
```py
def main():
    print("Hello from monitoramento-operadores!")


if __name__ == "__main__":
    main()
```

### Arquivo: `pyproject.toml`
```toml
[project]
name = "monitoramento-operadores"
version = "0.1.0"
description = "Add your description here"
readme = "README.md"
requires-python = ">=3.13"
dependencies = [
    "polars>=1.39.3",
    "pyarrow>=23.0.1",
    "requests>=2.33.1",
]
```

### Arquivo: `README.md`
```md

```

### Arquivo: `src/aggregate.py`
```py
from pathlib import Path

import polars as pl

ANOS = list(range(2016, 2026))


def load():
    return pl.read_parquet("data/silver/empresas.parquet")


def snapshot_ano(df: pl.DataFrame):
    resultados = []

    for ano in ANOS:
        # Filtra todos os snapshots para o ano corrente
        df_ano = df.filter(pl.col("data_snapshot").dt.year() == ano)

        # Se não houver dados para este ano, pula para o próximo
        if df_ano.height == 0:
            continue

        # Encontra a data do último snapshot disponível para aquele ano
        latest_snapshot_date = df_ano.select(pl.max("data_snapshot")).item()

        # Filtra o DataFrame para conter apenas os dados do último snapshot
        df_latest_snapshot = df_ano.filter(
            pl.col("data_snapshot") == latest_snapshot_date
        )

        # Agrega os dados deste snapshot final para o ano
        agg = (
            df_latest_snapshot.group_by("categoria")
            .agg(pl.col("cnpj").n_unique().alias("qtd"))
            .with_columns(pl.lit(ano).alias("ano"))
        )

        resultados.append(agg)

    if not resultados:
        return (
            pl.DataFrame()
        )  # Retorna um DataFrame vazio se nenhum dado foi processado

    return pl.concat(resultados)


def save_gold(df: pl.DataFrame):
    Path("data/gold").mkdir(parents=True, exist_ok=True)
    df.write_parquet("data/gold/historico_operadores.parquet")


if __name__ == "__main__":
    df = load()
    hist = snapshot_ano(df)
    save_gold(hist)
```

### Arquivo: `src/extract.py`
```py
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
```

### Arquivo: `src/transform.py`
```py
from pathlib import Path

import polars as pl


def standardize_columns(df: pl.DataFrame) -> pl.DataFrame:
    """
    Standardizes column names to a common format (lowercase, snake_case)
    and maps known aliases to a single name.
    """
    rename_map = {
        "Razão Social": "razao_social",
        "CNPJ": "cnpj",
        "Número TAF": "numero_taf",
        "Número TAR": "numero_tar",
        # Unificando as colunas de validade/vigência
        "Vigência": "data_validade",
        "vigencia": "data_validade",
        "Validade Habilitação": "data_validade",
        "validade_habilitacao": "data_validade",
        # Outras colunas
        "Situação Empresa": "situacao_empresa",
        "TIPO_SERVICO": "tipo_servico",
    }

    current_columns = df.columns
    actual_rename_map = {}
    for col in current_columns:
        clean_col = col.strip()
        if clean_col in rename_map:
            actual_rename_map[col] = rename_map[clean_col]
        else:
            actual_rename_map[col] = clean_col.lower().replace(" ", "_")

    return df.rename(actual_rename_map)


def load_raw():
    dfs = []
    essential_cols = ["cnpj", "data_validade"]
    for file in Path("data/raw").glob("*.csv"):
        try:
            df = pl.read_csv(
                file,
                separator=";",
                encoding="latin-1",
                infer_schema_length=1000,
                ignore_errors=True,
                truncate_ragged_lines=True,
            )

            # 1. Standardize column names
            df = standardize_columns(df)

            # 2. Ensure essential columns exist, creating them with nulls if not
            for col in essential_cols:
                if col not in df.columns:
                    df = df.with_columns(pl.lit(None).alias(col))

            # 3. Add the origin file name
            df = df.with_columns(pl.lit(file.name).alias("arquivo_origem"))
            dfs.append(df)
        except Exception as e:
            print(f"Aviso: Não foi possível ler {file} - {e}")

    if not dfs:
        raise ValueError(
            "Nenhum arquivo CSV foi lido com sucesso. A pasta 'data/raw' pode estar vazia ou os arquivos estão corrompidos."
        )

    return pl.concat(dfs, how="diagonal")


def add_snapshot_date(df: pl.DataFrame) -> pl.DataFrame:
    """Extrai a data do snapshot a partir do nome do arquivo de origem."""
    # Extrai as partes de mês e ano do nome do arquivo
    df = df.with_columns(
        mes_str=pl.col("arquivo_origem").str.extract(r"([A-Za-z]{3})", 1),
        ano_str=pl.col("arquivo_origem").str.extract(r"(\d{2})", 1).cast(pl.Int16),
    )

    # Mapeia a abreviação do mês para um número
    mes_expr = (
        pl.when(pl.col("mes_str") == "Jan")
        .then(1)
        .when(pl.col("mes_str") == "Fev")
        .then(2)
        .when(pl.col("mes_str") == "Mar")
        .then(3)
        .when(pl.col("mes_str") == "Abr")
        .then(4)
        .when(pl.col("mes_str") == "Mai")
        .then(5)
        .when(pl.col("mes_str") == "Jun")
        .then(6)
        .when(pl.col("mes_str") == "Jul")
        .then(7)
        .when(pl.col("mes_str") == "Ago")
        .then(8)
        .when(pl.col("mes_str") == "Set")
        .then(9)
        .when(pl.col("mes_str") == "Out")
        .then(10)
        .when(pl.col("mes_str") == "Nov")
        .then(11)
        .when(pl.col("mes_str") == "Dez")
        .then(12)
        .otherwise(None)
        .alias("mes_int")
    )

    # Constrói a data do snapshot e remove colunas temporárias
    return (
        df.with_columns(mes_expr)
        .with_columns(
            data_snapshot=pl.date(pl.col("ano_str") + 2000, pl.col("mes_int"), 1)
        )
        .drop("mes_str", "ano_str", "mes_int")
    )


def normalize(df: pl.DataFrame) -> pl.DataFrame:
    return df.with_columns(
        [
            pl.coalesce(
                [
                    pl.col("data_validade").str.to_date(
                        format="%d/%m/%Y", strict=False
                    ),
                    pl.col("data_validade").str.to_date(
                        format="%Y-%m-%d", strict=False
                    ),
                ]
            ).alias("data_validade")
        ]
    )


def classify_categoria(df: pl.DataFrame) -> pl.DataFrame:
    return df.with_columns(
        [
            pl.when(pl.col("arquivo_origem").str.to_lowercase().str.contains("fret"))
            .then(pl.lit("Fretamento"))
            .when(pl.col("arquivo_origem").str.to_lowercase().str.contains("reg"))
            .then(pl.lit("Regular"))
            .when(pl.col("arquivo_origem").str.to_lowercase().str.contains("semi"))
            .then(pl.lit("Semiurbano"))
            .otherwise(pl.lit("Outros"))
            .alias("categoria")
        ]
    )


def save_silver(df: pl.DataFrame):
    Path("data/silver").mkdir(parents=True, exist_ok=True)
    df.write_parquet("data/silver/empresas.parquet")


if __name__ == "__main__":
    df = load_raw()
    df = add_snapshot_date(df)
    df = normalize(df)
    df = classify_categoria(df)
    save_silver(df)
```

### Arquivo: `src/utils.py`
```py
# Funções utilitárias

def saudacao():
    print("Utils funcionando!")
```

