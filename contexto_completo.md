# CONTEXTO COMPLETO DO PROJETO

## ESTRUTURA DE PASTAS:
```text
.
├── notebooks
│   └── plot_projecao.py
├── src
│   ├── aggregate.py
│   ├── extract.py
│   ├── legacy.py
│   ├── pipeline.py
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
# --- Python ---
# Byte-compiled / optimized / DLL files
__pycache__/
*.py[oc]
*.so

# Distribution / packaging
build/
dist/
wheels/
*.egg-info/
*.egg

# --- Orchestration ---
# Prefect local database
prefect.db
prefect.db-journal

# --- Data Outputs ---
# Ignore all generated data files.
# Add a .gitkeep file in these directories if you want to commit the empty folder structure.
data/raw/*
!data/raw/legacy/
data/silver/
data/gold/

# --- IDE / Editor ---
.vscode/
.idea/

# --- OS Generated Files ---
.DS_Store
Thumbs.db

# --- Environment & Logs ---
.env
*.log
```

### Arquivo: `main.py`
```py
from src.pipeline import run_pipeline


def main():
    print("Iniciando o pipeline de dados da ANTT...")
    run_pipeline(skip_extract=True)


if __name__ == "__main__":
    main()
```

### Arquivo: `pyproject.toml`
```toml
[project]
name = "monitoramento-operadores"
version = "0.2.0"
description = "Monitoramento de Operadores TRIIP"
readme = "README.md"
requires-python = ">=3.13"
dependencies = [
    "matplotlib>=3.10.8",
    "numpy>=2.4.4",
    "polars>=1.39.3",
    "prefect>=3.6.26",
    "pyarrow>=23.0.1",
    "requests>=2.33.1",
]
```

### Arquivo: `README.md`
```md
# Monitoramento de Operadores TRIIP ANTT

Este projeto implementa um pipeline de dados para extrair, transformar e agregar informações sobre as empresas de transporte de passageiros (Regular, Fretamento e Semiurbano) habilitadas pela Agência Nacional de Transportes Terrestres (ANTT), a partir de seu portal de dados abertos.

O objetivo é criar uma série histórica confiável do número de operadores ativos por categoria, servindo como base para análises e relatórios.

## ✨ Funcionalidades

- **Extração Automática**: Baixa todos os datasets de operadores habilitados (Fretamento, Regular, Semiurbano) diretamente da API de Dados Abertos da ANTT.
- **Limpeza e Padronização**: Unifica os múltiplos arquivos CSV, que possuem schemas e codificações diferentes, em um formato padronizado.
- **Integração de Dados Legados**: Incorpora snapshots históricos (CSVs antigos) armazenados localmente na pasta `data/raw/legacy`, garantindo uma série temporal mais abrangente.
- **Série Histórica**: Processa os snapshots mensais para gerar uma série histórica anual do número de operadores únicos por categoria.
- **Orquestração Robusta**: Utiliza **Prefect** para orquestrar o pipeline (Extract → Transform → Aggregate), garantindo a ordem de execução, logs detalhados e resiliência.
- **Versionamento de Dados**: Salva os artefatos das camadas Silver e Gold com um timestamp, criando um histórico de execuções e facilitando a rastreabilidade.

## 🛠️ Tecnologias Utilizadas

- **Python 3.13+**
- **Polars**: Para manipulação de dados de alta performance.
- **Prefect**: Para orquestração do fluxo de trabalho (pipeline).
- **Requests**: Para realizar as chamadas HTTP à API da ANTT.
- **uv**: Para gerenciamento de ambiente virtual e dependências.

## 📂 Estrutura do Projeto

```text
.
├── data/
│   ├── raw/        # CSVs brutos baixados da ANTT
│   │   └── legacy/ # Arquivos CSV de histórico legado
│   ├── silver/     # Dados limpos e unificados (Parquet)
│   └── gold/       # Dados agregados para análise (Parquet)
├── src/
│   ├── extract.py
│   ├── transform.py
│   ├── aggregate.py
│   └── pipeline.py # Orquestrador Prefect
├── main.py         # Ponto de entrada para executar o pipeline
└── ...
```

## 🚀 Instalação e Execução

### 1. Pré-requisitos

Certifique-se de ter o [uv](https://github.com/astral-sh/uv) instalado.

### 2. Instalação

Clone o repositório e instale as dependências:

```bash
# Cria o ambiente virtual e instala as dependências do pyproject.toml
uv sync
```

### 3. Execução do Pipeline

Para rodar o pipeline completo (Extract → Transform → Aggregate), execute:

```bash
uv run python main.py
```

Ao final da execução, os arquivos processados estarão disponíveis nas pastas `data/silver` e `data/gold`.

## 📊 Camadas de Dados (Medallion Architecture)

- **Raw (`data/raw`):** Contém os arquivos CSV originais baixados da ANTT. A raiz da pasta é limpa a cada execução para garantir que apenas os dados mais recentes da API sejam processados, mantendo intactos os arquivos históricos em `data/raw/legacy/`.
- **Silver (`data/silver`):** Contém arquivos Parquet versionados (`empresas_<timestamp>.parquet`) com os dados de todos os CSVs unificados, limpos e com colunas padronizadas. Esta camada representa a "fonte da verdade" para os dados processados.
- **Gold (`data/gold`):** Contém arquivos Parquet versionados (`historico_operadores_<timestamp>.parquet`) com a série histórica anual de operadores ativos por categoria, pronto para consumo por dashboards ou relatórios.

## ⚙️ Orquestração com Prefect

O pipeline é gerenciado pelo Prefect, o que permite um controle fino sobre a execução. Para visualizar o dashboard web com o histórico de execuções, status e logs, execute em um terminal separado:

```bash
uv run prefect server start
```

Depois, acesse `http://127.0.0.1:4200` no seu navegador.
```

### Arquivo: `notebooks/plot_projecao.py`
```py
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import polars as pl


def main():
    # Tenta usar o arquivo especificado, mas faz fallback para o mais recente se não existir
    target_file = Path("data/gold/historico_operadores_20260417_145132.parquet")

    if not target_file.exists():
        print(f"Aviso: Arquivo {target_file.name} não encontrado.")
        try:
            target_file = sorted(
                Path("data/gold").glob("historico_operadores_*.parquet")
            )[-1]
            print(f"Usando o arquivo mais recente: {target_file}")
        except IndexError:
            raise FileNotFoundError(
                "Nenhum arquivo parquet encontrado em 'data/gold/'."
            )

    # Carrega os dados agregados
    df = pl.read_parquet(target_file)

    # Obtém a lista de categorias únicas
    categorias = df["categoria"].unique().to_list()

    # Configura o tamanho e estilo básico do gráfico
    plt.figure(figsize=(12, 7))
    anos_futuros = 3  # Quantos anos projetar no futuro

    print("--- Projeções para os próximos anos ---")

    for cat in categorias:
        df_cat = df.filter(pl.col("categoria") == cat).sort("ano")

        x = df_cat["ano"].to_numpy()
        y = df_cat["qtd"].to_numpy()

        if len(x) < 2:
            print(f"[{cat}] Dados insuficientes para projeção.")
            continue

        # Plota os dados REAIS e captura a linha gerada
        linha_real = plt.plot(
            x,
            y,
            marker="o",
            linestyle="-",
            linewidth=2,
            label=f"{cat} (Real)",
        )[0]

        # Extrai a cor gerada automaticamente para usar na projeção
        color = linha_real.get_color()

        # Calcula a REGRESSÃO LINEAR (Tendência de Grau 1)

        # usa só últimos 4 anos
        x_fit = x[-5:]
        y_fit = y[-5:]

        z = np.polyfit(x_fit, y_fit, 1)
        tendencia = np.poly1d(z)

        # Cria o eixo X com os anos futuros e plota a PROJEÇÃO
        x_proj = np.arange(x.min(), x.max() + anos_futuros + 1)
        y_proj = tendencia(x_proj)
        plt.plot(
            x_proj,
            y_proj,
            linestyle="--",
            alpha=0.6,
            label=f"{cat} (Tendência)",
            color=color,
        )

    plt.title(
        "Evolução e Projeção do Número de Operadores por Categoria",
        fontsize=14,
        fontweight="bold",
    )
    plt.xlabel("Ano", fontsize=12)
    plt.ylabel("Quantidade de Operadores Únicos", fontsize=12)
    plt.xticks(np.arange(df["ano"].min(), df["ano"].max() + anos_futuros + 1, 1))
    plt.grid(True, linestyle=":", alpha=0.7)
    plt.legend(bbox_to_anchor=(1.05, 1), loc="upper left")
    plt.tight_layout()

    # Salva a imagem localmente e a exibe
    output_path = Path("notebooks/projecao_categorias.png")
    output_path.parent.mkdir(exist_ok=True)
    plt.savefig(output_path, dpi=300)
    print(f"\n✔ Gráfico salvo com sucesso em: {output_path.resolve()}")
    plt.show()


if __name__ == "__main__":
    main()
```

### Arquivo: `src/aggregate.py`
```py
from datetime import datetime
from pathlib import Path

import polars as pl


def load():
    silver_folder = Path("data/silver")
    try:
        # Encontra o arquivo silver mais recente baseado no timestamp do nome
        latest_file = sorted(silver_folder.glob("empresas_*.parquet"))[-1]
    except IndexError:
        raise FileNotFoundError(
            "Nenhum arquivo silver encontrado em 'data/silver/'. Rode a etapa de transformação primeiro."
        )
    print(f"Lendo camada Silver de: {latest_file}")
    return pl.read_parquet(latest_file)


def snapshot_ano(df: pl.DataFrame):
    # Garante que a coluna seja tratada como Data
    df = df.with_columns(pl.col("data_snapshot").cast(pl.Date))

    resultados = []

    # Obtém os anos disponíveis dinamicamente direto dos dados
    anos_disponiveis = (
        df.select(pl.col("data_snapshot").dt.year().unique().drop_nulls())
        .to_series()
        .to_list()
    )

    for ano in sorted(anos_disponiveis):
        # Filtra todos os snapshots para o ano corrente
        df_ano = df.filter(pl.col("data_snapshot").dt.year() == ano)

        # Encontra a data do último snapshot disponível para aquele ano
        latest_snapshot_date = df_ano.select(pl.max("data_snapshot")).item()

        # Filtra o DataFrame para conter apenas os dados do último snapshot
        df_latest_snapshot = df_ano.filter(
            pl.col("data_snapshot") == latest_snapshot_date
        )

        # Filtra operadores ativos conforme a regra de cada categoria:
        # - Semiurbano: possui a flag explícita 'situacao_empresa'
        # - Demais: validamos se a licença não expirou
        # df_latest_snapshot = df_latest_snapshot.filter(
        #     (
        #         (pl.col("categoria") == "Semiurbano")
        #         & (pl.col("situacao_empresa") == "Empresa Habilitada")
        #     )
        #     | (
        #         (pl.col("categoria") != "Semiurbano")
        #         & pl.col("data_validade").is_not_null()
        #         & (pl.col("data_validade") >= pl.col("data_snapshot"))
        #     )
        # )
        df_latest_snapshot = df_latest_snapshot.filter(
            (
                # 🔹 SEMIURBANO (CKAN)
                (pl.col("categoria") == "Semiurbano")
                & pl.col("situacao_empresa").is_not_null()
                & (pl.col("situacao_empresa") == "Empresa Habilitada")
            )
            | (
                # 🔹 CKAN (Regular + Fretamento)
                (pl.col("categoria") != "Semiurbano")
                & pl.col("data_validade").is_not_null()
                & (pl.col("data_validade") >= pl.col("data_snapshot"))
            )
            | (
                # 🔥 LEGACY (NÃO TEM ESSAS COLUNAS)
                pl.col("data_validade").is_null()
            )
        )

        # Agrega os dados deste snapshot final para o ano
        agg = (
            df_latest_snapshot.group_by("categoria")
            .agg(pl.col("cnpj").n_unique().alias("qtd"))
            .with_columns(
                [
                    pl.lit(ano).alias("ano"),
                    pl.lit(latest_snapshot_date).alias("data_referencia"),
                ]
            )
        )

        resultados.append(agg)

    if not resultados:
        return (
            pl.DataFrame()
        )  # Retorna um DataFrame vazio se nenhum dado foi processado

    return pl.concat(resultados).sort(["ano", "categoria"])


def save_gold(df: pl.DataFrame):
    Path("data/gold").mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filepath = Path("data/gold") / f"historico_operadores_{timestamp}.parquet"
    df.write_parquet(filepath)
    print(f"✔ Camada Gold salva com sucesso em: {filepath}")


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
```

### Arquivo: `src/legacy.py`
```py
from pathlib import Path

import polars as pl


def normalize_legacy(df: pl.DataFrame) -> pl.DataFrame:
    return df.with_columns(
        [
            # garantir tipos essenciais
            pl.col("cnpj").cast(pl.Utf8),
            pl.col("data_snapshot").cast(pl.Date),
        ]
    )


def explode_categorias(df: pl.DataFrame) -> pl.DataFrame:
    df = df.with_columns(
        [
            pl.concat_list(
                [
                    pl.when(pl.col("rodoviarioAtivo") == "sim").then(pl.lit("Regular")),
                    pl.when(pl.col("semiurbanoAtivo") == "sim").then(
                        pl.lit("Semiurbano")
                    ),
                    pl.when(pl.col("fretamentoAtivo") == "sim").then(
                        pl.lit("Fretamento")
                    ),
                ]
            ).alias("categorias")
        ]
    )

    return (
        df.explode("categorias")
        .rename({"categorias": "categoria"})
        .filter(pl.col("categoria").is_not_null())
    )


def load_legacy():
    dfs = []

    for file in Path("data/raw/legacy").glob("*.csv"):
        df = pl.read_csv(
            file,
            separator=",",
            encoding="latin-1",
            schema_overrides={"cnpj": pl.Utf8},
            infer_schema_length=1000,
        )

        ano = int(file.stem)

        df = df.rename(
            {
                "cnpj": "cnpj",
                "razaoSocial": "razao_social",
            }
        )

        df = df.with_columns(
            [
                pl.date(ano, 12, 1).alias("data_snapshot"),
                pl.lit(file.name).alias("arquivo_origem"),
            ]
        )

        df = explode_categorias(df)

        # padroniza tudo como string
        df = df.with_columns(
            [
                pl.all().cast(pl.Utf8),
            ]
        )

        dfs.append(df)

    return pl.concat(dfs, how="diagonal")
```

### Arquivo: `src/pipeline.py`
```py
import polars as pl
from prefect import flow, task

from src.aggregate import load as load_silver
from src.aggregate import save_gold, snapshot_ano

# Importa as funções do seu projeto
from src.extract import download_raw
from src.legacy import load_legacy, normalize_legacy
from src.transform import (
    add_snapshot_date,
    classify_categoria,
    load_raw,
    normalize,
    save_silver,
)


@task
def task_extract():
    download_raw()


@task
def task_transform():
    df = load_raw()

    df = add_snapshot_date(df)
    df = normalize(df)
    df = classify_categoria(df)

    # 👇 legado
    df_legacy = load_legacy()
    df_legacy = normalize_legacy(df_legacy)

    # 👇 GARANTIA DE SCHEMA
    df = df.with_columns(pl.col("data_snapshot").cast(pl.Date))
    df_legacy = df_legacy.with_columns(pl.col("data_snapshot").cast(pl.Date))

    # 👇 união final
    df_final = pl.concat([df, df_legacy], how="diagonal")

    save_silver(df_final)


@task
def task_aggregate():
    df = load_silver()
    hist = snapshot_ano(df)
    save_gold(hist)


@flow(name="Pipeline Monitoramento Operadores")
def run_pipeline(skip_extract: bool = False):
    if not skip_extract:
        print("⬇️ Executando etapa de extract (CKAN)")
        e = task_extract()
        t = task_transform(wait_for=[e])
    else:
        print("⏭️ Pulando etapa de extract")
        t = task_transform()

    print("⬇️ Executando etapa de aggregate")
    task_aggregate(wait_for=[t])
```

### Arquivo: `src/transform.py`
```py
from datetime import datetime
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
                schema_overrides={"cnpj": pl.Utf8},
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


# def normalize(df: pl.DataFrame) -> pl.DataFrame:
#     return df.with_columns(
#         [
#             pl.coalesce(
#                 [
#                     pl.col("data_validade").str.to_date(
#                         format="%d/%m/%Y", strict=False
#                     ),
#                     pl.col("data_validade").str.to_date(
#                         format="%Y-%m-%d", strict=False
#                     ),
#                 ]
#             ).alias("data_validade")
#         ]
#     )


def normalize(df: pl.DataFrame) -> pl.DataFrame:
    return df.with_columns(
        [
            # data_validade
            pl.coalesce(
                [
                    pl.col("data_validade").str.to_date(
                        format="%d/%m/%Y", strict=False
                    ),
                    pl.col("data_validade").str.to_date(
                        format="%Y-%m-%d", strict=False
                    ),
                ]
            ).alias("data_validade"),
            pl.col("data_snapshot").cast(pl.Date).alias("data_snapshot"),
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
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filepath = Path("data/silver") / f"empresas_{timestamp}.parquet"
    df.write_parquet(filepath)
    print(f"✔ Camada Silver salva com sucesso em: {filepath}")


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

