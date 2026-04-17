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
