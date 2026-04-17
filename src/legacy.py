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
