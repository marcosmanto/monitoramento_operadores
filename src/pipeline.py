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
