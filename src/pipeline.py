from prefect import flow, task

from src.aggregate import load as load_silver
from src.aggregate import save_gold, snapshot_ano

# Importa as funções do seu projeto
from src.extract import download_raw
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
    save_silver(df)


@task
def task_aggregate():
    df = load_silver()
    hist = snapshot_ano(df)
    save_gold(hist)


@flow(name="Pipeline Monitoramento Operadores")
def run_pipeline():
    e = task_extract()
    t = task_transform(wait_for=[e])
    task_aggregate(wait_for=[t])
