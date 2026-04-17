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
