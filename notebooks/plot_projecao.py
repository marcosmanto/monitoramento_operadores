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
