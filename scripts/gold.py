import pandas as pd
from pathlib import Path

# Paths
BASE_DIR = Path(__file__).resolve().parent.parent

SILVER_DIR = BASE_DIR / "data" / "silver"
GOLD_DIR = BASE_DIR / "data" / "gold"

(GOLD_DIR / "vendas_enriquecidas").mkdir(parents=True, exist_ok=True)
(GOLD_DIR / "faturamento_dia").mkdir(parents=True, exist_ok=True)
(GOLD_DIR / "faturamento_produto").mkdir(parents=True, exist_ok=True)
(GOLD_DIR / "faturamento_categoria").mkdir(parents=True, exist_ok=True)

# Gold - Join de Negócio
def gold_vendas_enriquecidas():
    vendas = pd.read_parquet(
        SILVER_DIR / "vendas" / "vendas.parquet"
    )

    produtos = pd.read_parquet(
        SILVER_DIR / "produtos" / "produtos.parquet"
    )

    df = vendas.merge(
        produtos,
        left_on="cod_produto",
        right_on="cod",
        how="inner"
    )

    df = df[
        [
            "cod_produto",
            "descricao",
            "categoria",
            "valor_venda",
            "data_venda",
        ]
    ]

    df.to_parquet(
        GOLD_DIR / "vendas_enriquecidas" / "vendas_enriquecidas.parquet",
        index=False
    )
    print(df.dtypes)
    print(df.head())

def carregar_base_gold():
    return pd.read_parquet(
        GOLD_DIR / "vendas_enriquecidas" / "vendas_enriquecidas.parquet"
    )

# Gold - Faturamento por dia
def gold_faturamento_dia():
    df = carregar_base_gold()

    faturamento = (
        df
        .groupby("data_venda", as_index=False)
        .agg(
            faturamento_total=("valor_venda", "sum"),
            quantidade_vendas=("valor_venda", "count")
        )
        .sort_values("data_venda")
    )

    faturamento.to_parquet(
        GOLD_DIR / "faturamento_dia" / "faturamento_dia.parquet",
        index=False
    )

    print("\nGold - Faturamento por dia")
    print(faturamento.dtypes)
    print(faturamento.head())

# Gold - Faturamento por produto
def gold_faturamento_produto():
    df = carregar_base_gold()

    faturamento = (
        df
        .groupby(
            ["cod_produto", "descricao"],
            as_index=False
        )
        .agg(
            faturamento_total=("valor_venda", "sum"),
            quantidade_vendas=("valor_venda", "count")
        )
        .sort_values("faturamento_total", ascending=False)
    )

    faturamento.to_parquet(
        GOLD_DIR / "faturamento_produto" / "faturamento_produto.parquet",
        index=False
    )

    print("\nGold - Faturamento por produto")
    print(faturamento.dtypes)
    print(faturamento.head())

# Gold - Faturamento por categoria
def gold_faturamento_categoria():
    df = carregar_base_gold()

    faturamento = (
        df
        .groupby("categoria", as_index=False)
        .agg(
            faturamento_total=("valor_venda", "sum"),
            quantidade_vendas=("valor_venda", "count")
        )
        .sort_values("faturamento_total", ascending=False)
    )

    faturamento.to_parquet(
        GOLD_DIR / "faturamento_categoria" / "faturamento_categoria.parquet",
        index=False
    )

    print("\nGold - Faturamento por categoria")
    print(faturamento.dtypes)
    print(faturamento.head())

# Execução
if __name__ == "__main__":
    gold_vendas_enriquecidas()
    gold_faturamento_dia()
    gold_faturamento_produto()
    gold_faturamento_categoria()

    print("\nCamada Gold (métricas) gerada com sucesso.")
