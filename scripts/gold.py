import pandas as pd
from pathlib import Path

# Paths
BASE_DIR = Path(__file__).resolve().parent.parent

SILVER_DIR = BASE_DIR / "data" / "silver"
GOLD_DIR = BASE_DIR / "data" / "gold"

(GOLD_DIR / "vendas_enriquecidas").mkdir(parents=True, exist_ok=True)

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

# Execução
if __name__ == "__main__":
    gold_vendas_enriquecidas()
    print("Camada Gold (join) gerada com sucesso.")
