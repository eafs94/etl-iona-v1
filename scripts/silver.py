import pandas as pd
from pathlib import Path
from datetime import date

# Paths
BASE_DIR = Path(__file__).resolve().parent.parent

BRONZE_DIR = BASE_DIR / "data" / "bronze"
SILVER_DIR = BASE_DIR / "data" / "silver"

SILVER_DIR.mkdir(parents=True, exist_ok=True)

DATA_INGESTAO = date.today().isoformat()

# Produtos
def silver_produtos():
    df = pd.read_csv(BRONZE_DIR / "produtos" / f"data_ingestao={DATA_INGESTAO}" / "produtos.csv")

    df.columns = ["cod", "descricao", "categoria"]

    df["cod"] = df["cod"].astype(int)

    df.to_parquet(
        SILVER_DIR / "produtos" / "produtos.parquet",
        index=False
    )
    print(df.dtypes)
    print(df.head())

# Vendas
def silver_vendas():
    df = pd.read_csv(BRONZE_DIR / "vendas" / f"data_ingestao={DATA_INGESTAO}" / "vendas.csv")

    df.columns = ["cod_produto", "valor_venda", "data_venda"]

    df["cod_produto"] = df["cod_produto"].astype(int)
    df["valor_venda"] = (
        df["valor_venda"]
        .astype(str)
        .str.replace("_", ",", regex=False)
        .str.replace(",", ".", regex=False)
        .astype(float)
    )
    df["data_venda"] = pd.to_datetime(df["data_venda"])

    df.to_parquet(
        SILVER_DIR / "vendas" / "vendas.parquet",
        index=False
    )
    print(df.dtypes)
    print(df.head())

# Execução
if __name__ == "__main__":
    (SILVER_DIR / "produtos").mkdir(parents=True, exist_ok=True)
    (SILVER_DIR / "vendas").mkdir(parents=True, exist_ok=True)

    silver_produtos()
    silver_vendas()

    print("Camada silver gerada com sucesso.")
