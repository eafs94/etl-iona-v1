import pandas as pd
from pathlib import Path
from datetime import date

# Paths
BASE_DIR = Path(__file__).resolve().parent.parent

ORIGEM_DIR = BASE_DIR / "origem_dos_dados"
BRONZE_DIR = BASE_DIR / "data" / "bronze"

DATA_INGESTAO = date.today().isoformat()

# Função genérica de ingestão
def ingestao_bronze(nome_tabela: str, nome_arquivo: str):
    print(f"Iniciando ingestão bronze: {nome_tabela}")

    df = pd.read_csv(ORIGEM_DIR / nome_arquivo)

    destino = (
        BRONZE_DIR
        / nome_tabela
        / f"data_ingestao={DATA_INGESTAO}"
    )

    destino.mkdir(parents=True, exist_ok=True)

    df.to_csv(destino / f"{nome_tabela}.csv", index=False)
    print(df.head())

    print(f"Tabela {nome_tabela} ingerida com sucesso.")

# Execução
if __name__ == "__main__":
    ingestao_bronze("produtos", "tabela_produtos.csv")
    ingestao_bronze("vendas", "tabela_vendas.csv")
