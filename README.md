# Pipeline ETL – Arquitetura Bronze • Silver • Gold

Este repositório implementa um **pipeline completo de dados** seguindo o padrão **Medallion Architecture (Bronze / Silver / Gold)**, utilizando **Python + Pandas**, com foco em **qualidade, rastreabilidade e clareza de responsabilidades entre camadas**.

O projeto foi construído com o objetivo de **simular um cenário real de engenharia de dados**, priorizando decisões arquiteturais corretas em vez de complexidade desnecessária.

---

## Objetivo do Projeto

Demonstrar, de forma prática e organizada:

* Ingestão de dados brutos (**Bronze**)
* Tratamento, padronização e tipagem (**Silver**)
* Modelagem analítica e métricas de negócio (**Gold**)

Tudo isso de maneira **reexecutável**, **auditável** e **evolutiva**.

---

## Arquitetura de Dados

```
data/
├── bronze/   → dados crus (raw)
├── silver/   → dados tratados e tipados
└── gold/     → dados de negócio e métricas
```

Cada camada possui **responsabilidades bem definidas**, evitando mistura de lógica e garantindo confiabilidade no pipeline.

---

## Camada Bronze – Ingestão Raw

### Princípios

* Nenhuma transformação de dados
* Nenhuma inferência forçada
* Dados salvos **exatamente como chegam da origem**
* Particionamento por data de ingestão

### Estrutura

```
data/bronze/
├── produtos/
│   └── data_ingestao=YYYY-MM-DD/
│       └── produtos.csv
└── vendas/
    └── data_ingestao=YYYY-MM-DD/
        └── vendas.csv
```

### O que acontece na Bronze

* Leitura dos arquivos CSV de origem
* Criação de diretórios por `data_ingestao`
* Escrita dos dados sem qualquer modificação

Essa camada garante **auditabilidade total**.

---

## Camada Silver – Qualidade e Tipagem

### Princípios

* Conversão explícita de tipos
* Correção de inconsistências conhecidas
* Padronização de dados
* Preparação para joins e agregações

### Estrutura

```
data/silver/
├── produtos/
│   └── produtos.parquet
└── vendas/
    └── vendas.parquet
```

### Tratamentos aplicados

**Produtos**

* `cod` → `int`
* Colunas textuais mantidas como string

**Vendas**

* `cod_produto` → `int`
* `valor_venda` → normalização de formatos (`_`, `,`, `.`) e conversão para `float`
* `data_venda` → `datetime`

O resultado é um dataset **confiável e consistente**.

---

## Camada Gold – Negócio e Métricas

A camada Gold responde perguntas de negócio.

### Estrutura

```
data/gold/
├── vendas_enriquecidas/
│   └── vendas_enriquecidas.parquet
├── faturamento_dia/
│   └── faturamento_dia.parquet
├── faturamento_produto/
│   └── faturamento_produto.parquet
└── faturamento_categoria/
    └── faturamento_categoria.parquet
```

---

### Gold – Vendas Enriquecidas

Join entre **vendas** e **produtos**, resultando em uma base analítica única:

* codigo
* descricao
* categoria
* valor
* data da venda

Cada linha representa **uma venda explicada para o negócio**.

---

### Gold – Métricas Analíticas

#### Faturamento por Dia

* Soma do valor vendido por data
* Quantidade de vendas por dia

#### Faturamento por Produto

* Total vendido por produto
* Quantidade de vendas

#### Faturamento por Categoria

* Total vendido por categoria
* Volume de vendas

Essas tabelas estão **prontas para BI, dashboards ou APIs**.

---

## Execução do Pipeline

A execução é sequencial:

```bash
python scripts/bronze.py
python scripts/silver.py
python scripts/gold.py
```

Cada script pode ser reexecutado de forma independente.

---

## Boas Práticas Aplicadas

* Separação clara de responsabilidades
* Sem erros silenciosos de tipagem
* Dados sempre auditáveis
* Código simples, legível e evolutivo
* Arquitetura compatível com Spark / BigQuery / Databricks

---

## Possíveis Evoluções

* Orquestração com Airflow ou Dagster
* Validações automáticas de dados
* Migração para Spark ou Cloud
* Consumo em ferramentas de BI
* Testes automatizados

---

## Conclusão

Este projeto demonstra um **pipeline de engenharia de dados completo**, seguindo padrões utilizados em ambientes profissionais.

Mais do que tecnologia, o foco está em **arquitetura, clareza e qualidade de dados**.

---

**Autor:** Eric Adam Ferreira Santos
