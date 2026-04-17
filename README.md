# Monitoramento de Operadores TRIIP ANTT

Este projeto implementa um pipeline de dados para extrair, transformar e agregar informações sobre as empresas de transporte de passageiros (Regular, Fretamento e Semiurbano) habilitadas pela Agência Nacional de Transportes Terrestres (ANTT), a partir de seu portal de dados abertos.

O objetivo é criar uma série histórica confiável do número de operadores ativos por categoria, servindo como base para análises e relatórios.

## ✨ Funcionalidades

- **Extração Automática**: Baixa todos os datasets de operadores habilitados (Fretamento, Regular, Semiurbano) diretamente da API de Dados Abertos da ANTT.
- **Limpeza e Padronização**: Unifica os múltiplos arquivos CSV, que possuem schemas e codificações diferentes, em um formato padronizado.
- **Integração de Dados Legados**: Incorpora snapshots históricos (CSVs antigos) armazenados localmente na pasta `data/raw/legacy`, garantindo uma série temporal mais abrangente.
- **Série Histórica**: Processa os snapshots mensais para gerar uma série histórica anual do número de operadores únicos por categoria.
- **Orquestração Robusta**: Utiliza **Prefect** para orquestrar o pipeline (Extract → Transform → Aggregate), garantindo a ordem de execução, logs detalhados e resiliência.
- **Versionamento de Dados**: Salva os artefatos das camadas Silver e Gold com um timestamp, criando um histórico de execuções e facilitando a rastreabilidade.

## 🛠️ Tecnologias Utilizadas

- **Python 3.13+**
- **Polars**: Para manipulação de dados de alta performance.
- **Prefect**: Para orquestração do fluxo de trabalho (pipeline).
- **Requests**: Para realizar as chamadas HTTP à API da ANTT.
- **uv**: Para gerenciamento de ambiente virtual e dependências.

## 📂 Estrutura do Projeto

```text
.
├── data/
│   ├── raw/        # CSVs brutos baixados da ANTT
│   │   └── legacy/ # Arquivos CSV de histórico legado
│   ├── silver/     # Dados limpos e unificados (Parquet)
│   └── gold/       # Dados agregados para análise (Parquet)
├── src/
│   ├── extract.py
│   ├── transform.py
│   ├── aggregate.py
│   └── pipeline.py # Orquestrador Prefect
├── main.py         # Ponto de entrada para executar o pipeline
└── ...
```

## 🚀 Instalação e Execução

### 1. Pré-requisitos

Certifique-se de ter o [uv](https://github.com/astral-sh/uv) instalado.

### 2. Instalação

Clone o repositório e instale as dependências:

```bash
# Cria o ambiente virtual e instala as dependências do pyproject.toml
uv sync
```

### 3. Execução do Pipeline

Para rodar o pipeline completo (Extract → Transform → Aggregate), execute:

```bash
uv run python main.py
```

Ao final da execução, os arquivos processados estarão disponíveis nas pastas `data/silver` e `data/gold`.

## 📊 Camadas de Dados (Medallion Architecture)

- **Raw (`data/raw`):** Contém os arquivos CSV originais baixados da ANTT. A raiz da pasta é limpa a cada execução para garantir que apenas os dados mais recentes da API sejam processados, mantendo intactos os arquivos históricos em `data/raw/legacy/`.
- **Silver (`data/silver`):** Contém arquivos Parquet versionados (`empresas_<timestamp>.parquet`) com os dados de todos os CSVs unificados, limpos e com colunas padronizadas. Esta camada representa a "fonte da verdade" para os dados processados.
- **Gold (`data/gold`):** Contém arquivos Parquet versionados (`historico_operadores_<timestamp>.parquet`) com a série histórica anual de operadores ativos por categoria, pronto para consumo por dashboards ou relatórios.

## ⚙️ Orquestração com Prefect

O pipeline é gerenciado pelo Prefect, o que permite um controle fino sobre a execução. Para visualizar o dashboard web com o histórico de execuções, status e logs, execute em um terminal separado:

```bash
uv run prefect server start
```

Depois, acesse `http://127.0.0.1:4200` no seu navegador.
