from src.pipeline import run_pipeline


def main():
    print("Iniciando o pipeline de dados da ANTT...")
    run_pipeline(skip_extract=True)


if __name__ == "__main__":
    main()
