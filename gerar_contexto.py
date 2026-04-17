from pathlib import Path

# Configurações de exclusão (O que NÃO enviar para a IA)
IGNORE_DIRS = {
    "__pycache__",
    "node_modules",
    "dist",
    "build",
    "data",  # Ignora a pasta de dados (bancos sqlite, parquets)
}

# Diretórios que começam com "." que NÃO devem ser ignorados
ALLOWED_DOT_DIRS = {
    ".github",
}

# Arquivos que começam com "." que NÃO devem ser ignorados
ALLOWED_DOT_FILES = {
    ".gitignore",
    ".env.example",
    ".env",
    ".cursorrules",
    ".dockerignore",
}

IGNORE_FILES = {
    "gerar_contexto.py",  # Não precisa incluir este próprio script
    "poetry.lock",
    "package-lock.json",
    "yarn.lock",
    "pnpm-lock.yaml",
    "uv.lock",
    "contexto_completo.txt",
    "contexto_completo.md",
}

IGNORE_EXTENSIONS = {
    ".pyc",
    ".png",
    ".jpg",
    ".jpeg",
    ".gif",
    ".ico",
    ".svg",
    ".webp",
    ".db",
    ".sqlite",
    ".sqlite3",
    ".parquet",
    ".csv",
    ".xlsx",
    ".pdf",
    ".exe",
    ".bin",
    ".dll",
    ".so",
    ".dylib",
    ".zip",
    ".tar",
    ".gz",
    ".7z",
    ".mp3",
    ".mp4",
    ".wav",
    ".avi",
    ".mkv",
    ".log",
    ".lock",
    ".DS_Store",
}

OUTPUT_FILE = "contexto_completo.md"  # Alterado para .md para melhor formatação
MAX_FILE_SIZE_KB = (
    500  # Ignora arquivos maiores que 500KB para evitar estourar o contexto
)


def should_ignore_path(path: Path) -> bool:
    """Verifica se o arquivo ou diretório deve ser ignorado."""
    # Ignora itens ocultos (começam com .), com exceções específicas
    if path.name.startswith("."):
        if path.is_dir() and path.name not in ALLOWED_DOT_DIRS:
            return True
        if path.is_file() and path.name not in ALLOWED_DOT_FILES:
            return True

    # Ignora pelo nome do diretório ou arquivo exato
    if path.name in IGNORE_DIRS or path.name in IGNORE_FILES:
        return True

    # Ignora pela extensão (apenas para arquivos)
    if path.is_file() and path.suffix.lower() in IGNORE_EXTENSIONS:
        return True

    # Regras de caminho específico (ex: backend/static)
    parts = path.parts
    if "backend" in parts and "static" in parts:
        return True

    return False


def generate_tree(dir_path: Path, prefix: str = "") -> str:
    """Gera a representação em string da árvore de diretórios, aplicando os filtros."""
    tree_str = ""
    try:
        # Pega todos os itens, filtra os ignorados e ordena
        entries = [p for p in dir_path.iterdir() if not should_ignore_path(p)]
        # Ordena pastas primeiro, depois arquivos
        entries.sort(key=lambda p: (not p.is_dir(), p.name.lower()))
    except PermissionError:
        return tree_str

    for i, path in enumerate(entries):
        is_last = i == len(entries) - 1
        connector = "└── " if is_last else "├── "
        tree_str += f"{prefix}{connector}{path.name}\n"

        if path.is_dir():
            extension = "    " if is_last else "│   "
            tree_str += generate_tree(path, prefix + extension)

    return tree_str


def main():
    root_dir = Path(".")

    with open(OUTPUT_FILE, "w", encoding="utf-8") as out_f:
        out_f.write("# CONTEXTO COMPLETO DO PROJETO\n\n")

        # Escreve a estrutura da árvore de pastas antes dos conteúdos
        out_f.write("## ESTRUTURA DE PASTAS:\n```text\n.\n")
        out_f.write(generate_tree(root_dir))
        out_f.write("```\n\n")

        out_f.write("## CONTEÚDO DOS ARQUIVOS:\n\n")

        # Varre os arquivos para extrair o conteúdo
        for path in root_dir.rglob("*"):
            if not path.is_file() or should_ignore_path(path):
                continue

            # Checa se algum dos diretórios pais deve ser ignorado
            if any(
                should_ignore_path(parent)
                for parent in path.parents
                if parent != root_dir
            ):
                continue

            # Verifica o tamanho do arquivo (evita arquivos individuais gigantes)
            try:
                file_size_kb = path.stat().st_size / 1024
                if file_size_kb > MAX_FILE_SIZE_KB:
                    file_path_str = path.as_posix()
                    out_f.write(f"### Arquivo: `{file_path_str}`\n")
                    out_f.write(f"> ⚠️ **Arquivo ignorado:** O tamanho do arquivo ({file_size_kb:.1f} KB) excede o limite configurado de {MAX_FILE_SIZE_KB} KB.\n\n")
                    continue
            except OSError:
                continue

            # Converte as barras para o padrão Unix/URL para facilitar leitura
            file_path_str = path.as_posix()

            # Descobre a linguagem para syntax highlighting
            ext = path.suffix.lower().strip(".")
            lang = ext if ext else "text"

            out_f.write(f"### Arquivo: `{file_path_str}`\n")
            out_f.write(f"```{lang}\n")

            try:
                with open(path, "r", encoding="utf-8") as in_f:
                    content = in_f.read()
                    out_f.write(content)
                    if not content.endswith("\n"):
                        out_f.write("\n")
            except UnicodeDecodeError:
                out_f.write("[Arquivo binário ou codificação não suportada ignorado]\n")
            except Exception as e:
                out_f.write(f"[Erro ao ler arquivo: {e}]\n")

            out_f.write("```\n\n")

    print(f"✅ Arquivo gerado com sucesso: {OUTPUT_FILE}")
    print("Agora basta copiar o conteúdo ou enviar este arquivo para a IA.")


if __name__ == "__main__":
    main()
