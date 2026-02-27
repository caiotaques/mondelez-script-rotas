# run_all.py
# -----------------------------------------------------------------------------
# Runner único: executa 01_prep -> 02_score -> 03_pcvrp -> 04_tasks
# -----------------------------------------------------------------------------

from __future__ import annotations

from importlib import import_module
import subprocess
import sys
from pathlib import Path


def run_step(py: str, name: str) -> None:
    print(f"\n==================== {name} ====================")
    print(f"Rodando: {py}")

    # Usa o mesmo interpretador Python que está executando o runner
    result = subprocess.run(
        [sys.executable, py],
        check=False,
        capture_output=True,
        text=True,
    )

    # imprime logs do script
    if result.stdout:
        print(result.stdout)
    if result.stderr:
        print(result.stderr)

    if result.returncode != 0:
        raise RuntimeError(f"Falhou em {name} (returncode={result.returncode}).")


def main():
    root = Path(__file__).resolve().parent

    # Ajuste aqui se seus arquivos estiverem em outro lugar/nome
    steps = [
        ("01_prep.py", "01_PREP (preparação dos dados)"),
        ("02_score.py", "02_SCORE (scoring + prioritários)"),
        ("03_pcvrp.py", "03_PCVRP (roteirização)"),
        ("04_tasks.py", "04_TASKS (tarefas/missões)"),
    ]

    # valida existência
    for py, _ in steps:
        if not (root / py).exists():
            raise FileNotFoundError(f"Não encontrei {py} na raiz do projeto: {root}")

    # roda em sequência
    for py, name in steps:
        run_step(str(root / py), name)

    # importa período de rotas do módulo 02_score
    sys.path.insert(0, '.')
    spec = import_module('02_score')
    periodo_rotas = spec.periodo_rotas

    print("\n✅ Pipeline completo finalizado.")
    print(f"Confira a pasta output/{periodo_rotas}/ para: pdvs_vendedores_*, scores_*, rotas_* e tarefas_*.")


if __name__ == "__main__":
    main()
