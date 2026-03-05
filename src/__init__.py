# src/__init__.py

from .prep import prep
from .score import gerar_scores
from .rotas import gerar_rotas
from .tarefas import gerar_tarefas
from.osrm_client import build_matrices_with_osrm_or_fallback

__all__ = ["prep", "gerar_scores", "gerar_rotas", "gerar_tarefas"]