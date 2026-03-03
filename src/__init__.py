# src/__init__.py

from .prep import prep
from .score import compute_scores
from .rotas import rotas
from .tarefas import gerar_tarefas
from.osrm_client import build_matrices_with_osrm_or_fallback

__all__ = ["prep", "compute_scores", "rotas", "gerar_tarefas"]