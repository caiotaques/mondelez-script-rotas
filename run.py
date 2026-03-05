# run_all.py
# -----------------------------------------------------------------------------
# Runner único: executa 01_prep -> 02_score -> 03_pcvrp -> 04_tasks
# -----------------------------------------------------------------------------
from src import prep, gerar_scores, gerar_rotas, gerar_tarefas

def main():
    print('\n\n\n\n\n=============================== GERADOR DE TAREFAS E MISSÕES - MONDELEZ ===============================\n')
    # Configurações iniciais
    cenario = input("Escolha o cenário (digite o número):\n1 - Misto\n2 - Loja Perfeita\n3 - Sellout/Tendência \n").strip()
    map_cenarios = {'1': 'Misto', '2': 'Loja Perfeita', '3': 'Sellout/Tendência'}

    if cenario not in map_cenarios:
        print(f"Cenário '{cenario}' inválido, usando misto como padrão.")
        cenario = '1'  # default para misto
    print(f"Cenário selecionado: {map_cenarios.get(cenario, 'Misto (padrão)')}")

    pular_prep = input("Deseja pular a etapa de preparação dos dados? (s/n) ").strip().lower() == 's'
    try:
        if not pular_prep:
            periodo_pesquisa, periodo_rotas = prep()
        else:
            periodo_pesquisa = input("Digite o período de pesquisa (pXX): ").strip().upper()
            periodo_rotas = input("Digite o período de rotas (pXX): ").strip().upper()
        gerar_scores(periodo_pesquisa, periodo_rotas, cenario=cenario)
        gerar_rotas(periodo_pesquisa, periodo_rotas)
        gerar_tarefas(periodo_pesquisa, periodo_rotas)
    except Exception as e:
        print(f"\n❌ Erro durante a execução do pipeline: {e}")
    
    print("\n✅ Pipeline completo finalizado.")
    print(f"Confira a pasta data/output/{periodo_rotas}/ para: pdvs_vendedores_*, scores_*, rotas_* e tarefas_*.")


if __name__ == "__main__":
    main()
