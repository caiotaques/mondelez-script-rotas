Perfeito. Vou te entregar um **README.md profissional**, pensado pra alguém da Mondelez conseguir rodar sem ficar te chamando no Teams a cada 5 minutos 😅


# 📦 Projeto de Geração de Scores e Rotas (PCVRP)

Este projeto realiza:

1. Processamento de dados de PDVs
2. Geração de scores
3. Otimização de rotas por vendedor (PCVRP – Periodic Capacitated Vehicle Routing Problem)
4. Exportação de layout final para integração com sistema MC1
  
  

  
# 🚀 Como Rodar o Projeto

## 1️⃣ Criar ambiente virtual

No diretório raiz do projeto:

```bash
python -m venv .venv
```

### Ativar o ambiente

Windows:

```bash
.venv\Scripts\activate
```

Mac/Linux:

```bash
source .venv/bin/activate
```



## 2️⃣ Instalar dependências
```bash
pip install -r requirements.txt
```



## 3️⃣ Executar o pipeline

No diretório raiz do projeto:

```bash
python run.py
```

Após executar o comando, o sistema solicitará o cenário via terminal.

```code
Escolha o cenário:
1 - Misto
2 - Loja Perfeita
3 - Sellout/Tendência
```

Digite o número do cenário escolhido e aperte enter.

# 📤 Output Gerado

O arquivo final será salvo em:

```
output/<periodo_rotas>/rotas_<periodo_rotas>.csv
```

Exemplo:

```
output/P03/rotas_P03.csv
```

Esse arquivo já está estruturado no layout esperado para integração com MC1.


# ⚙️ Parâmetros do Modelo de Rota

Os principais parâmetros estão definidos em:

```
py_scripts/pcvrp_mod.py
```

Parâmetros padrão:

* Dias por vendedor: 4
* Máximo de visitas por dia: 15
* Máximo de minutos por dia: 420
* Tempo de atendimento por PDV: 45 min

Esses valores podem ser ajustados conforme necessidade operacional.


# 🌍 Cálculo de Distâncias

O sistema utiliza:

* OSRM (Open Source Routing Machine).
* Fallback baseado em distância geográfica caso OSRM não responda


# 🧠 Lógica de Otimização

Para cada vendedor:

1. Filtra apenas PDVs prioritários
2. Calcula centroide como ponto de partida
3. Resolve problema PCVRP usando Google OR-Tools
4. Balanceia carga entre dias
5. Permite descarte de PDVs com penalização baseada no score

---

# 🛠️ Troubleshooting

---

### ❌ Erro ao instalar OR-Tools

Tente:

```bash
pip install --upgrade pip
pip install ortools
```

---

# 🔒 Observações Importantes

* Os nomes de período devem seguir o padrão: `P01`, `P02`, `P03`, etc.
* O sistema assume que o período de rota é o mês seguinte ao período do score.
* Os arquivos de entrada devem conter colunas:

---
