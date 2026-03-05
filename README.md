# Deepcenter Monitor (Terminal)

Monitor de carteiras no proprio terminal, em uma unica tela, com semaforo por `carteira_int`.

## O que faz

- Executa a consulta por `carteira_int`
- Calcula `minutos_ultima_insercao` e `minutos_ultimo_dado`
- Exibe dois farois por carteira (insercao e ultimo dado) com verde/amarelo/vermelho
- Exibe tendencia por carteira para cada metrica comparando com a leitura anterior
- Atualiza a tela automaticamente sem abrir browser
- Mantem tudo em uma unica tela com refresh continuo

## Como executar

1. Criar ambiente virtual e instalar dependencias:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

2. Criar `.env` com base no exemplo:

```powershell
Copy-Item .env.example .env
```

3. Ajustar credenciais no `.env`.

4. Executar o monitor no terminal:

```powershell
python app.py
```

## Parametrizacao

- `config.yaml`
  - `refresh_seconds`: intervalo de atualizacao da tela
  - `rules`: limites de minutos e cores do semaforo
- Tendencia (colunas `Tend. Ins` e `Tend. Dado`):
  - `^` vermelho: aumento maior que 10% em relacao ao ciclo anterior
  - `-`: variacao dentro da margem de 10%
  - `v` verde: queda maior que 10% em relacao ao ciclo anterior

Exemplo de regras:

```yaml
rules:
  - label: Verde
    max_minutes: 5
    color: "green"
  - label: Amarelo
    max_minutes: 10
    color: "yellow"
  - label: Vermelho
    max_minutes: null
    color: "red"
```

Para sair do monitor: `Ctrl + C`.
