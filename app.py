from __future__ import annotations

import os
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Any

import psycopg2
import yaml
from dotenv import load_dotenv
from rich.console import Console, Group
from rich.live import Live
from rich.panel import Panel
from rich.table import Table
from sqlalchemy import create_engine, text


QUERY = """
SELECT
    carteira_int,
    MAX(dtdatainsercao) AS ultima_insercao,
    EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - MAX(dtdatainsercao))) / 60.0 AS minutos_ultima_insercao,
    MAX(hrhorainicio) AS hora_ultimo_dado,
    EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP::time - MAX(hrhorainicio))) / 60.0 AS minutos_ultimo_dado
FROM esdeepcenter_acionamento_bradesco
WHERE dtdatareferencia = CURRENT_DATE
GROUP BY carteira_int
ORDER BY minutos_ultimo_dado DESC, carteira_int;
"""


@dataclass
class Rule:
    label: str
    max_minutes: float | None
    color: str


def load_config(path: str = "config.yaml") -> dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def build_dsn() -> str:
    load_dotenv()
    dsn = os.getenv("DB_DSN")
    if dsn:
        return dsn

    host = os.getenv("DB_HOST", "localhost")
    port = os.getenv("DB_PORT", "5432")
    dbname = os.getenv("DB_NAME", "")
    user = os.getenv("DB_USER", "")
    password = os.getenv("DB_PASSWORD", "")
    sslmode = os.getenv("DB_SSLMODE", "prefer")
    if not password:
        raise ValueError("Defina DB_PASSWORD no arquivo .env ou na variavel de ambiente.")
    return (
        f"host={host} port={port} dbname={dbname} user={user} "
        f"password={password} sslmode={sslmode}"
    )


def build_engine(dsn: str):
    if dsn.lower().startswith("postgresql://") or dsn.lower().startswith("postgresql+psycopg2://"):
        return create_engine(dsn)
    return create_engine(
        "postgresql+psycopg2://",
        creator=lambda: psycopg2.connect(dsn),
    )


def fetch_data(engine) -> list[dict[str, Any]]:
    with engine.connect() as conn:
        rows = conn.execute(text(QUERY)).mappings().all()
    return [dict(row) for row in rows]


def pick_rule(minutes: float, rules: list[Rule]) -> Rule:
    for rule in rules:
        if rule.max_minutes is None:
            return rule
        if minutes <= rule.max_minutes:
            return rule
    return rules[-1]


def load_rules(cfg: dict[str, Any]) -> list[Rule]:
    rules: list[Rule] = []
    for r in cfg.get("rules", []):
        rules.append(
            Rule(
                label=str(r["label"]),
                max_minutes=r.get("max_minutes"),
                color=str(r["color"]),
            )
        )
    if not rules:
        raise ValueError("Nenhuma regra de semaforo foi configurada em config.yaml")
    return rules


def build_legend(rules: list[Rule], refresh_seconds: int, remaining_seconds: int) -> str:
    parts: list[str] = []
    for rule in rules:
        if rule.max_minutes is None:
            parts.append(f"[{rule.color}]o {rule.label}[/{rule.color}] (> ultimo limite)")
        else:
            parts.append(f"[{rule.color}]o {rule.label}[/{rule.color}] (<= {rule.max_minutes:.1f} min)")
    trend_legend = "Tendencia: [red]^[/red] aumento >10% | - normal | [green]v[/green] queda >10%"
    return (
        " | ".join(parts)
        + f" | Atualizacao: {refresh_seconds}s"
        + f" | Proxima em: {remaining_seconds}s"
        + " | Farois: Insercao e Ultimo Dado"
        + f" | {trend_legend}"
    )


def trend_cell(current: float, previous: float | None, margin_pct: float = 0.10) -> str:
    if previous is None:
        return "-"

    diff = current - previous
    threshold = abs(previous) * margin_pct
    if abs(diff) <= threshold:
        return "-"
    if diff > 0:
        return "[red]^[/red]"
    return "[green]v[/green]"


def format_time_only(value: Any) -> str:
    if value is None:
        return "-"
    if hasattr(value, "strftime"):
        return value.strftime("%H:%M:%S")
    text = str(value)
    if " " in text:
        return text.split(" ")[-1].split(".")[0]
    return text.split(".")[0]


def status_cell(minutes: float, rules: list[Rule]) -> tuple[str, Rule]:
    rule = pick_rule(minutes, rules)
    return f"[{rule.color}]o {rule.label}[/{rule.color}]", rule


def build_table(
    rows: list[dict[str, Any]],
    rules: list[Rule],
    checked_at: datetime,
    prev_insert_by_wallet: dict[str, float],
    prev_data_by_wallet: dict[str, float],
) -> Group:
    table = Table(title="Monitor de Carteiras", expand=True)
    table.add_column("Carteira", no_wrap=True)
    table.add_column("Farol Insercao", no_wrap=True)
    table.add_column("Tend. Ins", justify="center", no_wrap=True)
    table.add_column("Min Ins", justify="right", no_wrap=True)
    table.add_column("Ultima Insercao", no_wrap=True)
    table.add_column("Farol Ult. Dado", no_wrap=True)
    table.add_column("Tend. Dado", justify="center", no_wrap=True)
    table.add_column("Min Dado", justify="right", no_wrap=True)
    table.add_column("Hora Ult. Dado", no_wrap=True)

    critical_insert = 0
    critical_data = 0
    for row in rows:
        carteira = str(row.get("carteira_int", "-"))
        ultima = format_time_only(row.get("ultima_insercao"))
        hora_ultimo_dado = format_time_only(row.get("hora_ultimo_dado"))

        min_insert = float(row.get("minutos_ultima_insercao") or 0)
        min_data = float(row.get("minutos_ultimo_dado") or 0)

        farol_insert, rule_insert = status_cell(min_insert, rules)
        farol_data, rule_data = status_cell(min_data, rules)
        trend_insert = trend_cell(min_insert, prev_insert_by_wallet.get(carteira))
        trend_data = trend_cell(min_data, prev_data_by_wallet.get(carteira))

        prev_insert_by_wallet[carteira] = min_insert
        prev_data_by_wallet[carteira] = min_data
        if rule_insert is rules[-1]:
            critical_insert += 1
        if rule_data is rules[-1]:
            critical_data += 1

        table.add_row(
            carteira,
            farol_insert,
            trend_insert,
            f"{min_insert:.1f}",
            ultima,
            farol_data,
            trend_data,
            f"{min_data:.1f}",
            hora_ultimo_dado,
        )

    summary = f"Critico Insercao: {critical_insert} | Critico Ultimo Dado: {critical_data}"
    header = Panel(f"Ultima verificacao: {checked_at:%Y-%m-%d %H:%M:%S} | {summary}", expand=True)
    return Group(header, table)


def main() -> None:
    cfg = load_config()
    rules = load_rules(cfg)
    refresh_seconds = int(cfg.get("refresh_seconds", 30))

    dsn = build_dsn()
    engine = build_engine(dsn)
    console = Console()
    prev_insert_by_wallet: dict[str, float] = {}
    prev_data_by_wallet: dict[str, float] = {}

    initial_legend = Panel(build_legend(rules, refresh_seconds, refresh_seconds), title="Semaforo", expand=True)
    with Live(Group(initial_legend, Panel("Iniciando monitor...")), console=console, screen=True, auto_refresh=False) as live:
        while True:
            checked_at = datetime.now()
            try:
                rows = fetch_data(engine)
                if rows:
                    content = build_table(rows, rules, checked_at, prev_insert_by_wallet, prev_data_by_wallet)
                else:
                    content = Panel(
                        f"Ultima verificacao: {checked_at:%Y-%m-%d %H:%M:%S}\nNenhum registro retornado."
                    )
            except Exception as exc:
                content = Panel(f"Falha ao consultar o banco: {exc}", title="Erro")

            for remaining in range(refresh_seconds, 0, -1):
                legend = Panel(
                    build_legend(rules, refresh_seconds, remaining),
                    title="Semaforo",
                    expand=True,
                )
                live.update(Group(legend, content), refresh=True)
                time.sleep(1)


if __name__ == "__main__":
    main()
