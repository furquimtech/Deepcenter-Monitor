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
    MAX(dtdatainsercao) AS ultima_atualizacao,
    EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - MAX(dtdatainsercao))) / 60.0 AS minutos_sem_atualizacao
FROM esdeepcenter_acionamento_bradesco
GROUP BY carteira_int
ORDER BY minutos_sem_atualizacao DESC, carteira_int;
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


def build_legend(rules: list[Rule], refresh_seconds: int) -> str:
    parts: list[str] = []
    for rule in rules:
        if rule.max_minutes is None:
            parts.append(f"[{rule.color}]o {rule.label}[/{rule.color}] (> ultimo limite)")
        else:
            parts.append(f"[{rule.color}]o {rule.label}[/{rule.color}] (<= {rule.max_minutes:.1f} min)")
    trend_legend = "Tendencia: [red]^[/red] aumento >10% | - normal | [green]v[/green] queda >10%"
    return " | ".join(parts) + f" | Atualizacao: {refresh_seconds}s | {trend_legend}"


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


def build_table(
    rows: list[dict[str, Any]],
    rules: list[Rule],
    checked_at: datetime,
    previous_minutes_by_wallet: dict[str, float],
) -> Group:
    table = Table(title="Monitor de Carteiras", expand=True)
    table.add_column("Carteira", no_wrap=True)
    table.add_column("Status", no_wrap=True)
    table.add_column("Tendencia", justify="center", no_wrap=True)
    table.add_column("Min sem atualizacao", justify="right", no_wrap=True)
    table.add_column("Ultima atualizacao", no_wrap=True)

    count_by_status: dict[str, int] = {}
    for row in rows:
        minutos = float(row.get("minutos_sem_atualizacao") or 0)
        rule = pick_rule(minutos, rules)
        is_critical = rule is rules[-1]
        carteira = str(row.get("carteira_int", "-"))
        ultima = str(row.get("ultima_atualizacao", "-"))
        trend = trend_cell(minutos, previous_minutes_by_wallet.get(carteira))
        previous_minutes_by_wallet[carteira] = minutos

        if is_critical:
            status_cell = f"[blink {rule.color}]o {rule.label}[/]"
        else:
            status_cell = f"[{rule.color}]o {rule.label}[/{rule.color}]"

        table.add_row(
            carteira,
            status_cell,
            trend,
            f"{minutos:.1f}",
            ultima,
        )
        count_by_status[rule.label] = count_by_status.get(rule.label, 0) + 1

    summary = ", ".join([f"{k}: {v}" for k, v in count_by_status.items()]) or "Sem dados"
    header = Panel(f"Ultima verificacao: {checked_at:%Y-%m-%d %H:%M:%S} | {summary}", expand=True)
    return Group(header, table)


def main() -> None:
    cfg = load_config()
    rules = load_rules(cfg)
    refresh_seconds = int(cfg.get("refresh_seconds", 30))

    dsn = build_dsn()
    engine = build_engine(dsn)
    console = Console()
    legend = Panel(build_legend(rules, refresh_seconds), title="Semaforo", expand=True)
    previous_minutes_by_wallet: dict[str, float] = {}

    with Live(Group(legend, Panel("Iniciando monitor...")), console=console, screen=True, auto_refresh=False) as live:
        while True:
            checked_at = datetime.now()
            try:
                rows = fetch_data(engine)
                if rows:
                    live.update(
                        Group(legend, build_table(rows, rules, checked_at, previous_minutes_by_wallet)),
                        refresh=True,
                    )
                else:
                    live.update(
                        Group(
                            legend,
                            Panel(f"Ultima verificacao: {checked_at:%Y-%m-%d %H:%M:%S}\nNenhum registro retornado."),
                        ),
                        refresh=True,
                    )
            except Exception as exc:
                live.update(
                    Group(
                        legend,
                        Panel(f"Falha ao consultar o banco: {exc}", title="Erro"),
                    ),
                    refresh=True,
                )
            time.sleep(refresh_seconds)


if __name__ == "__main__":
    main()
