from datetime import date
from pathlib import Path
import pytest
import respx
import httpx
from data_engine.sources.wikipedia_polls import (
    parse_polls_html,
    fetch_polls_html,
    POLLS_URL,
)


def test_parse_returns_one_row_per_poll(fixtures_dir: Path):
    html = (fixtures_dir / "wikipedia_polls_sample.html").read_text(encoding="utf-8")
    df = parse_polls_html(html, geography="GB")
    assert len(df) == 3
    assert set(df["pollster"]) == {"YouGov", "Ipsos", "Survation"}


def test_parse_extracts_published_dates(fixtures_dir: Path):
    html = (fixtures_dir / "wikipedia_polls_sample.html").read_text(encoding="utf-8")
    df = parse_polls_html(html, geography="GB")
    # We use the END date of fieldwork as published_date proxy in the parser
    yougov = df[df["pollster"] == "YouGov"].iloc[0]
    assert yougov["fieldwork_start"] == "2026-04-18"
    assert yougov["fieldwork_end"] == "2026-04-20"


def test_parse_party_shares(fixtures_dir: Path):
    html = (fixtures_dir / "wikipedia_polls_sample.html").read_text(encoding="utf-8")
    df = parse_polls_html(html, geography="GB")
    yougov = df[df["pollster"] == "YouGov"].iloc[0]
    assert yougov["lab"] == 28.0
    assert yougov["con"] == 22.0
    assert yougov["reform"] == 24.0


def test_parse_geography_column_set(fixtures_dir: Path):
    html = (fixtures_dir / "wikipedia_polls_sample.html").read_text(encoding="utf-8")
    df = parse_polls_html(html, geography="Wales")
    assert (df["geography"] == "Wales").all()


@respx.mock
def test_fetch_uses_user_agent_and_returns_text():
    route = respx.get(POLLS_URL).mock(
        return_value=httpx.Response(200, text="<html>ok</html>")
    )
    text = fetch_polls_html(POLLS_URL)
    assert text == "<html>ok</html>"
    assert route.called
    sent = route.calls[0].request
    assert "User-Agent" in sent.headers
    assert "seatpredictor" in sent.headers["User-Agent"]
