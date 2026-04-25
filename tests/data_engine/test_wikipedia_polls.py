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


def test_implausible_share_sum_rows_are_filtered():
    """A row whose party shares sum to ~600 (seat-projection table) must be dropped."""
    html = '''
    <html><body>
    <table class="wikitable">
    <tr><th>Pollster</th><th>Date</th><th>Sample size</th>
        <th>Lab</th><th>Con</th><th>Reform</th><th>LD</th><th>Grn</th><th>SNP</th><th>PC</th><th>Others</th></tr>
    <tr><td>NormalPoll</td><td>1 Apr 2026</td><td>1000</td>
        <td>30</td><td>22</td><td>24</td><td>11</td><td>8</td><td>3</td><td>1</td><td>1</td></tr>
    <tr><td>SeatProjMRP</td><td>1 Apr 2026</td><td>1000</td>
        <td>250</td><td>120</td><td>200</td><td>40</td><td>20</td><td>10</td><td>5</td><td>5</td></tr>
    </table>
    </body></html>
    '''
    df = parse_polls_html(html, geography="GB")
    assert set(df["pollster"]) == {"NormalPoll"}  # SeatProjMRP filtered out
