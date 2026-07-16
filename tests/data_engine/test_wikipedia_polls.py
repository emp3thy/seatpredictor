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


def test_rb_column_parses_as_restore():
    """Tables with a Restore Britain (RB) column must not lose those shares —
    RB maps to its own 'restore' column and the row passes the plausible-sum
    filter (pre-fix the 6pp went missing and the row was dropped)."""
    html = '''
    <html><body>
    <table class="wikitable">
    <tr><th>Pollster</th><th>Date</th><th>Sample size</th>
        <th>Lab</th><th>Con</th><th>Ref</th><th>LD</th><th>Grn</th><th>SNP</th><th>PC</th><th>RB</th><th>Others</th></tr>
    <tr><td>RestoreEraPoll</td><td>1 Jul 2026</td><td>1000</td>
        <td>20</td><td>19</td><td>24</td><td>13</td><td>14</td><td>2</td><td>1</td><td>6</td><td>1</td></tr>
    </table>
    </body></html>
    '''
    df = parse_polls_html(html, geography="GB")
    assert set(df["pollster"]) == {"RestoreEraPoll"}
    row = df.iloc[0]
    assert row["reform"] == 24.0
    assert row["restore"] == 6.0
    assert row["other"] == 1.0


def test_dash_cell_is_missing_not_zero():
    """A pollster that doesn't prompt for a party shows an em-dash — that must
    parse as NaN (not measured), not 0.0, so window means over pollsters that DO
    measure it aren't dragged down."""
    import math
    html = '''
    <html><body>
    <table class="wikitable">
    <tr><th>Pollster</th><th>Date</th><th>Sample size</th>
        <th>Lab</th><th>Con</th><th>Ref</th><th>LD</th><th>Grn</th><th>SNP</th><th>PC</th><th>RB</th><th>Others</th></tr>
    <tr><td>MeasuresRB</td><td>1 Jul 2026</td><td>1000</td>
        <td>20</td><td>19</td><td>24</td><td>13</td><td>14</td><td>2</td><td>1</td><td>4</td><td>3</td></tr>
    <tr><td>NoRBPrompt</td><td>2 Jul 2026</td><td>1000</td>
        <td>21</td><td>20</td><td>25</td><td>13</td><td>14</td><td>2</td><td>1</td><td>—</td><td>4</td></tr>
    </table>
    </body></html>
    '''
    df = parse_polls_html(html, geography="GB")
    assert set(df["pollster"]) == {"MeasuresRB", "NoRBPrompt"}
    measured = df[df["pollster"] == "MeasuresRB"].iloc[0]
    missing = df[df["pollster"] == "NoRBPrompt"].iloc[0]
    assert measured["restore"] == 4.0
    assert math.isnan(missing["restore"])
    assert df["restore"].mean() == 4.0  # NaN skipped


def test_multiple_columns_mapping_to_same_slot_accumulate():
    """Two headers that both fold into 'other' (e.g. YP + Others) must sum,
    not overwrite each other."""
    html = '''
    <html><body>
    <table class="wikitable">
    <tr><th>Pollster</th><th>Date</th><th>Sample size</th>
        <th>Lab</th><th>Con</th><th>Ref</th><th>LD</th><th>Grn</th><th>SNP</th><th>PC</th><th>YP</th><th>Others</th></tr>
    <tr><td>AccumPoll</td><td>1 Jul 2026</td><td>1000</td>
        <td>20</td><td>19</td><td>24</td><td>13</td><td>14</td><td>2</td><td>1</td><td>4</td><td>3</td></tr>
    </table>
    </body></html>
    '''
    df = parse_polls_html(html, geography="GB")
    assert set(df["pollster"]) == {"AccumPoll"}
    assert df.iloc[0]["other"] == 7.0  # YP 4 + Others 3


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
