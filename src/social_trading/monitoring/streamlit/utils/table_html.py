"""
table_html.py — render a pandas DataFrame as a styled HTML table with
per-cell tooltips on specified columns.

Streamlit's st.dataframe does not support per-cell hover tooltips, so this
utility falls back to st.markdown(unsafe_allow_html=True) with <td title="…">
attributes to get native browser tooltip behaviour.

Usage:
    from social_trading.monitoring.streamlit.utils.table_html import render_table

    render_table(
        df,
        tooltips={"ticker": ticker_to_company_tooltip_dict},
        link_cols={"chart": ("📈", "_blank")},   # col → (display_text, target)
        hide_cols=["_internal"],
    )
"""
from __future__ import annotations

import html
import streamlit as st
import pandas as pd


# ── CSS (theme-neutral) ───────────────────────────────────────────────────────
_CSS = """
<style>
.ht-table {
    width: 100%;
    border-collapse: collapse;
    font-size: 0.85rem;
    font-family: "Source Sans Pro", sans-serif;
}
.ht-table th {
    font-weight: 600;
    text-align: left;
    padding: 7px 10px;
    border-bottom: 2px solid rgba(128,128,128,0.3);
    white-space: nowrap;
    opacity: 0.75;
}
.ht-table td {
    padding: 6px 10px;
    border-bottom: 1px solid rgba(128,128,128,0.15);
    white-space: nowrap;
    vertical-align: middle;
}
.ht-table tr:hover td {
    background: rgba(128,128,128,0.1);
}
.ht-table td[title] {
    cursor: help;
    text-decoration: underline dotted rgba(128,128,128,0.5);
}
.ht-table a {
    color: #4A90D9;
    text-decoration: none;
    font-weight: bold;
}
.ht-table a:hover {
    text-decoration: underline;
}
</style>
"""


def render_table(
    df: pd.DataFrame,
    *,
    tooltips: dict[str, dict[str, str]] | None = None,
    link_cols: dict[str, tuple[str, str]] | None = None,
    cell_styles: dict[str, dict[str, str]] | None = None,
    hide_cols: list[str] | None = None,
    max_rows: int = 500,
    key: str = "ht",
) -> None:
    """
    Render *df* as a dark-themed HTML table via st.markdown.

    Args:
        df:           DataFrame to render.
        tooltips:     Mapping of column_name → {row_value → tooltip_string}.
                      The tooltip is attached to the cell as a ``title`` attribute
                      (shown as a native browser tooltip on hover).
        link_cols:    Mapping of column_name → (display_text, target).
                      The cell value is used as the href; display_text is shown.
        cell_styles:  Mapping of column_name → {cell_value → css_style_string}.
                      Applies inline style to matching cells.
                      E.g. ``{"phase": {"phase1": "color:#4A90D9;font-weight:bold"}}``.
        hide_cols:    Column names to omit from output.
        max_rows:     Silently cap rows to avoid huge HTML blobs.
        key:          Unused; kept for API consistency.
    """
    if df.empty:
        st.info("No data")
        return

    tooltips    = tooltips    or {}
    link_cols   = link_cols   or {}
    cell_styles = cell_styles or {}
    hide_cols   = set(hide_cols or [])

    display_cols = [c for c in df.columns if c not in hide_cols]
    rows = df.head(max_rows)

    lines: list[str] = [_CSS, '<table class="ht-table"><thead><tr>']
    for col in display_cols:
        lines.append(f"<th>{html.escape(str(col))}</th>")
    lines.append("</tr></thead><tbody>")

    for _, row in rows.iterrows():
        lines.append("<tr>")
        for col in display_cols:
            val = row[col]

            # Boolean columns → subtle filled/empty dot
            if isinstance(val, (bool,)) or (hasattr(val, 'item') and pd.api.types.is_bool_dtype(type(val))):
                if val:
                    icon = "<span style='color:#4A90D9;font-weight:bold'>✓</span>"
                else:
                    icon = "<span style='opacity:0.3'>✗</span>"
                lines.append(f"<td style='text-align:center'>{icon}</td>")
                continue

            val_str = "" if pd.isna(val) else str(val)

            if col in link_cols:
                display_text, target = link_cols[col]
                cell = (
                    f'<a href="{html.escape(val_str)}" target="{target}">'
                    f"{display_text}</a>"
                )
                lines.append(f"<td>{cell}</td>")
            else:
                style  = cell_styles.get(col, {}).get(val_str, "")
                tip    = tooltips.get(col, {}).get(val_str, "")
                bold   = col in tooltips
                inner  = f"<b>{html.escape(val_str)}</b>" if bold else html.escape(val_str)
                attrs  = ""
                if style:
                    attrs += f' style="{html.escape(style)}"'
                if tip:
                    attrs += f' title="{html.escape(tip)}"'
                lines.append(f"<td{attrs}>{inner}</td>")
        lines.append("</tr>")

    lines.append("</tbody></table>")
    st.markdown("\n".join(lines), unsafe_allow_html=True)
