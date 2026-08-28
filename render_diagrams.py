"""Render PizzaFlow Session 1 entity and architecture diagrams with Graphviz.

Beautified version: proper ER-style table nodes for the entity model,
clustered/gradient-filled stages for the architecture diagram, a legend,
and a consistent modern color system.
"""

import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
ARCH = ROOT / "architecture"
DOCS = ROOT / "docs"

ARCH.mkdir(parents=True, exist_ok=True)
DOCS.mkdir(parents=True, exist_ok=True)

# ---------------------------------------------------------------------------
# Color system
# ---------------------------------------------------------------------------
INK = "#1F2A44"          # near-black text
NAVY = "#1E3A5F"          # header bars / strong borders
BLUE = "#2E74B5"          # primary accent
BLUE_LIGHT = "#EAF2FA"    # card fill (light)
BLUE_MID = "#CFE2F3"      # card fill (gradient stop)
TEAL = "#0F9B8E"          # secondary accent (processing)
TEAL_LIGHT = "#E4F7F5"
TEAL_MID = "#BFEDE8"
AMBER = "#C9891A"         # ingestion / prep accent
AMBER_LIGHT = "#FFF3E0"
AMBER_MID = "#FBE3B8"
CORAL = "#C1441E"         # validation accent
CORAL_LIGHT = "#FDEBE4"
CORAL_MID = "#F7C9B8"
SLATE = "#8A94A6"         # muted lines / dotted edges
CANVAS = "#FBFCFE"        # page background

FONT = "Helvetica"
FONT_BOLD = "Helvetica-Bold"

# ---------------------------------------------------------------------------
# Entity model — classic ER "table card" look via HTML-like labels
# ---------------------------------------------------------------------------


def er_table(title: str, subtitle: str, rows: list[tuple[str, str, str]], header_color: str) -> str:
    """Build an HTML-like label that renders as a database table card.

    rows: list of (badge, field_name, field_note) tuples.
    badge is 'PK', 'FK', or '' — rendered as a small colored chip.
    """
    badge_colors = {"PK": NAVY, "FK": TEAL, "": "#FFFFFF"}
    row_html = []
    for badge, field, note in rows:
        chip_color = badge_colors.get(badge, "#FFFFFF")
        chip = (
            f'<td width="28" bgcolor="{chip_color}">'
            f'<font color="white" point-size="8" face="{FONT_BOLD}">{badge}</font></td>'
            if badge
            else '<td width="28"></td>'
        )
        row_html.append(
            f'<tr>{chip}'
            f'<td align="left"><font face="{FONT}" point-size="11" color="{INK}">{field}</font></td>'
            f'<td align="left"><font face="{FONT}" point-size="9" color="{SLATE}">{note}</font></td>'
            f'</tr>'
        )

    rows_joined = "".join(row_html)
    return f"""<
    <table border="1" cellborder="0" cellspacing="0" cellpadding="6"
           bgcolor="white" color="#D7DEE8">
        <tr>
            <td colspan="3" bgcolor="{header_color}" align="left" cellpadding="8">
                <font face="{FONT_BOLD}" point-size="13" color="white">{title}</font><br/>
                <font face="{FONT}" point-size="9" color="white">{subtitle}</font>
            </td>
        </tr>
        {rows_joined}
    </table>>"""


ENTITY = f"""
digraph EntityModel {{
    rankdir=LR;
    bgcolor="{CANVAS}";
    splines=spline;
    nodesep=0.9;
    ranksep=1.1;
    fontname="{FONT}";
    labelloc="t";
    fontsize=20;
    pad="0.4";

    label=<
        <font face="{FONT_BOLD}" point-size="20" color="{INK}">PizzaFlow — Session 1 Entity Model</font><br/>
        <font face="{FONT}" point-size="11" color="{SLATE}">Order-driven pizzeria retail revenue intelligence</font>
    >;

    node [shape=plain fontname="{FONT}"];

    edge [
        color="{SLATE}"
        fontname="{FONT}"
        fontsize=9
        fontcolor="{INK}"
        penwidth=1.4
        arrowsize=0.8
        arrowhead=vee
    ];

    orders [label={er_table(
        "Orders", "21,350 rows",
        [("PK", "order_id", "identifier"),
         ("", "date", "order date"),
         ("", "time", "order time")],
        NAVY,
    )}];

    details [label={er_table(
        "Order Details", "48,620 rows",
        [("PK", "order_details_id", "identifier"),
         ("FK", "order_id", "→ orders"),
         ("FK", "pizza_id", "→ pizzas"),
         ("", "quantity", "units on line")],
        BLUE,
    )}];

    pizzas [label={er_table(
        "Pizzas", "96 rows",
        [("PK", "pizza_id", "identifier"),
         ("FK", "pizza_type_id", "→ pizza_types"),
         ("", "size", "S · M · L · XL"),
         ("", "price", "unit price, USD")],
        TEAL,
    )}];

    types [label={er_table(
        "Pizza Types", "32 rows",
        [("PK", "pizza_type_id", "identifier"),
         ("", "name", "menu name"),
         ("", "category", "classic/chicken/…"),
         ("", "ingredients", "comma-delimited")],
        AMBER,
    )}];

    orders -> details [label="  1 : many  " color="{NAVY}" fontcolor="{NAVY}"];
    pizzas -> details [label="  1 : many  " color="{TEAL}" fontcolor="{TEAL}"];
    types -> pizzas   [label="  1 : many  " color="{AMBER}" fontcolor="{AMBER}"];
}}
"""

# ---------------------------------------------------------------------------
# Architecture — clustered pipeline stages with gradient cards
# ---------------------------------------------------------------------------


def card(label_lines: list[str], fill_a: str, fill_b: str, border: str, icon: str = "") -> str:
    title = label_lines[0]
    rest = "\\n".join(label_lines[1:])
    icon_part = f"{icon}  " if icon else ""
    body = f"{icon_part}{title}"
    if rest:
        body += f"\\n{rest}"
    return body, fill_a, fill_b, border


ARCHITECTURE = f"""
digraph PizzaFlowArchitecture {{
    rankdir=TB;
    bgcolor="{CANVAS}";
    splines=spline;
    nodesep=0.6;
    ranksep=0.8;
    fontname="{FONT}";
    labelloc="t";
    fontsize=20;
    pad="0.4";

    label=<
        <font face="{FONT_BOLD}" point-size="20" color="{INK}">PizzaFlow — Session 1 Parallel-Compute Architecture</font><br/>
        <font face="{FONT}" point-size="11" color="{SLATE}">Pandas reference vs. PySpark bounded parallelism</font>
    >;

    node [
        shape=box
        style="rounded,filled"
        fontname="{FONT}"
        fontsize=10
        fontcolor="{INK}"
        penwidth=1.2
        margin="0.22,0.14"
    ];

    edge [
        color="{SLATE}"
        penwidth=1.6
        fontname="{FONT}"
        fontsize=9
        fontcolor="{INK}"
        arrowsize=0.85
        arrowhead=vee
    ];

    source [
        label="📥  Source Dataset\\norders.csv · order_details.csv\\npizzas.csv · pizza_types.csv"
        fillcolor="{BLUE_LIGHT}:{BLUE_MID}" gradientangle=90
        color="{BLUE}"
    ];

    subgraph cluster_prep {{
        label=<<font face="{FONT_BOLD}" point-size="10" color="{AMBER}">PREP</font>>;
        style="rounded,dashed";
        color="{AMBER}";
        bgcolor="{AMBER_LIGHT}";
        margin=16;

        profile [
            label="🔍  profile_files.py\\nSchema + PK/FK + relationship checks"
            fillcolor="{AMBER_LIGHT}:{AMBER_MID}" gradientangle=90
            color="{AMBER}"
        ];

        join [
            label="🔗  load_and_join.py\\norders + order_details + pizzas + pizza_types\\n48,620 order-detail rows"
            fillcolor="{AMBER_LIGHT}:{AMBER_MID}" gradientangle=90
            color="{AMBER}"
        ];

        profile -> join [label="valid" color="{AMBER}" fontcolor="{AMBER}"];
    }}

    subgraph cluster_compute {{
        label=<<font face="{FONT_BOLD}" point-size="10" color="{TEAL}">COMPUTE</font>>;
        style="rounded,dashed";
        color="{TEAL}";
        bgcolor="{TEAL_LIGHT}";
        margin=16;

        baseline [
            label="🧮  sequential_baseline.py\\nPandas reference\\ngroupBy category"
            fillcolor="{CORAL_LIGHT}:{CORAL_MID}" gradientangle=90
            color="{CORAL}"
        ];

        repart [
            label="⚙️  parallel_compute.py\\nrepartition(4, category)"
            fillcolor="{TEAL_LIGHT}:{TEAL_MID}" gradientangle=90
            color="{TEAL}"
        ];

        agg [
            label="📊  PySpark aggregation\\norder count · units · revenue\\nmean line revenue"
            fillcolor="{TEAL_LIGHT}:{TEAL_MID}" gradientangle=90
            color="{TEAL}"
        ];

        repart -> agg;
    }}

    subgraph cluster_validate {{
        label=<<font face="{FONT_BOLD}" point-size="10" color="{CORAL}">VALIDATE</font>>;
        style="rounded,dashed";
        color="{CORAL}";
        bgcolor="{CORAL_LIGHT}";
        margin=16;

        validate [
            label="✅  Validation\\nparallel == baseline\\ntolerance ≤ 1e-6"
            fillcolor="{CORAL_LIGHT}:{CORAL_MID}" gradientangle=90
            color="{CORAL}"
        ];
    }}

    output [
        label="📦  results/category_revenue.parquet\\n4 categories\\nresults/session1_benchmark.csv"
        fillcolor="{BLUE_LIGHT}:{BLUE_MID}" gradientangle=90
        color="{BLUE}"
    ];

    benchmark [
        label="⏱  benchmark.py\\n2 / 4 / 8 partitions\\nmedian runtime"
        style="rounded,filled,dashed"
        fillcolor="white"
        color="{SLATE}"
        fontcolor="{SLATE}"
    ];

    analysis [
        label="📈  partition_analysis.py\\ncategory skew + physical partition balance"
        style="rounded,filled,dashed"
        fillcolor="white"
        color="{SLATE}"
        fontcolor="{SLATE}"
    ];

    source -> profile;
    join -> baseline;
    join -> repart;
    baseline -> validate [label="reference" color="{CORAL}" fontcolor="{CORAL}"];
    agg -> validate;
    validate -> output [label="passed" color="{BLUE}" fontcolor="{BLUE}"];
    repart -> benchmark [style=dotted color="{SLATE}" arrowhead=none];
    repart -> analysis [style=dotted color="{SLATE}" arrowhead=none];

    {{ rank=same; benchmark; analysis; }}
}}
"""


def render(dot_source: str, out_png: Path) -> None:
    out_png.parent.mkdir(parents=True, exist_ok=True)
    dot_file = out_png.with_suffix(".dot")
    dot_file.write_text(dot_source, encoding="utf-8")

    try:
        subprocess.run(
            [
                "dot",
                "-Tpng",
                "-Gdpi=200",
                str(dot_file),
                "-o",
                str(out_png),
            ],
            check=True,
        )
    finally:
        if dot_file.exists():
            dot_file.unlink()

    print(f"Wrote {out_png}")


def main() -> int:
    render(
        ENTITY,
        DOCS / "entity-model-session1.png",
    )

    render(
        ARCHITECTURE,
        ARCH / "architecture-session1.png",
    )

    return 0


if __name__ == "__main__":
    sys.exit(main())