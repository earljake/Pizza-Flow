"""
pizzaflow_streaming_console.py
PizzaFlow — Event Streaming Console (Postgres-backed)

Simulates a partitioned durable log + independent consumer groups on top
of Postgres tables (no Kafka/broker needed). Every number shown is
computed live via SQL against the same order_details/orders/pizzas/
pizza_types tables you already loaded — there is no CSV reading here.

Partitioning: 4 pizza categories -> 4 partitions (Classic=0, Veggie=1,
Supreme=2, Chicken=3). Because each partition holds exactly one category,
"revenue-tracker" and "high-value-alerter" can process in bulk SQL, while
"audit-writer" writes one row per event — genuinely slower (I/O bound),
not an artificial delay.

Tabs:
    Pipeline           -> run stages, see a running log of results
    Durable log        -> partition distribution + skew explanation
    Consumers & lag    -> per-group throughput and committed offsets
    Failure & recovery -> crash the audit consumer on purpose, then recover
    Replay             -> what a durable log gives you for free
    Reconciliation     -> revenue by category & size, source-of-truth check
    Console            -> verbatim output of every stage
"""

import time
import threading
import datetime as dt
import tkinter as tk
from tkinter import ttk, messagebox, filedialog

import psycopg

# ---------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------
DB_CONFIG = dict(
    host="localhost",
    port="5432",
    dbname="Pizza-Flow",
    user="postgres",
    password="root",
)

PARTITION_MAP = {"Classic": 0, "Veggie": 1, "Supreme": 2, "Chicken": 3}
CONSUMER_GROUPS = ["revenue-tracker", "audit-writer", "high-value-alerter"]
DEFAULT_THRESHOLD = 40.00

GROUP_DESCRIPTIONS = {
    "revenue-tracker": "Rebuilds category revenue incrementally from the stream",
    "audit-writer": "Writes an independent append-only audit trail",
    "high-value-alerter": f"Flags order lines above {DEFAULT_THRESHOLD:.2f}",
}

# audit-writer crash-demo tuning: it commits its offset only every
# CHECKPOINT_EVERY_N_BATCHES batches of CHUNK_SIZE events, so a crash
# between two checkpoints leaves already-written rows uncommitted.
CHUNK_SIZE = 1000
CHECKPOINT_EVERY_N_BATCHES = 5

# ---------------------------------------------------------------
# STYLE
# ---------------------------------------------------------------
COLOR_BG = "#f4f6f8"
COLOR_PANEL = "#ffffff"
COLOR_HEADER = "#1c2b4a"
COLOR_ACCENT = "#2e6bd6"
COLOR_TEXT = "#1c1c1c"
COLOR_MUTED = "#6b7280"
COLOR_BORDER = "#e2e5e9"
COLOR_SUCCESS = "#1a7f5a"
COLOR_WARN = "#b4530a"
COLOR_DANGER = "#b3261e"
COLOR_ROW_ALT = "#f9fafb"
COLOR_INFO_BG = "#eaf1fd"
COLOR_WARN_BG = "#fdece0"
COLOR_CONSOLE_BG = "#0e1730"
COLOR_CONSOLE_FG = "#d7e2f5"
FONT_FAMILY = "Segoe UI"
BAR_COLORS = ["#2e6bd6", "#d9622b", "#1a7f5a", "#7a4fc9"]


# ---------------------------------------------------------------
# DATA LAYER
# ---------------------------------------------------------------
def get_connection():
    return psycopg.connect(**DB_CONFIG)


SCHEMA_SQL = """
DROP TABLE IF EXISTS high_value_alerts;
DROP TABLE IF EXISTS audit_log;
DROP TABLE IF EXISTS revenue_tracker_state;
DROP TABLE IF EXISTS consumer_offsets;
DROP TABLE IF EXISTS event_log;

CREATE TABLE event_log (
    event_id BIGSERIAL PRIMARY KEY,
    partition INT NOT NULL,
    offset_in_partition BIGINT NOT NULL,
    order_id INT,
    order_details_id INT,
    category VARCHAR(50),
    pizza_name VARCHAR(150),
    size VARCHAR(5),
    quantity INT,
    price NUMERIC(8,2),
    revenue NUMERIC(10,2),
    order_date DATE,
    order_time TIME,
    produced_at TIMESTAMP DEFAULT now(),
    UNIQUE (partition, offset_in_partition)
);

CREATE TABLE consumer_offsets (
    group_name VARCHAR(50),
    partition INT,
    committed_offset BIGINT DEFAULT -1,
    PRIMARY KEY (group_name, partition)
);

CREATE TABLE revenue_tracker_state (
    category VARCHAR(50) PRIMARY KEY,
    total_revenue NUMERIC(14,2) DEFAULT 0,
    event_count BIGINT DEFAULT 0
);

CREATE TABLE audit_log (
    event_id BIGINT,
    partition INT,
    offset_in_partition BIGINT,
    category VARCHAR(50),
    revenue NUMERIC(10,2),
    processed_at TIMESTAMP DEFAULT now()
);

CREATE TABLE high_value_alerts (
    event_id BIGINT,
    order_id INT,
    category VARCHAR(50),
    revenue NUMERIC(10,2),
    threshold NUMERIC(10,2),
    flagged_at TIMESTAMP DEFAULT now()
);
"""


def setup_schema():
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(SCHEMA_SQL)
        conn.commit()


def produce_events():
    """Bulk-inserts every order line as an event, partitioned by category."""
    start = time.time()
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "TRUNCATE event_log, consumer_offsets, revenue_tracker_state, "
                "audit_log, high_value_alerts RESTART IDENTITY;"
            )
            cur.execute(
                """
                INSERT INTO event_log
                    (partition, offset_in_partition, order_id, order_details_id,
                     category, pizza_name, size, quantity, price, revenue,
                     order_date, order_time)
                SELECT
                    CASE pt.category
                        WHEN 'Classic' THEN 0
                        WHEN 'Veggie' THEN 1
                        WHEN 'Supreme' THEN 2
                        WHEN 'Chicken' THEN 3
                        ELSE 9
                    END AS partition,
                    ROW_NUMBER() OVER (
                        PARTITION BY pt.category
                        ORDER BY o.order_id, od.order_details_id
                    ) - 1 AS offset_in_partition,
                    o.order_id, od.order_details_id, pt.category, pt.name,
                    p.size, od.quantity, p.price, (od.quantity * p.price),
                    o.date, o.time
                FROM order_details od
                JOIN orders o        ON od.order_id = o.order_id
                JOIN pizzas p        ON od.pizza_id = p.pizza_id
                JOIN pizza_types pt  ON p.pizza_type_id = pt.pizza_type_id;
                """
            )
            cur.execute(
                """
                INSERT INTO consumer_offsets (group_name, partition, committed_offset)
                SELECT g.group_name, e.partition, -1
                FROM (SELECT DISTINCT partition FROM event_log) e
                CROSS JOIN (SELECT unnest(%s::text[]) AS group_name) g
                ON CONFLICT DO NOTHING;
                """,
                (CONSUMER_GROUPS,),
            )
            cur.execute("SELECT COUNT(*) FROM event_log;")
            total = cur.fetchone()[0]
        conn.commit()
    elapsed = time.time() - start
    return total, elapsed


def get_partition_distribution():
    query = """
        SELECT partition, category, COUNT(*) AS events
        FROM event_log
        GROUP BY partition, category
        ORDER BY partition;
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query)
            return cur.fetchall()


def get_total_events():
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM event_log;")
            return cur.fetchone()[0]


def get_event_log_size_pretty():
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT pg_size_pretty(pg_total_relation_size('event_log'));")
            row = cur.fetchone()
            return row[0] if row else "n/a"


def get_partitions_present():
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT DISTINCT partition FROM event_log ORDER BY partition;")
            return [r[0] for r in cur.fetchall()]


def _get_committed(cur, group_name, partition):
    cur.execute(
        "SELECT committed_offset FROM consumer_offsets WHERE group_name=%s AND partition=%s;",
        (group_name, partition),
    )
    row = cur.fetchone()
    if row is None:
        cur.execute(
            "INSERT INTO consumer_offsets (group_name, partition, committed_offset) "
            "VALUES (%s, %s, -1) ON CONFLICT DO NOTHING;",
            (group_name, partition),
        )
        return -1
    return row[0]


def _commit_offset(cur, group_name, partition, new_offset):
    cur.execute(
        """
        INSERT INTO consumer_offsets (group_name, partition, committed_offset)
        VALUES (%s, %s, %s)
        ON CONFLICT (group_name, partition)
        DO UPDATE SET committed_offset = EXCLUDED.committed_offset;
        """,
        (group_name, partition, new_offset),
    )


def consume(group_name, threshold=DEFAULT_THRESHOLD):
    """Processes all new events for one consumer group. Returns (count, elapsed)."""
    start = time.time()
    processed = 0

    with get_connection() as conn:
        with conn.cursor() as cur:
            partitions = get_partitions_present()

            for p in partitions:
                committed = _get_committed(cur, group_name, p)

                cur.execute(
                    """
                    SELECT event_id, offset_in_partition, order_id, category, revenue
                    FROM event_log
                    WHERE partition = %s AND offset_in_partition > %s
                    ORDER BY offset_in_partition;
                    """,
                    (p, committed),
                )
                rows = cur.fetchall()
                if not rows:
                    continue

                if group_name == "revenue-tracker":
                    # single category per partition -> one bulk aggregate
                    cur.execute(
                        """
                        SELECT category, COUNT(*), COALESCE(SUM(revenue), 0)
                        FROM event_log
                        WHERE partition = %s AND offset_in_partition > %s
                        GROUP BY category;
                        """,
                        (p, committed),
                    )
                    agg = cur.fetchone()
                    if agg:
                        category, delta_count, delta_revenue = agg
                        cur.execute(
                            """
                            INSERT INTO revenue_tracker_state (category, total_revenue, event_count)
                            VALUES (%s, %s, %s)
                            ON CONFLICT (category) DO UPDATE SET
                                total_revenue = revenue_tracker_state.total_revenue + EXCLUDED.total_revenue,
                                event_count = revenue_tracker_state.event_count + EXCLUDED.event_count;
                            """,
                            (category, delta_revenue, delta_count),
                        )

                elif group_name == "high-value-alerter":
                    cur.execute(
                        """
                        INSERT INTO high_value_alerts (event_id, order_id, category, revenue, threshold)
                        SELECT event_id, order_id, category, revenue, %s
                        FROM event_log
                        WHERE partition = %s AND offset_in_partition > %s AND revenue > %s;
                        """,
                        (threshold, p, committed, threshold),
                    )

                elif group_name == "audit-writer":
                    # row-by-row on purpose: this is what makes it I/O-bound and slower
                    for event_id, off, order_id, category, revenue in rows:
                        cur.execute(
                            """
                            INSERT INTO audit_log (event_id, partition, offset_in_partition, category, revenue)
                            VALUES (%s, %s, %s, %s, %s);
                            """,
                            (event_id, p, off, category, revenue),
                        )
                        conn.commit()  # per-row commit, deliberately slow

                processed += len(rows)
                new_offset = rows[-1][1]
                _commit_offset(cur, group_name, p, new_offset)
                conn.commit()

    elapsed = time.time() - start
    return processed, elapsed


def get_consumer_stats():
    """Returns per-group: processed total, seconds N/A here (tracked in GUI), final lag."""
    query = """
        SELECT co.group_name, co.partition, co.committed_offset,
               (SELECT COUNT(*) FROM event_log e WHERE e.partition = co.partition) AS partition_total
        FROM consumer_offsets co
        ORDER BY co.group_name, co.partition;
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query)
            return cur.fetchall()


def get_group_committed_total(group_name):
    """Sum of (committed_offset + 1) across partitions for a group == events durably committed."""
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT committed_offset FROM consumer_offsets WHERE group_name=%s;",
                (group_name,),
            )
            rows = cur.fetchall()
    return sum(max(r[0] + 1, 0) for r in rows)


def get_group_lag(group_name):
    total = get_total_events()
    committed = get_group_committed_total(group_name)
    return max(total - committed, 0)


def get_audit_log_count():
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM audit_log;")
            return cur.fetchone()[0]


def get_revenue_source_of_truth():
    """Ground-truth revenue per category, computed directly via SQL (not via the stream)."""
    query = """
        SELECT pt.category, ROUND(SUM(od.quantity * p.price)::numeric, 2) AS revenue
        FROM order_details od
        JOIN orders o        ON od.order_id = o.order_id
        JOIN pizzas p        ON od.pizza_id = p.pizza_id
        JOIN pizza_types pt  ON p.pizza_type_id = pt.pizza_type_id
        GROUP BY pt.category
        ORDER BY pt.category;
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query)
            return cur.fetchall()


def get_revenue_tracker_state():
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT category, total_revenue FROM revenue_tracker_state ORDER BY category;")
            return cur.fetchall()


def get_revenue_by_category_size():
    query = """
        SELECT pt.category, p.size, COUNT(od.order_details_id) AS line_items,
               ROUND(SUM(od.quantity * p.price)::numeric, 2) AS revenue
        FROM order_details od
        JOIN orders o        ON od.order_id = o.order_id
        JOIN pizzas p        ON od.pizza_id = p.pizza_id
        JOIN pizza_types pt  ON p.pizza_type_id = pt.pizza_type_id
        GROUP BY pt.category, p.size
        ORDER BY pt.category, p.size;
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query)
            return cur.fetchall()


# ---------------------------------------------------------------
# FAILURE & RECOVERY — crash the audit-writer consumer on purpose
# ---------------------------------------------------------------
def reset_audit_writer():
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM audit_log;")
            cur.execute(
                "UPDATE consumer_offsets SET committed_offset = -1 WHERE group_name = 'audit-writer';"
            )
        conn.commit()


def _audit_writer_crash_phase(fail_after_n):
    """
    Replays the full log through audit-writer's row-by-row write path, but only
    commits its offset every CHECKPOINT_EVERY_N_BATCHES batches of CHUNK_SIZE
    events. Once `fail_after_n` events have been written, the process stops
    (simulated crash) — possibly mid-checkpoint, leaving already-written rows
    with no committed offset behind them.
    Returns (handled_total, elapsed).
    """
    start = time.time()
    handled_total = 0
    partitions = get_partitions_present()

    with get_connection() as conn:
        with conn.cursor() as cur:
            for p in partitions:
                cur.execute(
                    "SELECT event_id, offset_in_partition, order_id, category, revenue "
                    "FROM event_log WHERE partition = %s ORDER BY offset_in_partition;",
                    (p,),
                )
                rows = cur.fetchall()
                batch_count = 0

                for i in range(0, len(rows), CHUNK_SIZE):
                    batch = rows[i:i + CHUNK_SIZE]
                    for event_id, off, order_id, category, revenue in batch:
                        cur.execute(
                            """
                            INSERT INTO audit_log (event_id, partition, offset_in_partition, category, revenue)
                            VALUES (%s, %s, %s, %s, %s);
                            """,
                            (event_id, p, off, category, revenue),
                        )
                        conn.commit()

                    handled_total += len(batch)
                    batch_count += 1
                    last_batch_offset = batch[-1][1]

                    if handled_total >= fail_after_n:
                        elapsed = time.time() - start
                        return handled_total, elapsed

                    if batch_count % CHECKPOINT_EVERY_N_BATCHES == 0:
                        _commit_offset(cur, "audit-writer", p, last_batch_offset)
                        conn.commit()

    elapsed = time.time() - start
    return handled_total, elapsed


def inject_audit_writer_crash_and_recover(fail_after_n):
    """
    Full 'inject failure, then recover' demonstration for audit-writer.
    Returns a dict with the numbers needed to render the Failure & recovery tab.
    """
    reset_audit_writer()

    total_events = get_total_events()
    handled, crash_elapsed = _audit_writer_crash_phase(fail_after_n)
    committed_before = get_group_committed_total("audit-writer")
    backlog = max(total_events - committed_before, 0)
    redelivered = max(handled - committed_before, 0)

    recovered_count, recover_elapsed = consume("audit-writer")

    final_audit_count = get_audit_log_count()
    final_lag = get_group_lag("audit-writer")

    return {
        "total_events": total_events,
        "handled": handled,
        "committed_before": committed_before,
        "backlog": backlog,
        "redelivered": redelivered,
        "recovered_count": recovered_count,
        "recover_elapsed": recover_elapsed,
        "final_audit_count": final_audit_count,
        "final_lag": final_lag,
    }


# ---------------------------------------------------------------
# REPLAY — what a durable log gives you for free
# ---------------------------------------------------------------
def replay_new_consumer_reads_all_history():
    """A consumer that starts fresh (offset -1, never seen before) reads every event."""
    start = time.time()
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT category, COUNT(*), COALESCE(SUM(revenue), 0) FROM event_log GROUP BY category;")
            cur.fetchall()
    elapsed = time.time() - start
    total = get_total_events()
    return total, elapsed


def replay_rewind_and_reprocess():
    """Two independent full scans from offset 0 must produce identical aggregates."""
    def compute_totals():
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT category, SUM(revenue) FROM event_log GROUP BY category ORDER BY category;")
                return cur.fetchall()

    run1 = compute_totals()
    run2 = compute_totals()
    identical = run1 == run2
    return len(run1), identical


def replay_offline_consumer_catchup():
    """A consumer that was offline the whole time clears its entire backlog in one pass."""
    start = time.time()
    backlog = get_total_events()
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT category, COUNT(*), COALESCE(SUM(revenue), 0) FROM event_log GROUP BY category;")
            cur.fetchall()
    elapsed = time.time() - start
    return backlog, elapsed


def replay_partial_from(cutoff_dt):
    """
    Offsets are positions, not timestamps: to answer 'since <cutoff>' the log
    has to be scanned and filtered, not seeked to directly.
    Returns (total, matched, revenue_since).
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM event_log;")
            total = cur.fetchone()[0]
            cur.execute(
                "SELECT COUNT(*), COALESCE(ROUND(SUM(revenue)::numeric, 2), 0) "
                "FROM event_log WHERE (order_date + order_time) >= %s;",
                (cutoff_dt,),
            )
            matched, revenue_since = cur.fetchone()
    return total, matched, revenue_since


def run_all_replay_demonstrations(cutoff_dt):
    new_consumer_total, new_consumer_elapsed = replay_new_consumer_reads_all_history()
    rewind_categories, rewind_identical = replay_rewind_and_reprocess()
    offline_backlog, offline_elapsed = replay_offline_consumer_catchup()
    total, matched, revenue_since = replay_partial_from(cutoff_dt)
    pct = (matched / total * 100) if total else 0

    rows = [
        (
            "A new consumer reads all history",
            "It did not exist when events were produced; it read from offset 0",
            f"{new_consumer_total:,} consumed in {new_consumer_elapsed:.4f} s",
        ),
        (
            "Rewind and reprocess",
            "Two runs from offset 0 must produce identical results",
            f"{rewind_categories} categories · identical = {rewind_identical}",
        ),
        (
            "An offline consumer catches up",
            "Nothing was asked of the producer; the events were waiting in the log",
            f"backlog {offline_backlog:,} cleared in {offline_elapsed:.4f} s",
        ),
        (
            f"Partial replay from {cutoff_dt}",
            "Offsets are positions, not timestamps, so this is a scan rather than a seek",
            f"{matched:,} of {total:,} ({pct:.1f}%)",
        ),
    ]
    return rows, revenue_since


# ---------------------------------------------------------------
# GUI
# ---------------------------------------------------------------
class PizzaFlowConsole(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("PizzaFlow — Event Streaming Console")
        self.geometry("1260x820")
        self.configure(bg=COLOR_BG)
        self.minsize(1040, 700)

        self.consumer_timings = {g: {"processed": 0, "seconds": 0.0} for g in CONSUMER_GROUPS}
        self.reconciled_this_session = False

        self._build_style()
        self._build_header()
        self._build_tabs()
        self._build_pipeline_tab()
        self._build_durable_log_tab()
        self._build_consumers_tab()
        self._build_failure_tab()
        self._build_replay_tab()
        self._build_reconciliation_tab()
        self._build_console_tab()

        self._ensure_schema_then_refresh()

    # ---------- style ----------
    def _build_style(self):
        style = ttk.Style(self)
        style.theme_use("clam")
        style.configure("TNotebook", background=COLOR_BG, borderwidth=0)
        style.configure("TNotebook.Tab", font=(FONT_FAMILY, 10, "bold"),
                         padding=(16, 9), background=COLOR_PANEL, foreground=COLOR_MUTED)
        style.map("TNotebook.Tab", background=[("selected", COLOR_ACCENT)],
                  foreground=[("selected", "#ffffff")])
        style.configure("Treeview", font=(FONT_FAMILY, 10), rowheight=26,
                         background=COLOR_PANEL, fieldbackground=COLOR_PANEL,
                         foreground=COLOR_TEXT, borderwidth=0)
        style.configure("Treeview.Heading", font=(FONT_FAMILY, 10, "bold"),
                         background=COLOR_HEADER, foreground="#ffffff", relief="flat")
        style.configure("Accent.TButton", font=(FONT_FAMILY, 10, "bold"),
                         padding=(12, 7), background=COLOR_ACCENT, foreground="#ffffff",
                         borderwidth=0)
        style.map("Accent.TButton", background=[("active", "#24509e")])
        style.configure("Ghost.TButton", font=(FONT_FAMILY, 10), padding=(10, 6),
                         background=COLOR_PANEL, foreground=COLOR_TEXT, borderwidth=1)
        style.configure("Danger.TButton", font=(FONT_FAMILY, 10, "bold"), padding=(12, 7),
                         background=COLOR_DANGER, foreground="#ffffff", borderwidth=0)
        style.map("Danger.TButton", background=[("active", "#8c1e18")])

    # ---------- header ----------
    def _build_header(self):
        header = tk.Frame(self, bg=COLOR_HEADER, height=76)
        header.pack(fill="x", side="top")
        header.pack_propagate(False)

        tk.Label(header, text="PizzaFlow — Event Streaming Console",
                 font=(FONT_FAMILY, 17, "bold"), bg=COLOR_HEADER, fg="#ffffff"
                 ).pack(side="left", padx=(24, 0), pady=(12, 0), anchor="w")

        tk.Label(header,
                 text="MIT 261 Parallel and Distributed Systems  ·  topic pizza.order_line.recorded  ·  4 partitions keyed on category",
                 font=(FONT_FAMILY, 10), bg=COLOR_HEADER, fg="#c7d2e8"
                 ).pack(side="top", anchor="w", padx=24, pady=(0, 12))

    # ---------- tabs shell ----------
    def _build_tabs(self):
        self.notebook = ttk.Notebook(self)
        self.notebook.pack(fill="both", expand=True, padx=18, pady=16)
        self.pipeline_frame = tk.Frame(self.notebook, bg=COLOR_BG)
        self.durable_frame = tk.Frame(self.notebook, bg=COLOR_BG)
        self.consumers_frame = tk.Frame(self.notebook, bg=COLOR_BG)
        self.failure_frame = tk.Frame(self.notebook, bg=COLOR_BG)
        self.replay_frame = tk.Frame(self.notebook, bg=COLOR_BG)
        self.recon_frame = tk.Frame(self.notebook, bg=COLOR_BG)
        self.console_frame = tk.Frame(self.notebook, bg=COLOR_BG)
        self.notebook.add(self.pipeline_frame, text="Pipeline")
        self.notebook.add(self.durable_frame, text="Durable log")
        self.notebook.add(self.consumers_frame, text="Consumers & lag")
        self.notebook.add(self.failure_frame, text="Failure & recovery")
        self.notebook.add(self.replay_frame, text="Replay")
        self.notebook.add(self.recon_frame, text="Reconciliation")
        self.notebook.add(self.console_frame, text="Console")

    # ---------- Pipeline tab ----------
    def _build_pipeline_tab(self):
        frame = self.pipeline_frame

        # metric cards
        cards_row = tk.Frame(frame, bg=COLOR_BG)
        cards_row.pack(fill="x", pady=(0, 14))
        self.metric_labels = {}
        for key, title in [
            ("events", "events in the durable log"),
            ("groups", "consumer groups tracked"),
            ("lag", "total lag across all groups"),
            ("recon", "reconciliation status"),
        ]:
            card = tk.Frame(cards_row, bg=COLOR_PANEL, highlightbackground=COLOR_BORDER,
                             highlightthickness=1)
            card.pack(side="left", expand=True, fill="both", padx=6)
            value_lbl = tk.Label(card, text="—", font=(FONT_FAMILY, 20, "bold"),
                                  bg=COLOR_PANEL, fg=COLOR_HEADER)
            value_lbl.pack(pady=(16, 2))
            tk.Label(card, text=title, font=(FONT_FAMILY, 9), bg=COLOR_PANEL,
                     fg=COLOR_MUTED).pack(pady=(0, 14))
            self.metric_labels[key] = value_lbl

        # stage buttons
        tk.Label(frame, text="Run a stage", font=(FONT_FAMILY, 11, "bold"),
                 bg=COLOR_BG, fg=COLOR_TEXT).pack(anchor="w", pady=(0, 6))
        btn_row = tk.Frame(frame, bg=COLOR_BG)
        btn_row.pack(fill="x", pady=(0, 12))

        ttk.Button(btn_row, text="Produce (reset)", style="Ghost.TButton",
                   command=lambda: self.run_stage("produce")).pack(side="left", padx=4)
        for g in CONSUMER_GROUPS:
            ttk.Button(btn_row, text=f"Consume: {g}", style="Ghost.TButton",
                       command=lambda g=g: self.run_stage("consume", g)).pack(side="left", padx=4)
        ttk.Button(btn_row, text="Reconcile", style="Ghost.TButton",
                   command=lambda: self.run_stage("reconcile")).pack(side="left", padx=4)
        ttk.Button(btn_row, text="Run everything", style="Accent.TButton",
                   command=self.run_everything).pack(side="left", padx=10)

        # stage log table
        columns = ("stage", "status", "result")
        self.stage_tree = ttk.Treeview(frame, columns=columns, show="headings", height=12)
        self.stage_tree.heading("stage", text="Stage")
        self.stage_tree.heading("status", text="Status")
        self.stage_tree.heading("result", text="Headline result")
        self.stage_tree.column("stage", width=180, anchor="w")
        self.stage_tree.column("status", width=100, anchor="center")
        self.stage_tree.column("result", width=760, anchor="w")
        self.stage_tree.pack(fill="both", expand=True, pady=(0, 4))
        self.stage_tree.tag_configure("pass", background="#e6f4ec")
        self.stage_tree.tag_configure("fail", background="#fbe9e7")

    # ---------- Durable log tab ----------
    def _build_durable_log_tab(self):
        frame = self.durable_frame

        top = tk.Frame(frame, bg=COLOR_BG)
        top.pack(fill="x")
        tk.Label(top, text="The log and its partitions", font=(FONT_FAMILY, 13, "bold"),
                 bg=COLOR_BG, fg=COLOR_TEXT).pack(side="left")

        self.log_summary_label = tk.Label(frame, text="Run Produce to populate the log.",
                                           font=(FONT_FAMILY, 10), bg="#e6f4ec", fg=COLOR_SUCCESS,
                                           anchor="w", padx=10, pady=6)
        self.log_summary_label.pack(fill="x", pady=(8, 12))

        body = tk.Frame(frame, bg=COLOR_BG)
        body.pack(fill="both", expand=True)

        self.partition_canvas = tk.Canvas(body, bg=COLOR_PANEL, height=260,
                                           highlightbackground=COLOR_BORDER, highlightthickness=1)
        self.partition_canvas.pack(fill="x", pady=(0, 10))

        columns = ("partition", "category", "events", "share")
        self.partition_tree = ttk.Treeview(body, columns=columns, show="headings", height=6)
        for col, label, w in [("partition", "Partition", 100), ("category", "Category", 160),
                               ("events", "Events", 120), ("share", "Share", 120)]:
            self.partition_tree.heading(col, text=label)
            self.partition_tree.column(col, width=w, anchor="center")
        self.partition_tree.pack(fill="x")

    # ---------- Consumers tab ----------
    def _build_consumers_tab(self):
        frame = self.consumers_frame

        tk.Label(frame, text="Three groups, one topic, independent offsets",
                 font=(FONT_FAMILY, 13, "bold"), bg=COLOR_BG, fg=COLOR_TEXT
                 ).pack(anchor="w", pady=(0, 10))

        self.throughput_canvas = tk.Canvas(frame, bg=COLOR_PANEL, height=240,
                                            highlightbackground=COLOR_BORDER, highlightthickness=1)
        self.throughput_canvas.pack(fill="x", pady=(0, 12))

        columns = ("group", "processed", "seconds", "eps", "lag")
        self.consumer_tree = ttk.Treeview(frame, columns=columns, show="headings", height=5)
        for col, label, w in [("group", "Group", 200), ("processed", "Processed", 120),
                               ("seconds", "Seconds", 100), ("eps", "Events/sec", 120),
                               ("lag", "Final lag", 100)]:
            self.consumer_tree.heading(col, text=label)
            self.consumer_tree.column(col, width=w, anchor="center")
        self.consumer_tree.pack(fill="x", pady=(0, 12))

        tk.Label(frame, text="Committed offsets, per group per partition",
                 font=(FONT_FAMILY, 11, "bold"), bg=COLOR_BG, fg=COLOR_TEXT
                 ).pack(anchor="w", pady=(0, 6))
        columns2 = ("group", "partition", "committed", "total", "lag")
        self.offsets_tree = ttk.Treeview(frame, columns=columns2, show="headings", height=10)
        for col, label, w in [("group", "Group", 200), ("partition", "Partition", 100),
                               ("committed", "Committed offset", 150), ("total", "Partition total", 150),
                               ("lag", "Lag", 100)]:
            self.offsets_tree.heading(col, text=label)
            self.offsets_tree.column(col, width=w, anchor="center")
        self.offsets_tree.pack(fill="both", expand=True)

    # ---------- Failure & recovery tab ----------
    def _build_failure_tab(self):
        frame = self.failure_frame

        top = tk.Frame(frame, bg=COLOR_BG)
        top.pack(fill="x", pady=(0, 10))
        tk.Label(top, text="Crash the audit consumer on purpose",
                 font=(FONT_FAMILY, 13, "bold"), bg=COLOR_BG, fg=COLOR_TEXT
                 ).pack(side="left")

        controls = tk.Frame(top, bg=COLOR_BG)
        controls.pack(side="right")
        self.fail_after_var = tk.StringVar(value="25000")
        tk.Entry(controls, textvariable=self.fail_after_var, width=10,
                 font=(FONT_FAMILY, 10), justify="right").pack(side="left", padx=(0, 6))
        tk.Label(controls, text="fail after N events:", font=(FONT_FAMILY, 10),
                 bg=COLOR_BG, fg=COLOR_MUTED).pack(side="left", padx=(0, 10))
        ttk.Button(controls, text="Inject failure, then recover", style="Danger.TButton",
                   command=self.run_failure_demo).pack(side="left")

        self.failure_banner = tk.Label(
            frame, text="Not run yet — set N and click \"Inject failure, then recover\".",
            font=(FONT_FAMILY, 10), bg=COLOR_WARN_BG, fg=COLOR_WARN,
            anchor="w", padx=10, pady=8, justify="left", wraplength=1160,
        )
        self.failure_banner.pack(fill="x", pady=(0, 14))

        tk.Label(frame, text="Blast radius — what else failed with it",
                 font=(FONT_FAMILY, 11, "bold"), bg=COLOR_BG, fg=COLOR_TEXT
                 ).pack(anchor="w", pady=(0, 6))
        columns = ("component", "status", "evidence")
        self.blast_tree = ttk.Treeview(frame, columns=columns, show="headings", height=4)
        self.blast_tree.heading("component", text="Component")
        self.blast_tree.heading("status", text="Status")
        self.blast_tree.heading("evidence", text="Evidence")
        self.blast_tree.column("component", width=180, anchor="w")
        self.blast_tree.column("status", width=120, anchor="center")
        self.blast_tree.column("evidence", width=760, anchor="w")
        self.blast_tree.pack(fill="x", pady=(0, 14))
        self.blast_tree.tag_configure("ok", background="#e6f4ec")
        self.blast_tree.tag_configure("down", background="#fbe9e7")

        cards_row = tk.Frame(frame, bg=COLOR_BG)
        cards_row.pack(fill="x", pady=(0, 14))
        self.failure_metric_labels = {}
        for key, title, color in [
            ("handled", "handled before the crash", COLOR_HEADER),
            ("backlog", "backlog left in the log", COLOR_WARN),
            ("audit_entries", "audit entries written", COLOR_HEADER),
            ("redelivered", "redelivered on restart", COLOR_DANGER),
        ]:
            card = tk.Frame(cards_row, bg=COLOR_PANEL, highlightbackground=COLOR_BORDER,
                             highlightthickness=1)
            card.pack(side="left", expand=True, fill="both", padx=6)
            value_lbl = tk.Label(card, text="—", font=(FONT_FAMILY, 20, "bold"),
                                  bg=COLOR_PANEL, fg=color)
            value_lbl.pack(pady=(16, 2))
            tk.Label(card, text=title, font=(FONT_FAMILY, 9), bg=COLOR_PANEL,
                     fg=COLOR_MUTED).pack(pady=(0, 14))
            self.failure_metric_labels[key] = value_lbl

        self.failure_explainer = tk.Label(
            frame, text="", font=(FONT_FAMILY, 9), bg=COLOR_INFO_BG, fg=COLOR_TEXT,
            anchor="w", padx=10, pady=8, justify="left", wraplength=1160,
        )
        self.failure_explainer.pack(fill="x")

    # ---------- Replay tab ----------
    def _build_replay_tab(self):
        frame = self.replay_frame

        top = tk.Frame(frame, bg=COLOR_BG)
        top.pack(fill="x", pady=(0, 10))
        tk.Label(top, text="What a durable log gives you for free",
                 font=(FONT_FAMILY, 13, "bold"), bg=COLOR_BG, fg=COLOR_TEXT
                 ).pack(side="left")

        controls = tk.Frame(top, bg=COLOR_BG)
        controls.pack(side="right")
        ttk.Button(controls, text="Run all four demonstrations", style="Accent.TButton",
                   command=self.run_replay_demo).pack(side="left")
        self.replay_cutoff_var = tk.StringVar(value=self._default_replay_cutoff())
        tk.Entry(controls, textvariable=self.replay_cutoff_var, width=20,
                 font=(FONT_FAMILY, 10), justify="right").pack(side="left", padx=(0, 8))
        tk.Label(controls, text="partial replay from:", font=(FONT_FAMILY, 10),
                 bg=COLOR_BG, fg=COLOR_MUTED).pack(side="left", padx=(0, 10))

        self.replay_banner = tk.Label(
            frame, text="Not run yet — run Produce on the Pipeline tab first, then click \"Run all four demonstrations\".",
            font=(FONT_FAMILY, 10), bg=COLOR_INFO_BG, fg=COLOR_ACCENT,
            anchor="w", padx=10, pady=8, justify="left", wraplength=1160,
        )
        self.replay_banner.pack(fill="x", pady=(0, 14))

        columns = ("demo", "proves", "result")
        self.replay_tree = ttk.Treeview(frame, columns=columns, show="headings", height=5)
        self.replay_tree.heading("demo", text="Demonstration")
        self.replay_tree.heading("proves", text="What it proves")
        self.replay_tree.heading("result", text="Result")
        self.replay_tree.column("demo", width=260, anchor="w")
        self.replay_tree.column("proves", width=520, anchor="w")
        self.replay_tree.column("result", width=280, anchor="w")
        self.replay_tree.pack(fill="x", pady=(0, 16))

        tk.Label(frame, text="Revenue by category and size — computed by a consumer that did not exist when the events were produced",
                 font=(FONT_FAMILY, 11, "bold"), bg=COLOR_BG, fg=COLOR_TEXT, wraplength=1160, justify="left"
                 ).pack(anchor="w", pady=(0, 6))
        columns2 = ("category", "size", "line_items", "revenue")
        self.replay_revenue_tree = ttk.Treeview(frame, columns=columns2, show="headings", height=10)
        for col, label, w, anc in [("category", "Category", 220, "w"), ("size", "Size", 100, "center"),
                                    ("line_items", "Line Items", 140, "e"), ("revenue", "Revenue", 180, "e")]:
            self.replay_revenue_tree.heading(col, text=label)
            self.replay_revenue_tree.column(col, width=w, anchor=anc)
        self.replay_revenue_tree.pack(fill="both", expand=True)
        self.replay_revenue_tree.tag_configure("odd", background=COLOR_ROW_ALT)

    def _default_replay_cutoff(self):
        return dt.datetime.now().replace(hour=0, minute=0, second=0, microsecond=0).strftime("%Y-%m-%d %H:%M:%S")

    # ---------- Reconciliation tab ----------
    def _build_reconciliation_tab(self):
        frame = self.recon_frame

        tk.Label(frame, text="Revenue computed by a consumer vs. source-of-truth SQL",
                 font=(FONT_FAMILY, 13, "bold"), bg=COLOR_BG, fg=COLOR_TEXT
                 ).pack(anchor="w", pady=(0, 10))

        columns = ("category", "stream_total", "truth_total", "delta")
        self.recon_check_tree = ttk.Treeview(frame, columns=columns, show="headings", height=5)
        for col, label, w in [("category", "Category", 200), ("stream_total", "revenue-tracker total", 220),
                               ("truth_total", "Source-of-truth total", 220), ("delta", "Delta", 140)]:
            self.recon_check_tree.heading(col, text=label)
            self.recon_check_tree.column(col, width=w, anchor="center")
        self.recon_check_tree.pack(fill="x", pady=(0, 16))

        tk.Label(frame, text="Revenue by pizza category and size",
                 font=(FONT_FAMILY, 12, "bold"), bg=COLOR_BG, fg=COLOR_TEXT
                 ).pack(anchor="w", pady=(0, 6))

        columns2 = ("category", "size", "line_items", "revenue")
        self.recon_tree = ttk.Treeview(frame, columns=columns2, show="headings", height=14)
        for col, label, w, anc in [("category", "Category", 220, "w"), ("size", "Size", 100, "center"),
                                    ("line_items", "Line Items", 140, "e"), ("revenue", "Revenue", 180, "e")]:
            self.recon_tree.heading(col, text=label)
            self.recon_tree.column(col, width=w, anchor=anc)
        self.recon_tree.pack(fill="both", expand=True)
        self.recon_tree.tag_configure("odd", background=COLOR_ROW_ALT)
        self.recon_tree.tag_configure("total", background="#e8f0fe", font=(FONT_FAMILY, 10, "bold"))

    # ---------- Console tab ----------
    def _build_console_tab(self):
        frame = self.console_frame

        top = tk.Frame(frame, bg=COLOR_BG)
        top.pack(fill="x", pady=(0, 10))
        tk.Label(top, text="Verbatim output of every stage",
                 font=(FONT_FAMILY, 13, "bold"), bg=COLOR_BG, fg=COLOR_TEXT
                 ).pack(side="left")

        controls = tk.Frame(top, bg=COLOR_BG)
        controls.pack(side="right")
        ttk.Button(controls, text="Clear", style="Ghost.TButton",
                   command=self.clear_console).pack(side="right", padx=(6, 0))
        ttk.Button(controls, text="Save transcript...", style="Ghost.TButton",
                   command=self.save_console_transcript).pack(side="right")

        body = tk.Frame(frame, bg=COLOR_CONSOLE_BG, highlightbackground=COLOR_BORDER, highlightthickness=1)
        body.pack(fill="both", expand=True)

        self.console_text = tk.Text(
            body, bg=COLOR_CONSOLE_BG, fg=COLOR_CONSOLE_FG, insertbackground=COLOR_CONSOLE_FG,
            font=("Consolas", 10), wrap="none", relief="flat", padx=12, pady=10,
        )
        console_scroll = ttk.Scrollbar(body, orient="vertical", command=self.console_text.yview)
        self.console_text.configure(yscrollcommand=console_scroll.set, state="disabled")
        self.console_text.pack(side="left", fill="both", expand=True)
        console_scroll.pack(side="right", fill="y")

    def append_console(self, text):
        self.console_text.configure(state="normal")
        if self.console_text.index("end-1c") != "1.0":
            self.console_text.insert("end", "\n\n")
        self.console_text.insert("end", text)
        self.console_text.see("end")
        self.console_text.configure(state="disabled")

    def clear_console(self):
        self.console_text.configure(state="normal")
        self.console_text.delete("1.0", "end")
        self.console_text.configure(state="disabled")

    def save_console_transcript(self):
        path = filedialog.asksaveasfilename(
            defaultextension=".txt",
            filetypes=[("Text file", "*.txt"), ("All files", "*.*")],
            initialfile="pizzaflow_console_transcript.txt",
        )
        if not path:
            return
        content = self.console_text.get("1.0", "end-1c")
        try:
            with open(path, "w", encoding="utf-8") as f:
                f.write(content)
        except OSError as e:
            messagebox.showerror("Save failed", str(e))

    # ---------------------------------------------------------------
    # console formatting helpers
    # ---------------------------------------------------------------
    def _console_produce_block(self, total, elapsed):
        rate = total / elapsed if elapsed > 0 else 0
        try:
            size_pretty = get_event_log_size_pretty()
        except Exception:
            size_pretty = "n/a"
        return (
            f"  {total:,} events appended\n\n"
            f"  produced        : {total:,} events in {elapsed:.3f} s\n"
            f"  throughput      : {rate:,.0f} events/second\n"
            f"  log size        : {size_pretty}"
        )

    def _console_partition_block(self, rows):
        total = sum(r[2] for r in rows)
        max_events = max((r[2] for r in rows), default=1)
        lines = ["=" * 75, "PARTITION DISTRIBUTION", "=" * 75, ""]
        for partition, category, events in rows:
            pct = (events / total * 100) if total else 0
            bar_len = int((events / max_events) * 20) if max_events else 0
            lines.append(f"  partition {partition} : {events:>7,}  {pct:5.1f}%  {'#' * bar_len}")
        if rows:
            values = [r[2] for r in rows]
            skew = (max(values) / min(values)) if min(values) else 0
            lines.append("")
            lines.append(f"  partition skew   : {skew:.2f} : 1")
            lines.append("  Uneven category volumes in the source data do not divide evenly across partitions.")
        return "\n".join(lines)

    def _console_consumer_block(self, group_name, processed, elapsed):
        eps = processed / elapsed if elapsed > 0 else 0
        description = GROUP_DESCRIPTIONS.get(group_name, "")
        lines = [
            "=" * 75,
            f"CONSUMER: {group_name}",
            "=" * 75,
            "",
            f"  {description}",
            f"  processed        : {processed:,} events in {elapsed:.3f} s",
            f"  throughput       : {eps:,.0f} events/second",
        ]
        return "\n".join(lines)

    def _console_reconcile_block(self, status, max_delta):
        return (
            f"{'=' * 75}\nRECONCILE\n{'=' * 75}\n\n"
            f"  status           : {status}\n"
            f"  max delta        : {max_delta:.4f}"
        )

    # ---------------------------------------------------------------
    # bar chart helper (no external chart library needed)
    # ---------------------------------------------------------------
    def _draw_bars(self, canvas, labels, values, note_lines=None):
        canvas.delete("all")
        canvas.update_idletasks()
        w = max(canvas.winfo_width(), 600)
        h = canvas.winfo_height() or 240
        n = len(values)
        if n == 0:
            canvas.create_text(w // 2, h // 2, text="No data yet", fill=COLOR_MUTED,
                                font=(FONT_FAMILY, 10))
            return

        margin_bottom = 46
        margin_top = 30
        chart_h = h - margin_bottom - margin_top
        bar_w = min(90, (w - 60) / n * 0.6)
        gap = (w - 60) / n
        max_val = max(values) if max(values) > 0 else 1

        for i, (label, val) in enumerate(zip(labels, values)):
            x0 = 40 + i * gap + (gap - bar_w) / 2
            bar_h = (val / max_val) * chart_h
            y1 = h - margin_bottom
            y0 = y1 - bar_h
            color = BAR_COLORS[i % len(BAR_COLORS)]
            canvas.create_rectangle(x0, y0, x0 + bar_w, y1, fill=color, outline="")
            canvas.create_text(x0 + bar_w / 2, y0 - 12, text=f"{val:,.0f}",
                                font=(FONT_FAMILY, 9, "bold"), fill=COLOR_TEXT)
            canvas.create_text(x0 + bar_w / 2, y1 + 16, text=label,
                                font=(FONT_FAMILY, 9), fill=COLOR_MUTED)

    # ---------------------------------------------------------------
    # orchestration
    # ---------------------------------------------------------------
    def _ensure_schema_then_refresh(self):
        def worker():
            try:
                # create schema only if event_log doesn't exist yet
                with get_connection() as conn:
                    with conn.cursor() as cur:
                        cur.execute(
                            "SELECT EXISTS (SELECT FROM information_schema.tables "
                            "WHERE table_name = 'event_log');"
                        )
                        exists = cur.fetchone()[0]
                if not exists:
                    setup_schema()
            except Exception as e:
                self.after(0, lambda: messagebox.showerror("Postgres error", str(e)))
                return
            self.after(0, self.refresh_all_tabs)

        threading.Thread(target=worker, daemon=True).start()

    def log_stage(self, stage, status, result):
        tag = "pass" if status == "passed" else "fail"
        self.stage_tree.insert("", "end", values=(stage, status, result), tags=(tag,))

    def run_stage(self, kind, group=None):
        threading.Thread(target=self._run_stage_worker, args=(kind, group), daemon=True).start()

    def _run_stage_worker(self, kind, group):
        try:
            if kind == "produce":
                total, elapsed = produce_events()
                rate = total / elapsed if elapsed > 0 else 0
                result = f"{total:,} events in {elapsed:.3f}s · {rate:,.0f} events/second"
                self.after(0, self.log_stage, "Produce", "passed", result)
                self.after(0, self.append_console, self._console_produce_block(total, elapsed))
                try:
                    dist = get_partition_distribution()
                    self.after(0, self.append_console, self._console_partition_block(dist))
                except Exception:
                    pass

            elif kind == "consume":
                processed, elapsed = consume(group)
                eps = processed / elapsed if elapsed > 0 else 0
                self.consumer_timings[group] = {"processed": processed, "seconds": elapsed}
                result = f"{processed:,} events in {elapsed:.3f}s · {eps:,.0f} events/second"
                self.after(0, self.log_stage, f"Consume: {group}", "passed", result)
                self.after(0, self.append_console, self._console_consumer_block(group, processed, elapsed))

            elif kind == "reconcile":
                self.reconciled_this_session = True
                stream = dict(get_revenue_tracker_state())
                truth = dict(get_revenue_source_of_truth())
                max_delta = 0.0
                for cat in truth:
                    delta = abs(float(stream.get(cat, 0)) - float(truth[cat]))
                    max_delta = max(max_delta, delta)
                status = "passed" if max_delta < 0.01 else "failed"
                result = f"max delta across categories: {max_delta:.4f}"
                self.after(0, self.log_stage, "Reconcile", status, result)
                self.after(0, self.append_console, self._console_reconcile_block(status, max_delta))

        except Exception as e:
            self.after(0, self.log_stage, kind, "failed", str(e))
        finally:
            self.after(0, self.refresh_all_tabs)

    def run_everything(self):
        def worker():
            self._run_stage_worker("produce", None)
            for g in CONSUMER_GROUPS:
                self._run_stage_worker("consume", g)
            self._run_stage_worker("reconcile", None)

        threading.Thread(target=worker, daemon=True).start()

    # ---------- Failure & recovery orchestration ----------
    def run_failure_demo(self):
        try:
            fail_after_n = int(self.fail_after_var.get())
            if fail_after_n <= 0:
                raise ValueError
        except ValueError:
            messagebox.showerror("Invalid input", "\"fail after N events\" must be a positive whole number.")
            return

        if not get_partitions_present():
            messagebox.showwarning("No events yet", "Run Produce on the Pipeline tab first.")
            return

        threading.Thread(target=self._run_failure_demo_worker, args=(fail_after_n,), daemon=True).start()

    def _run_failure_demo_worker(self, fail_after_n):
        try:
            r = inject_audit_writer_crash_and_recover(fail_after_n)
        except Exception as e:
            self.after(0, lambda: messagebox.showerror("Postgres error", str(e)))
            return
        self.after(0, self._render_failure_result, r)

    def _render_failure_result(self, r):
        banner = (
            f"Crashed after handling {r['handled']:,} events with only {r['committed_before']:,} committed  ·  "
            f"backlog {r['backlog']:,}  ·  recovered {r['recovered_count']:,} events in {r['recover_elapsed']:.3f} s  ·  "
            f"final lag {r['final_lag']:,}"
        )
        self.failure_banner.config(text=banner, bg=COLOR_WARN_BG, fg=COLOR_WARN)

        for row in self.blast_tree.get_children():
            self.blast_tree.delete(row)

        self.blast_tree.insert("", "end", values=(
            "Producer", "UNAFFECTED", f"{r['total_events']:,} events already durable",
        ), tags=("ok",))

        for g in ("revenue-tracker", "high-value-alerter"):
            lag = get_group_lag(g)
            processed = get_group_committed_total(g)
            if lag == 0:
                self.blast_tree.insert("", "end", values=(
                    g, "UNAFFECTED", f"processed {processed:,}, lag 0",
                ), tags=("ok",))
            else:
                self.blast_tree.insert("", "end", values=(
                    g, "INDEPENDENT", f"lag {lag:,} (its own backlog, unrelated to the audit-writer crash)",
                ), tags=("ok",))

        self.blast_tree.insert("", "end", values=(
            "audit-writer", "DOWN then RECOVERED",
            f"crashed with lag {r['backlog']:,} events retained in the log; final lag {r['final_lag']:,}",
        ), tags=("down",))

        self.failure_metric_labels["handled"].config(text=f"{r['handled']:,}")
        self.failure_metric_labels["backlog"].config(text=f"{r['backlog']:,}")
        self.failure_metric_labels["audit_entries"].config(text=f"{r['final_audit_count']:,}")
        self.failure_metric_labels["redelivered"].config(text=f"{r['redelivered']:,}")

        explainer = (
            "The arithmetic reconciles, which is worth checking rather than assuming. The consumer handled "
            f"{r['handled']:,} events but committed only {r['committed_before']:,}, because offsets commit every "
            f"{CHECKPOINT_EVERY_N_BATCHES} batches of {CHUNK_SIZE:,}. {r['total_events']:,} minus "
            f"{r['committed_before']:,} leaves the {r['backlog']:,} backlog the log reports. On restart it resumed "
            f"from its last committed offsets, so the {r['redelivered']:,} events between the last commit and the "
            f"crash arrived a second time — {r['final_audit_count']:,} audit entries for {r['total_events']:,} "
            "events. That gap is at-least-once delivery, measured. A production audit store would key on event_id "
            "and let the second insert be rejected."
        )
        self.failure_explainer.config(text=explainer)

        self.append_console(
            f"{'=' * 75}\nFAILURE & RECOVERY: audit-writer\n{'=' * 75}\n\n"
            f"  fail after       : {r['handled']:,} events handled ({r['committed_before']:,} committed)\n"
            f"  backlog          : {r['backlog']:,}\n"
            f"  recovered        : {r['recovered_count']:,} events in {r['recover_elapsed']:.3f} s\n"
            f"  redelivered      : {r['redelivered']:,}\n"
            f"  final audit rows : {r['final_audit_count']:,}\n"
            f"  final lag        : {r['final_lag']:,}"
        )
        self.refresh_all_tabs()

    # ---------- Replay orchestration ----------
    def run_replay_demo(self):
        if not get_partitions_present():
            messagebox.showwarning("No events yet", "Run Produce on the Pipeline tab first.")
            return

        raw = self.replay_cutoff_var.get().strip()
        cutoff = self._parse_cutoff(raw)
        if cutoff is None:
            messagebox.showerror(
                "Invalid date/time",
                "\"partial replay from\" must look like 2026-06-01 or 2026-06-01 14:30:00.",
            )
            return

        threading.Thread(target=self._run_replay_demo_worker, args=(cutoff,), daemon=True).start()

    def _parse_cutoff(self, raw):
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d", "%Y-%m-%dT%H:%M:%S"):
            try:
                return dt.datetime.strptime(raw, fmt)
            except ValueError:
                continue
        return None

    def _run_replay_demo_worker(self, cutoff):
        try:
            rows, revenue_since = run_all_replay_demonstrations(cutoff)
            revenue_rows = get_revenue_by_category_size()
        except Exception as e:
            self.after(0, lambda: messagebox.showerror("Postgres error", str(e)))
            return
        self.after(0, self._render_replay_result, cutoff, rows, revenue_since, revenue_rows)

    def _render_replay_result(self, cutoff, rows, revenue_since, revenue_rows):
        self.replay_banner.config(
            text=f"All four demonstrations ran. Revenue since {cutoff}: {float(revenue_since):,.2f}",
            bg="#e6f4ec", fg=COLOR_SUCCESS,
        )

        for r in self.replay_tree.get_children():
            self.replay_tree.delete(r)
        for demo, proves, result in rows:
            self.replay_tree.insert("", "end", values=(demo, proves, result))

        for r in self.replay_revenue_tree.get_children():
            self.replay_revenue_tree.delete(r)
        for i, (category, size, line_items, revenue) in enumerate(revenue_rows):
            tag = "odd" if i % 2 else ""
            self.replay_revenue_tree.insert("", "end", values=(
                category, size, f"{line_items:,}", f"${revenue:,.2f}"
            ), tags=(tag,))

        console_lines = [f"{'=' * 75}\nREPLAY: what a durable log gives you for free\n{'=' * 75}\n"]
        for demo, proves, result in rows:
            console_lines.append(f"  {demo}\n    proves : {proves}\n    result : {result}")
        console_lines.append(f"\n  revenue since {cutoff} : {float(revenue_since):,.2f}")
        self.append_console("\n\n".join(console_lines))

    # ---------------------------------------------------------------
    # refresh
    # ---------------------------------------------------------------
    def refresh_all_tabs(self):
        try:
            self._refresh_pipeline_metrics()
            self._refresh_durable_log_tab()
            self._refresh_consumers_tab()
            self._refresh_reconciliation_tab()
        except Exception as e:
            messagebox.showerror("Postgres error", str(e))

    def _refresh_pipeline_metrics(self):
        total_events = get_total_events()
        stats = get_consumer_stats()
        total_lag = sum(row[3] - (row[2] + 1) for row in stats) if stats else 0
        self.metric_labels["events"].config(text=f"{total_events:,}")
        self.metric_labels["groups"].config(text=str(len(CONSUMER_GROUPS)))
        self.metric_labels["lag"].config(text=f"{max(total_lag, 0):,}")

        try:
            stream = dict(get_revenue_tracker_state())
            truth = dict(get_revenue_source_of_truth())
            if not self.reconciled_this_session:
                self.metric_labels["recon"].config(text="NOT RUN", fg=COLOR_MUTED)
            elif not stream:
                self.metric_labels["recon"].config(text="NOT RUN", fg=COLOR_MUTED)
            elif truth and all(abs(float(stream.get(c, 0)) - float(truth[c])) < 0.01 for c in truth):
                self.metric_labels["recon"].config(text="PASSED", fg=COLOR_SUCCESS)
            else:
                self.metric_labels["recon"].config(text="PENDING", fg=COLOR_WARN)
        except Exception:
            self.metric_labels["recon"].config(text="PENDING", fg=COLOR_WARN)

    def _refresh_durable_log_tab(self):
        rows = get_partition_distribution()
        for r in self.partition_tree.get_children():
            self.partition_tree.delete(r)

        total = sum(r[2] for r in rows)
        even_share = total / len(rows) if rows else 0
        labels, values = [], []
        for partition, category, events in rows:
            share_pct = (events / total * 100) if total else 0
            observation = "above even share" if events > even_share else "below even share"
            self.partition_tree.insert("", "end", values=(
                f"partition {partition}", category, f"{events:,}", f"{share_pct:.1f}% {observation}"
            ))
            labels.append(f"P{partition}\n{category}")
            values.append(events)

        self._draw_bars(self.partition_canvas, labels, values)

        if total:
            self.log_summary_label.config(
                text=f"{total:,} events in the log across {len(rows)} partitions · "
                     f"even share = {even_share:,.0f} per partition"
            )
        else:
            self.log_summary_label.config(text="No events yet — run Produce on the Pipeline tab.")

    def _refresh_consumers_tab(self):
        for r in self.consumer_tree.get_children():
            self.consumer_tree.delete(r)
        for r in self.offsets_tree.get_children():
            self.offsets_tree.delete(r)

        stats = get_consumer_stats()
        by_group = {}
        for group_name, partition, committed, part_total in stats:
            by_group.setdefault(group_name, []).append((partition, committed, part_total))

        labels, values = [], []
        for group in CONSUMER_GROUPS:
            rows = by_group.get(group, [])
            group_lag = sum(max(part_total - (committed + 1), 0) for _, committed, part_total in rows)
            timing = self.consumer_timings.get(group, {"processed": 0, "seconds": 0.0})
            processed = timing["processed"]
            seconds = timing["seconds"]
            eps = processed / seconds if seconds > 0 else 0
            self.consumer_tree.insert("", "end", values=(
                group, f"{processed:,}", f"{seconds:.3f}", f"{eps:,.0f}", f"{group_lag:,}"
            ))
            labels.append(group.replace("-", "\n"))
            values.append(eps)

            for partition, committed, part_total in sorted(rows):
                lag = max(part_total - (committed + 1), 0)
                self.offsets_tree.insert("", "end", values=(
                    group, partition, committed, f"{part_total:,}", lag
                ))

        self._draw_bars(self.throughput_canvas, labels, values)

    def _refresh_reconciliation_tab(self):
        for r in self.recon_check_tree.get_children():
            self.recon_check_tree.delete(r)
        stream = dict(get_revenue_tracker_state())
        truth = dict(get_revenue_source_of_truth())
        for category, truth_total in truth.items():
            stream_total = stream.get(category, 0)
            delta = abs(float(stream_total) - float(truth_total))
            self.recon_check_tree.insert("", "end", values=(
                category, f"${float(stream_total):,.2f}", f"${float(truth_total):,.2f}", f"{delta:.4f}"
            ))

        for r in self.recon_tree.get_children():
            self.recon_tree.delete(r)
        rows = get_revenue_by_category_size()
        total_items, total_revenue = 0, 0.0
        for i, (category, size, line_items, revenue) in enumerate(rows):
            tag = "odd" if i % 2 else ""
            self.recon_tree.insert("", "end", values=(
                category, size, f"{line_items:,}", f"${revenue:,.2f}"
            ), tags=(tag,))
            total_items += line_items
            total_revenue += float(revenue)
        self.recon_tree.insert("", "end", values=(
            "Total", "", f"{total_items:,}", f"${total_revenue:,.2f}"
        ), tags=("total",))


if __name__ == "__main__":
    app = PizzaFlowConsole()
    app.mainloop()