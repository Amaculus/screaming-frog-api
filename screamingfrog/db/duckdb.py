from __future__ import annotations

import json
import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Iterator, Mapping, Optional, Sequence

from screamingfrog.db.packaging import find_project_dir


DEFAULT_DUCKDB_TABLES: tuple[str, ...] = ("APP.URLS", "APP.LINKS", "APP.UNIQUE_URLS")
DEFAULT_DUCKDB_TABS: tuple[str, ...] = (
    "internal_all",
    "all_inlinks",
    "all_outlinks",
    "redirect_chains",
    "canonical_chains",
    "redirect_and_canonical_chains",
)
_FETCH_BATCH_SIZE = 1000


def export_duckdb_from_derby(
    db_path: str,
    duckdb_path: str | Path,
    *,
    tables: Sequence[str] | None = None,
    tabs: Sequence[str] | str | None = None,
    if_exists: str = "auto",
    source_label: str | None = None,
    namespace: str | None = None,
    mapping_path: str | None = None,
    derby_jar: str | None = None,
) -> Path:
    from screamingfrog.backends.derby_backend import DerbyBackend

    backend = DerbyBackend(db_path, mapping_path=mapping_path, derby_jar=derby_jar)
    label = source_label or str(Path(db_path).resolve())
    return export_duckdb_from_backend(
        backend,
        duckdb_path,
        tables=tables,
        tabs=tabs,
        if_exists=if_exists,
        source_label=label,
        source_fingerprint=_source_fingerprint(Path(db_path)),
        namespace=namespace,
    )


def export_duckdb_from_db_id(
    db_id: str,
    duckdb_path: str | Path,
    *,
    tables: Sequence[str] | None = None,
    tabs: Sequence[str] | str | None = None,
    if_exists: str = "auto",
    project_root: str | Path | None = None,
    namespace: str | None = None,
    mapping_path: str | None = None,
    derby_jar: str | None = None,
) -> Path:
    project_dir = find_project_dir(db_id, project_root=project_root)
    return export_duckdb_from_derby(
        str(project_dir),
        duckdb_path,
        tables=tables,
        tabs=tabs,
        if_exists=if_exists,
        source_label=db_id,
        namespace=namespace,
        mapping_path=mapping_path,
        derby_jar=derby_jar,
    )


def ensure_duckdb_cache(
    duckdb_path: str | Path,
    *,
    source_label: str,
    source_fingerprint: str | None,
    namespace: str | None = None,
    if_exists: str = "auto",
) -> Path:
    mode = str(if_exists).strip().lower()
    if mode not in {"replace", "skip", "auto"}:
        raise ValueError("if_exists must be 'replace', 'skip', or 'auto'")

    target = Path(duckdb_path)
    target.parent.mkdir(parents=True, exist_ok=True)
    normalized_namespace = _normalize_namespace(namespace)

    duckdb = _import_duckdb()
    if target.exists():
        try:
            read_only_conn = duckdb.connect(str(target), read_only=True)
        except Exception:
            read_only_conn = None
        else:
            try:
                existing = _get_import_metadata(read_only_conn, namespace=normalized_namespace)
                same_source = bool(
                    existing
                    and existing.get("source_label") == source_label
                    and (
                        source_fingerprint is None
                        or existing.get("source_fingerprint") == source_fingerprint
                    )
                )
                if existing and mode == "skip":
                    return target
                if same_source and mode == "auto":
                    return target
            except Exception:
                pass
            finally:
                read_only_conn.close()

    conn = duckdb.connect(str(target))
    try:
        _ensure_metadata_tables(conn)
        existing = _get_import_metadata(conn, namespace=normalized_namespace)
        same_source = bool(
            existing
            and existing.get("source_label") == source_label
            and (
                source_fingerprint is None
                or existing.get("source_fingerprint") == source_fingerprint
            )
        )
        if existing and mode == "skip":
            return target
        if same_source and mode == "auto":
            return target
        if existing and (mode == "replace" or not same_source):
            _drop_exported_objects(conn, namespace=normalized_namespace)
        _store_export_metadata(
            conn,
            source_label=source_label,
            source_fingerprint=source_fingerprint,
            objects=[],
            namespace=normalized_namespace,
        )
        return target
    finally:
        conn.close()


def _try_syscs_export(backend: Any, conn: Any, relation_name: str, raw_name: str) -> bool:
    """Fast path: export a Derby table via SYSCS_UTIL.SYSCS_EXPORT_TABLE then bulk-load into DuckDB.

    Derby's native export writes directly to disk at I/O speed, completely bypassing
    JDBC row iteration and Java-to-Python value conversion.  DuckDB then reads the CSV
    in a single bulk operation.  For large tables (100K+ rows) this is 20-50x faster
    than the standard _write_relation() path.

    Returns True on success.  Returns False on any failure so the caller falls back to
    the normal JDBC row-iteration path.
    """
    derby_conn = getattr(backend, "_conn", None)
    if derby_conn is None:
        return False

    upper = str(raw_name).strip().upper()
    if "." in upper:
        schema, table = upper.split(".", 1)
    else:
        schema, table = "APP", upper

    tmp_fd, tmp_path = tempfile.mkstemp(suffix=".csv", prefix=f"sf_syscs_{table}_")
    os.close(tmp_fd)
    # SYSCS_EXPORT_TABLE requires the file to NOT already exist on some Derby versions.
    os.unlink(tmp_path)

    try:
        cursor = derby_conn.cursor()
        # Export table to CSV: no column delimiter override (comma), double-quote char delimiter,
        # UTF-8 encoding.  Derby writes one row per line with no header.
        cursor.execute(
            "CALL SYSCS_UTIL.SYSCS_EXPORT_TABLE(?, ?, ?, NULL, NULL, 'UTF-8')",
            [schema, table, tmp_path],
        )

        if not os.path.exists(tmp_path):
            return False
        if os.path.getsize(tmp_path) == 0:
            return False

        # Get column names from Derby metadata (zero-row fetch, no data transfer).
        cursor.execute(f"SELECT * FROM {schema}.{table} WHERE 1=0")
        columns = [desc[0] for desc in cursor.description or []]
        if not columns:
            return False

        # Bulk-load from CSV into DuckDB.  read_csv with header=false + names preserves
        # column names and lets DuckDB infer types (integers, booleans, strings) from data.
        _drop_relation(conn, relation_name)
        names_sql = "[" + ", ".join(f"'{c}'" for c in columns) + "]"
        conn.execute(
            f"""
            CREATE TABLE {relation_name} AS
            SELECT * FROM read_csv(
                '{tmp_path}',
                header      = false,
                names       = {names_sql},
                nullstr     = '',
                quote       = '"',
                delim       = ','
            )
            """
        )
        return True

    except Exception:
        return False

    finally:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass


def _syscs_export_all_app_tables(
    backend: Any,
    conn: Any,
    exported_keys: set[tuple[str, str]],
    exported_objects: list[tuple[str, str, str]],
    *,
    namespace: str | None = None,
) -> None:
    """Discover and SYSCS-export ALL tables in the APP schema to DuckDB.

    Rather than maintaining a hardcoded list of raw tables, this dynamically discovers
    every user table in Derby's APP schema and exports each one via the SYSCS fast path.
    Tables that fail to export (e.g. tables with unsupported column types) are silently
    skipped.  Tables already exported by the explicit raw-table loop are not re-exported.

    This ensures ALL raw data is available in DuckDB for tab computation and crawl.sql()
    queries, regardless of which Screaming Frog version created the database.
    """
    derby_conn = getattr(backend, "_conn", None)
    if derby_conn is None:
        return

    try:
        cursor = derby_conn.cursor()
        cursor.execute(
            "SELECT t.TABLENAME FROM SYS.SYSTABLES t "
            "JOIN SYS.SYSSCHEMAS s ON t.SCHEMAID = s.SCHEMAID "
            "WHERE s.SCHEMANAME = 'APP' AND t.TABLETYPE = 'T' "
            "ORDER BY t.TABLENAME"
        )
        all_tables = [str(row[0]) for row in cursor.fetchall()]
    except Exception:
        return

    normalized_namespace = namespace or ""
    for table_name in all_tables:
        raw_name = f"APP.{table_name}"
        export_name = _normalize_export_name("raw", raw_name)
        if ("raw", export_name) in exported_keys:
            continue
        relation_name = _raw_relation_name(export_name, namespace=normalized_namespace)
        try:
            written = _try_syscs_export(backend, conn, relation_name, raw_name)
            if not written:
                rows = backend.raw(raw_name)
                written = _write_relation(conn, relation_name, rows)
        except Exception:
            written = False
        if written:
            exported_objects.append((export_name, "raw", relation_name))
            exported_keys.add(("raw", export_name))


def _try_duckdb_compute_tab(
    conn: Any,
    backend: Any,
    tab_name: str,
    relation_name: str,
    *,
    namespace: str | None = None,
) -> bool:
    """Fast path: compute a tab from DuckDB-resident raw tables instead of Derby.

    Derby computes all_inlinks/all_outlinks using N+1 correlated subqueries — for each
    row in APP.LINKS it runs 5-10 separate sub-queries back into APP.UNIQUE_URLS and
    APP.URLS. A DuckDB JOIN on the already-extracted raw tables is orders of magnitude
    faster (seconds vs 30-60+ minutes for large crawls).

    Tabs handled:
      all_inlinks / all_outlinks  — JOIN on app.links + app.unique_urls + app.urls
      content_exact_duplicates    — simple SELECT from app.urls (MD5SUM column)
      near_duplicates_report      — on-demand SYSCS export of APP.COSINE_SIMILARITY
                                     then JOIN with app.urls

    Returns True if the tab was computed and stored in DuckDB.
    Returns False on any failure so the caller falls back to Derby backend.get_tab().
    """
    stem = Path(tab_name).stem  # strips .csv suffix: "all_inlinks.csv" -> "all_inlinks"
    links_rn = _raw_relation_name("APP.LINKS", namespace=namespace)
    urls_rn = _raw_relation_name("APP.URLS", namespace=namespace)
    unique_urls_rn = _raw_relation_name("APP.UNIQUE_URLS", namespace=namespace)
    cosine_rn = _raw_relation_name("APP.COSINE_SIMILARITY", namespace=namespace)

    try:
        if stem == "internal_all":
            if not _relation_exists(conn, urls_rn):
                return False
            # internal_all is just APP.URLS exposed as a DuckDB relation.
            # Derby's internal_all tab query fails on crawls that lack optional columns
            # (e.g. VIEWPORT from APP.PAGE_SPEED_API), but this DuckDB path always works
            # because it only reads the columns that actually exist in the raw table.
            # The DuckDBBackend's _INTERNAL_COMMON_FIELD_CANDIDATES handles mapping
            # Derby column names (ENCODED_URL, RESPONSE_CODE) to GUI names (Address, Status Code).
            _drop_relation(conn, relation_name)
            conn.execute(f"CREATE TABLE {relation_name} AS SELECT * FROM {urls_rn}")
            return True

        if stem in ("all_inlinks", "all_outlinks"):
            if not (_relation_exists(conn, links_rn) and _relation_exists(conn, unique_urls_rn)):
                return False
            has_urls = _relation_exists(conn, urls_rn)
            src_join = f"LEFT JOIN {urls_rn} src_u ON src_u.ENCODED_URL = src.ENCODED_URL" if has_urls else ""
            dst_join = f"LEFT JOIN {urls_rn} dst_u ON dst_u.ENCODED_URL = dst.ENCODED_URL" if has_urls else ""
            status_col = "dst_u.RESPONSE_CODE" if has_urls else "NULL"
            size_col   = "dst_u.PAGE_SIZE"     if has_urls else "NULL"
            src_seg    = "COALESCE(CAST(src_u.SEGMENTS AS VARCHAR), '')" if has_urls else "''"
            dst_seg    = "COALESCE(CAST(dst_u.SEGMENTS AS VARCHAR), '')" if has_urls else "''"
            _drop_relation(conn, relation_name)
            conn.execute(f"""
                CREATE TABLE {relation_name} AS
                SELECT
                    src.ENCODED_URL                                 AS "Source",
                    {src_seg}                                       AS "Source Segments",
                    dst.ENCODED_URL                                 AS "Destination",
                    {dst_seg}                                       AS "Destination Segments",
                    CASE l.LINK_TYPE
                        WHEN 1  THEN 'Hyperlinks'
                        WHEN 6  THEN 'Canonicals'
                        WHEN 8  THEN 'Rel Prev'
                        WHEN 10 THEN 'Rel Next'
                        WHEN 12 THEN 'Hreflang (HTTP)'
                        WHEN 13 THEN 'Hreflang'
                        ELSE CAST(l.LINK_TYPE AS VARCHAR)
                    END                                             AS "Type",
                    l.LINK_TEXT                                     AS "Anchor",
                    l.ALT_TEXT                                      AS "Alt Text",
                    COALESCE(LENGTH(l.LINK_TEXT), 0)               AS "Length",
                    {status_col}                                    AS "Status Code",
                    {size_col}                                      AS "Size (Bytes)",
                    l.HREF_LANG                                     AS "hreflang",
                    NOT CAST(COALESCE(l.NOFOLLOW, 0) AS BOOLEAN)  AS "Follow",
                    l.TARGET                                        AS "Target",
                    l.PATH_TYPE                                     AS "Path Type"
                FROM {links_rn} l
                JOIN {unique_urls_rn} src ON src.ID = l.SRC_ID
                JOIN {unique_urls_rn} dst ON dst.ID = l.DST_ID
                {src_join}
                {dst_join}
            """)
            return True

        if stem == "content_exact_duplicates":
            if not _relation_exists(conn, urls_rn):
                return False
            # Only proceed if APP.URLS actually has an MD5SUM column (not all SF versions do)
            if "md5sum" not in {c.lower() for c in _table_columns(conn, "app", "urls")}:
                return False
            _drop_relation(conn, relation_name)
            conn.execute(f"""
                CREATE TABLE {relation_name} AS
                SELECT ENCODED_URL AS "Address", MD5SUM AS "Hash"
                FROM {urls_rn}
                WHERE MD5SUM IS NOT NULL
            """)
            return True

        if stem == "near_duplicates_report":
            if not _relation_exists(conn, urls_rn):
                return False
            # Ensure APP.COSINE_SIMILARITY is in DuckDB (on-demand SYSCS export).
            # This table is computed by SF's near-duplicate analysis and may not exist
            # in all crawls — if it's absent the fast path returns False.
            if not _relation_exists(conn, cosine_rn):
                ok = _try_syscs_export(backend, conn, cosine_rn, "APP.COSINE_SIMILARITY")
                if not ok:
                    try:
                        rows = backend.raw("APP.COSINE_SIMILARITY")
                        ok = _write_relation(conn, cosine_rn, rows)
                    except Exception:
                        ok = False
                if not ok:
                    return False
            _drop_relation(conn, relation_name)
            conn.execute(f"""
                CREATE TABLE {relation_name} AS
                SELECT
                    u.ENCODED_URL  AS "Address",
                    cs.CLOSEST_URL AS "Near Duplicate Address",
                    cs.SCORE       AS "Similarity"
                FROM {cosine_rn} cs
                JOIN {urls_rn} u ON u.ENCODED_URL = cs.ENCODED_URL
            """)
            return True

        if stem in ("response_codes_all", "response_codes"):
            if not _relation_exists(conn, urls_rn):
                return False
            written = _try_duckdb_compute_response_codes(
                conn, backend, relation_name, urls_rn,
                links_rn=links_rn, unique_urls_rn=unique_urls_rn,
            )
            return written

        if stem in ("redirect_chains", "redirect_and_canonical_chains", "canonical_chains"):
            if not (_relation_exists(conn, urls_rn) and _relation_exists(conn, links_rn)
                    and _relation_exists(conn, unique_urls_rn)):
                return False
            written = _try_duckdb_compute_chain_tab(
                conn, relation_name, stem, urls_rn, links_rn, unique_urls_rn,
                namespace=namespace,
            )
            return written

    except Exception:
        return False

    return False


def _try_duckdb_compute_response_codes(
    conn: Any,
    backend: Any,
    relation_name: str,
    urls_rn: str,
    *,
    links_rn: str,
    unique_urls_rn: str,
) -> bool:
    """Build response_codes_all from DuckDB-resident APP.URLS + APP.LINKS.

    Avoids Derby's correlated sub-query against APP.INLINK_COUNTS for Inlinks and
    eliminates the JPype JDBC round-trip overhead for 16K+ page crawls.
    Indexability/Indexability Status expressions are read from the Derby mapping so
    they stay in sync with the SF schema version automatically.

    HTTP redirect destination (Location header) is unavailable because
    HTTP_RESPONSE_HEADER_COLLECTION is a binary Java-serialized BLOB that SYSCS
    exports as NULL. The Redirect URL column is populated for meta refresh only.
    """
    try:
        # Get Indexability / Indexability Status SQL CASE expressions from Derby mapping.
        mapping = getattr(backend, "_mapping", {})
        idx_expr: str = "'Indexable'"
        idx_status_expr: str = "NULL"
        for entry in mapping.get("internal_all.csv", []):
            if entry.get("csv_column") == "Indexability":
                raw = str(entry.get("db_expression") or "")
                if raw:
                    # DuckDB accepts VARCHAR(N) but width is ignored; strip the length
                    # to avoid any edge-case issues with older DuckDB builds.
                    idx_expr = raw.replace("VARCHAR(10)", "VARCHAR").replace(
                        "VARCHAR(255)", "VARCHAR"
                    )
            elif entry.get("csv_column") == "Indexability Status":
                raw = str(entry.get("db_expression") or "")
                if raw:
                    idx_status_expr = raw.replace("VARCHAR(10)", "VARCHAR").replace(
                        "VARCHAR(255)", "VARCHAR"
                    )

        urls_cols = {c.lower() for c in _table_columns(conn, "app", "urls")}

        def _opt(col: str, fallback: str = "NULL") -> str:
            return col if col.lower() in urls_cols else fallback

        response_msg_col   = _opt("RESPONSE_MSG")
        resp_time_col      = _opt("RESPONSE_TIME_MS")
        num_meta_col       = _opt("NUM_METAREFRESH", "0")
        meta_url1_col      = _opt("META_FULL_URL_1")
        is_canon_col       = _opt("IS_CANONICALISED")

        # Redirect URL: only meta refresh is derivable without HTTP headers.
        if "meta_full_url_1" in urls_cols:
            redirect_url_expr = (
                f"CASE WHEN COALESCE({num_meta_col}, 0) > 0 AND {meta_url1_col} IS NOT NULL"
                f" THEN {meta_url1_col} ELSE NULL END"
            )
        else:
            redirect_url_expr = "NULL"

        redirect_type_expr = (
            f"CASE WHEN COALESCE({num_meta_col}, 0) > 0 THEN 'Meta Refresh'"
            f" WHEN RESPONSE_CODE BETWEEN 300 AND 399 THEN 'HTTP Redirect'"
            f" ELSE NULL END"
        )

        # IS_CANONICALISED marks pages that point to a different canonical — Non-Indexable
        # in SF terms. The mapping expression covers robots/noindex/x-robots but omits this
        # flag (it is handled as a separate "supplementary" column in Derby). Wrap the
        # existing CASE to add the IS_CANONICALISED check first.
        if is_canon_col != "NULL":
            indexability_sql = (
                f"CASE WHEN LOWER(CAST({is_canon_col} AS VARCHAR)) IN ('1','true')"
                f" THEN 'Non-Indexable' ELSE ({idx_expr}) END"
            )
            idx_status_sql = (
                f"CASE WHEN LOWER(CAST({is_canon_col} AS VARCHAR)) IN ('1','true')"
                f" THEN 'canonicalised' ELSE ({idx_status_expr}) END"
            )
        else:
            indexability_sql = f"({idx_expr})"
            idx_status_sql = f"({idx_status_expr})"

        # Inlinks: count distinct hyperlink sources from APP.LINKS (LINK_TYPE=1).
        # Using a CTE + LEFT JOIN avoids a correlated sub-query per row.
        has_links = _relation_exists(conn, links_rn) and _relation_exists(conn, unique_urls_rn)

        _drop_relation(conn, relation_name)
        if has_links:
            conn.execute(f"""
                CREATE TABLE {relation_name} AS
                WITH inlinks_cte AS (
                    SELECT d.ENCODED_URL AS dst_url, COUNT(DISTINCT l.SRC_ID) AS cnt
                    FROM {links_rn} l
                    JOIN {unique_urls_rn} d ON l.DST_ID = d.ID
                    WHERE l.LINK_TYPE = 1
                    GROUP BY d.ENCODED_URL
                )
                SELECT
                    u.ENCODED_URL                       AS "Address",
                    u.CONTENT_TYPE                      AS "Content Type",
                    u.RESPONSE_CODE                     AS "Status Code",
                    {response_msg_col}                  AS "Status",
                    {indexability_sql}                  AS "Indexability",
                    {idx_status_sql}                    AS "Indexability Status",
                    COALESCE(il.cnt, 0)                 AS "Inlinks",
                    {resp_time_col}                     AS "Response Time",
                    {redirect_url_expr}                 AS "Redirect URL",
                    {redirect_type_expr}                AS "Redirect Type"
                FROM {urls_rn} u
                LEFT JOIN inlinks_cte il ON il.dst_url = u.ENCODED_URL
            """)
        else:
            conn.execute(f"""
                CREATE TABLE {relation_name} AS
                SELECT
                    u.ENCODED_URL                       AS "Address",
                    u.CONTENT_TYPE                      AS "Content Type",
                    u.RESPONSE_CODE                     AS "Status Code",
                    {response_msg_col}                  AS "Status",
                    {indexability_sql}                  AS "Indexability",
                    {idx_status_sql}                    AS "Indexability Status",
                    NULL                                AS "Inlinks",
                    {resp_time_col}                     AS "Response Time",
                    {redirect_url_expr}                 AS "Redirect URL",
                    {redirect_type_expr}                AS "Redirect Type"
                FROM {urls_rn} u
            """)
        return True
    except Exception:
        return False


_CHAIN_MAX_HOPS = 10


def _try_duckdb_compute_chain_tab(
    conn: Any,
    relation_name: str,
    stem: str,
    urls_rn: str,
    links_rn: str,
    unique_urls_rn: str,
    *,
    namespace: str | None = None,
) -> bool:
    """Build redirect_chains / canonical_chains / redirect_and_canonical_chains from DuckDB.

    Derby's _get_chain_tab() makes one JDBC query per URL in the chain — up to
    N × 4 round-trips for N start URLs. On a 16K-page crawl that is ~7,400 queries
    (~9.8s) and scales linearly with site size.

    This fast path bulk-loads all URL data (1 query) and the canonical link map
    (1 query) into Python dicts, then traverses chains entirely in memory.

    Limitation: HTTP redirect destination URLs are derived from the Location header
    in HTTP_RESPONSE_HEADER_COLLECTION, which is a binary Java-serialized BLOB that
    SYSCS exports as NULL. Redirect chains therefore only contain meta-refresh hops
    and report 0 rows for pure HTTP-redirect-only crawls. Canonical chains are not
    affected — canonical links are stored in APP.LINKS (LINK_TYPE=6) and are available.
    """
    try:
        if stem == "canonical_chains":
            mode = "canonical"
        elif stem == "redirect_chains":
            mode = "redirect"
        else:
            mode = "redirect_and_canonical"

        urls_cols = {c.lower() for c in _table_columns(conn, "app", "urls")}

        def _opt_col(col: str) -> str | None:
            return col if col.lower() in urls_cols else None

        # Build the SELECT column list for bulk URL load.
        select_parts = ["ENCODED_URL", "RESPONSE_CODE"]
        col_idx: dict[str, int] = {"ENCODED_URL": 0, "RESPONSE_CODE": 1}
        for col in ("RESPONSE_MSG", "CONTENT_TYPE", "NUM_METAREFRESH", "META_FULL_URL_1", "META_FULL_URL_2"):
            if _opt_col(col):
                col_idx[col] = len(select_parts)
                select_parts.append(col)

        # 1. Bulk-load all URL data into a dict.
        url_data: dict[str, dict[str, Any]] = {}
        rows_result = conn.execute(f"SELECT {', '.join(select_parts)} FROM {urls_rn}").fetchall()
        for row in rows_result:
            url = row[0]
            d: dict[str, Any] = {
                "response_code": row[1],
                "response_msg": row[col_idx["RESPONSE_MSG"]] if "RESPONSE_MSG" in col_idx else None,
                "content_type": row[col_idx["CONTENT_TYPE"]] if "CONTENT_TYPE" in col_idx else None,
                "num_metarefresh": row[col_idx["NUM_METAREFRESH"]] if "NUM_METAREFRESH" in col_idx else 0,
                "meta_url_1": row[col_idx["META_FULL_URL_1"]] if "META_FULL_URL_1" in col_idx else None,
                "meta_url_2": row[col_idx["META_FULL_URL_2"]] if "META_FULL_URL_2" in col_idx else None,
            }
            url_data[url] = d

        # 2. Bulk-load canonical link map: source -> first canonical target (LINK_TYPE=6).
        canonical_map: dict[str, str] = {}
        can_rows = conn.execute(f"""
            SELECT s.ENCODED_URL, d.ENCODED_URL
            FROM {links_rn} l
            JOIN {unique_urls_rn} s ON l.SRC_ID = s.ID
            JOIN {unique_urls_rn} d ON l.DST_ID = d.ID
            WHERE l.LINK_TYPE = 6
            ORDER BY s.ENCODED_URL, d.ENCODED_URL
        """).fetchall()
        for src, dst in can_rows:
            if src not in canonical_map:
                canonical_map[src] = dst

        # 3. Bulk-load first hyperlink inlink details per destination URL.
        #    Filter to LINK_TYPE=1 (hyperlinks) to match Derby's chain tab Source column.
        inlink_map: dict[str, dict[str, Any]] = {}
        inlink_rows = conn.execute(f"""
            SELECT d.ENCODED_URL, s.ENCODED_URL, l.ALT_TEXT, l.LINK_TEXT, l.ELEMENT_PATH, l.ELEMENT_POSITION
            FROM {links_rn} l
            JOIN {unique_urls_rn} s ON l.SRC_ID = s.ID
            JOIN {unique_urls_rn} d ON l.DST_ID = d.ID
            WHERE l.LINK_TYPE = 1
            ORDER BY d.ENCODED_URL, s.ENCODED_URL
        """).fetchall()
        for dst, src, alt, anchor, path, pos in inlink_rows:
            if dst not in inlink_map:
                inlink_map[dst] = {
                    "Source": src,
                    "Alt Text": alt,
                    "Anchor Text": anchor,
                    "Link Path": path,
                    "Link Position": pos,
                }

        # 4. Try to load Indexability from the already-materialized internal_all tab.
        #    If not available, leave None (downstream filters on Final Indexability are optional).
        indexability_map: dict[str, tuple[Any, Any]] = {}
        internal_rn = _tab_relation_name("internal_all", namespace=namespace)
        if _relation_exists(conn, internal_rn):
            try:
                int_cols_lower = {c.lower() for c in _table_columns(conn, "main", "sf_tab_internal_all")}
                idx_sel = '"Indexability"' if "indexability" in int_cols_lower else "NULL"
                idx_st_sel = '"Indexability Status"' if "indexability status" in int_cols_lower else "NULL"
                # Address column may be named "Address" or "ENCODED_URL" in internal_all
                addr_sel = '"Address"' if "address" in int_cols_lower else "ENCODED_URL"
                idx_rows = conn.execute(
                    f'SELECT {addr_sel}, {idx_sel}, {idx_st_sel} FROM {internal_rn}'
                ).fetchall()
                for url, idx_val, idx_st_val in idx_rows:
                    if url:
                        indexability_map[str(url)] = (idx_val, idx_st_val)
            except Exception:
                pass

        # 5. Build start URLs.
        start_urls: list[str] = []
        seen_starts: set[str] = set()
        if mode in ("redirect", "redirect_and_canonical"):
            for url, d in url_data.items():
                code = d.get("response_code")
                meta = d.get("num_metarefresh") or 0
                if (code is not None and 300 <= code < 400) or meta:
                    if url not in seen_starts:
                        seen_starts.add(url)
                        start_urls.append(url)
        if mode in ("canonical", "redirect_and_canonical"):
            for url in sorted(canonical_map.keys()):
                if url not in seen_starts:
                    seen_starts.add(url)
                    start_urls.append(url)

        # 6. Chain traversal helpers.
        def _safe_int_local(v: Any) -> int | None:
            try:
                return int(v) if v is not None else None
            except (TypeError, ValueError):
                return None

        def resolve_meta_redirect(d: dict[str, Any], base: str) -> str | None:
            meta = _safe_int_local(d.get("num_metarefresh"))
            if not meta:
                return None
            target = d.get("meta_url_1") or d.get("meta_url_2")
            if not target:
                return None
            from urllib.parse import urljoin
            return urljoin(base, str(target).strip())

        def build_chain(
            start_url: str, chain_mode: str
        ) -> tuple[list[dict[str, Any]], list[str], list[str], bool, bool] | None:
            steps: list[dict[str, Any]] = []
            hop_types: list[str] = []
            hop_targets: list[str] = []
            visited: set[str] = set()
            loop = False
            temp_redirect = False
            current = start_url

            # Redirect phase.
            while len(steps) < _CHAIN_MAX_HOPS and chain_mode in ("redirect", "redirect_and_canonical"):
                if current in visited:
                    loop = True
                    break
                visited.add(current)
                d = url_data.get(current)
                if not d:
                    break
                steps.append(d)
                code = _safe_int_local(d.get("response_code"))
                next_url: str | None = None
                hop_type: str | None = None
                # HTTP redirects: Location header unavailable (BLOB is NULL in DuckDB).
                # Meta refresh: can be resolved from META_FULL_URL columns.
                meta_target = resolve_meta_redirect(d, current)
                if meta_target:
                    next_url = meta_target
                    hop_type = "Meta Refresh"
                if not next_url or next_url == current:
                    break
                if code in (302, 303, 307):
                    temp_redirect = True
                hop_types.append(hop_type or "HTTP Redirect")
                hop_targets.append(next_url)
                current = next_url

            # Canonical phase.
            if chain_mode in ("canonical", "redirect_and_canonical"):
                while len(steps) < _CHAIN_MAX_HOPS:
                    if chain_mode == "canonical" and not steps:
                        # For canonical-only mode, seed with URL data even if no redirect hops.
                        d = url_data.get(current)
                        if d:
                            steps.append(d)
                    canon_target = canonical_map.get(current)
                    if not canon_target or canon_target == current:
                        break
                    if canon_target in visited:
                        loop = True
                        break
                    visited.add(current)
                    hop_types.append("Canonical")
                    hop_targets.append(canon_target)
                    current = canon_target
                    d = url_data.get(current)
                    if not d:
                        steps.append({
                            "response_code": None, "response_msg": None,
                            "content_type": None, "num_metarefresh": 0,
                            "meta_url_1": None, "meta_url_2": None,
                        })
                        break
                    steps.append(d)

                if chain_mode == "canonical" and not any(h == "Canonical" for h in hop_types):
                    return None

            if chain_mode == "redirect" and not hop_types:
                return None
            return steps, hop_types, hop_targets, loop, temp_redirect

        def chain_type_for(chain_mode: str, hop_types: list[str]) -> str | None:
            has_redirect = any(t in ("HTTP Redirect", "Meta Refresh") for t in hop_types)
            has_canonical = any(t == "Canonical" for t in hop_types)
            if chain_mode == "canonical":
                return "Canonical" if has_canonical else None
            if chain_mode == "redirect_and_canonical":
                if has_redirect and has_canonical:
                    return "Redirect & Canonical"
                if has_canonical:
                    return "Canonical"
            if has_redirect:
                return "HTTP Redirect" if any(t == "HTTP Redirect" for t in hop_types) else "Meta Refresh"
            return None

        def hop_count_for(chain_mode: str, hop_types: list[str]) -> int:
            if chain_mode == "canonical":
                return sum(1 for h in hop_types if h == "Canonical")
            if chain_mode == "redirect_and_canonical":
                return len(hop_types)
            return sum(1 for h in hop_types if h in ("HTTP Redirect", "Meta Refresh"))

        # 7. Compute chain rows.
        chain_rows: list[dict[str, Any]] = []
        seen_chains: set[str] = set()
        for start_url in start_urls:
            if start_url in seen_chains:
                continue
            seen_chains.add(start_url)
            result = build_chain(start_url, mode)
            if not result:
                continue
            steps, hop_types, hop_targets, loop, temp_redirect = result
            chain_type = chain_type_for(mode, hop_types)
            hop_count = hop_count_for(mode, hop_types)

            row: dict[str, Any] = {
                "Chain Type": chain_type,
                "Number of Redirects": hop_count,
                "Number of Redirects/Canonicals": hop_count,
                "Number of Canonicals": hop_count,
                "Loop": loop,
                "Temp Redirect in Chain": temp_redirect,
                "Address": start_url,
            }
            row.update(inlink_map.get(start_url, {
                "Source": None, "Alt Text": None, "Anchor Text": None,
                "Link Path": None, "Link Position": None,
            }))

            final = steps[-1] if steps else None
            final_url = None
            if hop_targets:
                final_url = hop_targets[-1]
            elif final:
                # canonical_chains: final step is the canonical target
                final_url = canonical_map.get(start_url)
            idx_val, idx_st_val = indexability_map.get(final_url or "", (None, None)) if final_url else (None, None)
            row["Final Address"]            = final_url
            row["Final Content"]            = final.get("content_type") if final else None
            row["Final Status Code"]        = final.get("response_code") if final else None
            row["Final Status"]             = final.get("response_msg") if final else None
            row["Final Indexability"]       = idx_val
            row["Final Indexability Status"] = idx_st_val

            for i in range(1, _CHAIN_MAX_HOPS + 1):
                if i <= len(steps):
                    s = steps[i - 1]
                    row[f"Content {i}"]     = s.get("content_type")
                    row[f"Status Code {i}"] = s.get("response_code")
                    row[f"Status {i}"]      = s.get("response_msg")
                else:
                    row[f"Content {i}"]     = None
                    row[f"Status Code {i}"] = None
                    row[f"Status {i}"]      = None
                if i <= len(hop_targets):
                    row[f"Redirect Type {i}"] = hop_types[i - 1]
                    row[f"Redirect URL {i}"]  = hop_targets[i - 1]
                else:
                    row[f"Redirect Type {i}"] = None
                    row[f"Redirect URL {i}"]  = None

            chain_rows.append(row)

        _drop_relation(conn, relation_name)
        if chain_rows:
            written = _write_relation(conn, relation_name, iter(chain_rows))
            if written:
                return True
        # Even with 0 rows, create an empty table with the correct schema so the export
        # loop marks this tab as written and does NOT fall back to Derby (saves Derby
        # chain traversal time on crawls where headers are NULL and redirect chains are empty).
        hop_cols = []
        for i in range(1, _CHAIN_MAX_HOPS + 1):
            hop_cols += [
                f'"Content {i}" VARCHAR', f'"Status Code {i}" BIGINT', f'"Status {i}" VARCHAR',
                f'"Redirect Type {i}" VARCHAR', f'"Redirect URL {i}" VARCHAR',
            ]
        hop_col_ddl = ", ".join(hop_cols)
        conn.execute(f"""
            CREATE TABLE {relation_name} (
                "Chain Type" VARCHAR,
                "Number of Redirects" BIGINT,
                "Number of Redirects/Canonicals" BIGINT,
                "Number of Canonicals" BIGINT,
                "Loop" BOOLEAN,
                "Temp Redirect in Chain" BOOLEAN,
                "Address" VARCHAR,
                "Source" VARCHAR,
                "Alt Text" VARCHAR,
                "Anchor Text" VARCHAR,
                "Link Path" VARCHAR,
                "Link Position" VARCHAR,
                "Final Address" VARCHAR,
                "Final Content" VARCHAR,
                "Final Status Code" BIGINT,
                "Final Status" VARCHAR,
                "Final Indexability" VARCHAR,
                "Final Indexability Status" VARCHAR,
                {hop_col_ddl}
            )
        """)
        return True

    except Exception:
        return False


def export_duckdb_from_backend(
    backend: Any,
    duckdb_path: str | Path,
    *,
    tables: Sequence[str] | None = None,
    tabs: Sequence[str] | str | None = None,
    if_exists: str = "replace",
    source_label: str | None = None,
    source_fingerprint: str | None = None,
    namespace: str | None = None,
) -> Path:
    mode = str(if_exists).strip().lower()
    if mode not in {"replace", "skip", "auto"}:
        raise ValueError("if_exists must be 'replace', 'skip', or 'auto'")

    relation_tables = DEFAULT_DUCKDB_TABLES if tables is None else tuple(tables)
    materialized_tabs = _resolve_export_tabs(backend, tabs)
    explicit_exports = tables is not None or tabs is not None
    target = Path(duckdb_path)
    target.parent.mkdir(parents=True, exist_ok=True)
    normalized_namespace = _normalize_namespace(namespace)

    duckdb = _import_duckdb()
    conn = duckdb.connect(str(target))
    try:
        _ensure_metadata_tables(conn)
        existing = _get_import_metadata(conn, namespace=normalized_namespace)
        existing_objects = _get_export_objects(conn, namespace=normalized_namespace)
        label = source_label or getattr(getattr(backend, "db_path", None), "name", None) or "crawl"
        fingerprint = source_fingerprint or _infer_source_fingerprint_from_backend(backend)

        same_source = bool(
            existing
            and existing.get("source_label") == label
            and (fingerprint is None or existing.get("source_fingerprint") == fingerprint)
        )
        requested_keys = {
            ("raw", _normalize_export_name("raw", raw_name)) for raw_name in relation_tables
        }
        requested_keys.update(
            ("tab", _normalize_export_name("tab", tab_name)) for tab_name in materialized_tabs
        )
        if existing and mode == "skip":
            return target
        if same_source and mode == "auto":
            if not explicit_exports:
                return target
            available_keys = {(kind, export_name) for export_name, kind, _ in existing_objects}
            if requested_keys.issubset(available_keys):
                return target

        if existing and mode in {"replace", "auto"} and not same_source:
            _drop_exported_objects(conn, namespace=normalized_namespace)
            existing_objects = []

        conn.execute("CREATE SCHEMA IF NOT EXISTS app")
        import time as _time
        _t0 = _time.monotonic()

        exported_objects: list[tuple[str, str, str]] = list(existing_objects)
        exported_keys = {(kind, export_name) for export_name, kind, _ in exported_objects}
        for raw_name in relation_tables:
            export_name = _normalize_export_name("raw", raw_name)
            if ("raw", export_name) in exported_keys:
                continue
            relation_name = _raw_relation_name(export_name, namespace=normalized_namespace)
            _ts = _time.monotonic()
            # Fast path: Derby SYSCS CSV export -> DuckDB bulk CSV import.
            # Falls back to JDBC row iteration when SYSCS is unavailable or fails.
            # Per-table exception handling: a missing optional table must not abort the whole export.
            try:
                written = _try_syscs_export(backend, conn, relation_name, raw_name)
                if not written:
                    rows = backend.raw(raw_name)
                    written = _write_relation(conn, relation_name, rows)
            except Exception:
                written = False
            print(f"  [duckdb] raw {raw_name}: {'OK' if written else 'SKIP'} ({_time.monotonic() - _ts:.1f}s)", flush=True)
            if written:
                exported_objects.append((export_name, "raw", relation_name))
                exported_keys.add(("raw", export_name))

        print(f"  [duckdb] explicit raw tables done ({_time.monotonic() - _t0:.1f}s)", flush=True)

        # Universal export: discover and SYSCS-export ALL remaining APP schema tables.
        # This ensures every raw Derby table is available in DuckDB for tab computation
        # and crawl.sql() queries, regardless of the hardcoded duckdb_tables list.
        _tu = _time.monotonic()
        _syscs_export_all_app_tables(
            backend, conn, exported_keys, exported_objects, namespace=normalized_namespace,
        )
        print(f"  [duckdb] universal raw export done ({_time.monotonic() - _tu:.1f}s, total {_time.monotonic() - _t0:.1f}s)", flush=True)

        for tab_name in materialized_tabs:
            normalized = _normalize_export_name("tab", tab_name)
            if ("tab", normalized) in exported_keys:
                continue
            relation_name = _tab_relation_name(normalized, namespace=normalized_namespace)
            _ts = _time.monotonic()
            # Fast path: compute from DuckDB-resident raw tables (avoids Derby N+1 subqueries).
            # Falls back to Derby backend.get_tab() when raw tables are unavailable or SQL fails.
            written = _try_duckdb_compute_tab(
                conn, backend, normalized, relation_name, namespace=normalized_namespace
            )
            path = "duckdb"
            if not written:
                path = "derby"
                rows = backend.get_tab(normalized)
                written = _write_relation(conn, relation_name, rows)
            print(f"  [duckdb] tab {normalized}: {'OK' if written else 'SKIP'} via {path} ({_time.monotonic() - _ts:.1f}s)", flush=True)
            if written:
                exported_objects.append((normalized, "tab", relation_name))
                exported_keys.add(("tab", normalized))

        print(f"  [duckdb] all exports done ({_time.monotonic() - _t0:.1f}s)", flush=True)

        _store_export_metadata(
            conn,
            source_label=str(label),
            source_fingerprint=fingerprint,
            objects=exported_objects,
            namespace=normalized_namespace,
        )
        return target
    finally:
        conn.close()


def iter_relation_rows(conn: Any, relation_name: str) -> Iterator[dict[str, Any]]:
    cursor = conn.execute(f"SELECT * FROM {relation_name}")
    columns = [desc[0] for desc in cursor.description or []]
    for row in iter_cursor_rows(cursor):
        yield {col: val for col, val in zip(columns, row)}


def list_exported_tabs(conn: Any) -> list[str]:
    return list_exported_tabs_for_namespace(conn, namespace="")


def list_exported_tabs_for_namespace(conn: Any, namespace: str | None = None) -> list[str]:
    normalized_namespace = _normalize_namespace(namespace)
    if _table_has_column(conn, "main", "sf_alpha_exports", "namespace"):
        cursor = conn.execute(
            """
            SELECT export_name
            FROM sf_alpha_exports
            WHERE kind = 'tab' AND COALESCE(namespace, '') = ?
            ORDER BY export_name
            """,
            [normalized_namespace],
        )
    else:
        if normalized_namespace:
            return []
        cursor = conn.execute(
            "SELECT export_name FROM sf_alpha_exports WHERE kind = 'tab' ORDER BY export_name"
        )
    return [str(row[0]) for row in cursor.fetchall()]


def list_duckdb_namespaces(path: str | Path) -> list[str]:
    duckdb = _import_duckdb()
    conn = duckdb.connect(str(path), read_only=True)
    try:
        return _list_namespaces(conn)
    finally:
        conn.close()


def resolve_relation_name(
    conn: Any, kind: str, export_name: str, *, namespace: str | None = None
) -> str | None:
    normalized_namespace = _normalize_namespace(namespace)
    normalized = export_name if kind == "raw" else _normalize_tab_name(export_name)
    if _table_has_column(conn, "main", "sf_alpha_exports", "namespace"):
        cursor = conn.execute(
            """
            SELECT relation_name
            FROM sf_alpha_exports
            WHERE kind = ? AND export_name = ? AND COALESCE(namespace, '') = ?
            LIMIT 1
            """,
            [kind, normalized.upper() if kind == "raw" else normalized, normalized_namespace],
        )
    else:
        if normalized_namespace:
            return None
        cursor = conn.execute(
            "SELECT relation_name FROM sf_alpha_exports WHERE kind = ? AND export_name = ? LIMIT 1",
            [kind, normalized.upper() if kind == "raw" else normalized],
        )
    row = cursor.fetchone()
    if row:
        return str(row[0])
    if kind == "raw":
        candidate = _raw_relation_name(export_name, namespace=normalized_namespace)
        return candidate if _relation_exists(conn, candidate) else None
    candidate = _tab_relation_name(normalized, namespace=normalized_namespace)
    return candidate if _relation_exists(conn, candidate) else None


def _write_relation(conn: Any, relation_name: str, rows: Iterable[Mapping[str, Any]]) -> bool:
    iterator = (_normalize_export_row(row) for row in rows)
    buffered: list[Mapping[str, Any]] = []
    for _ in range(200):
        try:
            buffered.append(next(iterator))
        except StopIteration:
            break
    if not buffered:
        return False

    columns = _ordered_columns(buffered)
    type_map = _infer_duckdb_types(buffered, columns)
    _drop_relation(conn, relation_name)
    _create_relation(conn, relation_name, columns, type_map)
    _insert_rows(conn, relation_name, columns, buffered)
    _insert_rows(conn, relation_name, columns, iterator)
    return True


def _normalize_export_row(row: Mapping[str, Any]) -> dict[str, Any]:
    normalized: dict[str, Any] = {}
    canonical_keys: dict[str, str] = {}
    for key, value in row.items():
        text = str(key)
        folded = text.casefold()
        existing_key = canonical_keys.get(folded)
        if existing_key is None:
            canonical_keys[folded] = text
            normalized[text] = value
            continue
        if normalized.get(existing_key) is None and value is not None:
            normalized[existing_key] = value
    return normalized


def _ordered_columns(rows: Sequence[Mapping[str, Any]]) -> list[str]:
    seen: set[str] = set()
    columns: list[str] = []
    for row in rows:
        for key in row.keys():
            text = str(key)
            if text in seen:
                continue
            seen.add(text)
            columns.append(text)
    return columns


def _infer_duckdb_types(rows: Sequence[Mapping[str, Any]], columns: Sequence[str]) -> dict[str, str]:
    inferred: dict[str, str] = {}
    precedence = {"BOOLEAN": 0, "BIGINT": 1, "DOUBLE": 2, "TIMESTAMP": 3, "BLOB": 4, "VARCHAR": 5}
    for col in columns:
        best: str | None = None
        for row in rows:
            value = row.get(col)
            current = _duckdb_type_for_value(value)
            if best is None or precedence[current] > precedence[best]:
                best = current
        inferred[col] = best or "VARCHAR"
    return inferred


def _duckdb_type_for_value(value: Any) -> str:
    if value is None:
        return "VARCHAR"
    if isinstance(value, bool):
        return "BOOLEAN"
    if isinstance(value, int):
        return "BIGINT"
    if isinstance(value, float):
        return "DOUBLE"
    if isinstance(value, datetime):
        return "TIMESTAMP"
    if isinstance(value, (bytes, bytearray, memoryview)):
        return "BLOB"
    return "VARCHAR"


def _create_relation(conn: Any, relation_name: str, columns: Sequence[str], type_map: Mapping[str, str]) -> None:
    column_sql = ", ".join(
        f'{_quote_identifier(column)} {type_map.get(column, "VARCHAR")}' for column in columns
    )
    conn.execute(f"CREATE TABLE {relation_name} ({column_sql})")


def _insert_rows(
    conn: Any,
    relation_name: str,
    columns: Sequence[str],
    rows: Iterable[Mapping[str, Any]],
) -> None:
    placeholders = ", ".join("?" for _ in columns)
    quoted_columns = ", ".join(_quote_identifier(column) for column in columns)
    sql = f"INSERT INTO {relation_name} ({quoted_columns}) VALUES ({placeholders})"
    batch: list[tuple[Any, ...]] = []
    for row in rows:
        batch.append(tuple(_convert_duckdb_value(row.get(column)) for column in columns))
        if len(batch) >= 1000:
            conn.executemany(sql, batch)
            batch.clear()
    if batch:
        conn.executemany(sql, batch)


def _convert_duckdb_value(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, memoryview):
        return bytes(value)
    if isinstance(value, bytearray):
        return bytes(value)
    derby_blob = _derby_blob_bytes(value)
    if derby_blob is not None:
        return derby_blob
    derby_clob = _derby_clob_text(value)
    if derby_clob is not None:
        return derby_clob
    java_scalar = _java_scalar_value(value)
    if java_scalar is not None:
        return java_scalar
    if isinstance(value, (dict, list, tuple, set)):
        return json.dumps(value, ensure_ascii=False)
    if isinstance(value, Path):
        return str(value)
    java_fallback = _java_object_fallback(value)
    if java_fallback is not None:
        return java_fallback
    return value


def _ensure_metadata_tables(conn: Any) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS sf_alpha_imports (
            namespace VARCHAR,
            source_label VARCHAR,
            source_fingerprint VARCHAR,
            imported_at TIMESTAMP
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS sf_alpha_exports (
            namespace VARCHAR,
            export_name VARCHAR,
            kind VARCHAR,
            relation_name VARCHAR
        )
        """
    )
    columns = _table_columns(conn, "main", "sf_alpha_imports")
    if "namespace" not in {column.lower() for column in columns}:
        conn.execute("ALTER TABLE sf_alpha_imports ADD COLUMN namespace VARCHAR")
    if "source_fingerprint" not in {column.lower() for column in columns}:
        conn.execute("ALTER TABLE sf_alpha_imports ADD COLUMN source_fingerprint VARCHAR")
    export_columns = _table_columns(conn, "main", "sf_alpha_exports")
    if "namespace" not in {column.lower() for column in export_columns}:
        conn.execute("ALTER TABLE sf_alpha_exports ADD COLUMN namespace VARCHAR")
    conn.execute("UPDATE sf_alpha_imports SET namespace = '' WHERE namespace IS NULL")
    conn.execute("UPDATE sf_alpha_exports SET namespace = '' WHERE namespace IS NULL")


def _get_import_metadata(conn: Any, *, namespace: str | None = None) -> dict[str, Any] | None:
    normalized_namespace = _normalize_namespace(namespace)
    columns = _table_columns(conn, "main", "sf_alpha_imports")
    has_namespace = "namespace" in {column.lower() for column in columns}
    has_fingerprint = "source_fingerprint" in {column.lower() for column in columns}
    if has_namespace:
        select_sql = (
            """
            SELECT source_label, source_fingerprint, imported_at
            FROM sf_alpha_imports
            WHERE COALESCE(namespace, '') = ?
            LIMIT 1
            """
            if has_fingerprint
            else """
            SELECT source_label, imported_at
            FROM sf_alpha_imports
            WHERE COALESCE(namespace, '') = ?
            LIMIT 1
            """
        )
        cursor = conn.execute(select_sql, [normalized_namespace])
    else:
        if normalized_namespace:
            return None
        select_sql = (
            "SELECT source_label, source_fingerprint, imported_at FROM sf_alpha_imports LIMIT 1"
            if has_fingerprint
            else "SELECT source_label, imported_at FROM sf_alpha_imports LIMIT 1"
        )
        cursor = conn.execute(select_sql)
    row = cursor.fetchone()
    if not row:
        return None
    if has_fingerprint:
        return {
            "namespace": normalized_namespace,
            "source_label": row[0],
            "source_fingerprint": row[1],
            "imported_at": row[2],
        }
    return {
        "namespace": normalized_namespace,
        "source_label": row[0],
        "source_fingerprint": None,
        "imported_at": row[1],
    }


def _get_export_objects(conn: Any, *, namespace: str | None = None) -> list[tuple[str, str, str]]:
    normalized_namespace = _normalize_namespace(namespace)
    if _table_has_column(conn, "main", "sf_alpha_exports", "namespace"):
        cursor = conn.execute(
            """
            SELECT export_name, kind, relation_name
            FROM sf_alpha_exports
            WHERE COALESCE(namespace, '') = ?
            """,
            [normalized_namespace],
        )
    else:
        if normalized_namespace:
            return []
        cursor = conn.execute("SELECT export_name, kind, relation_name FROM sf_alpha_exports")
    rows: list[tuple[str, str, str]] = []
    seen: set[tuple[str, str]] = set()
    for raw_export_name, raw_kind, raw_relation_name in cursor.fetchall():
        kind = str(raw_kind).strip().lower()
        if kind not in {"raw", "tab"}:
            continue
        export_name = _normalize_export_name(kind, raw_export_name)
        relation_name = str(raw_relation_name)
        if not _relation_exists(conn, relation_name):
            continue
        key = (kind, export_name)
        if key in seen:
            continue
        seen.add(key)
        rows.append((export_name, kind, relation_name))
    return rows


def _drop_exported_objects(conn: Any, *, namespace: str | None = None) -> None:
    normalized_namespace = _normalize_namespace(namespace)
    if _table_has_column(conn, "main", "sf_alpha_exports", "namespace"):
        cursor = conn.execute(
            "SELECT relation_name FROM sf_alpha_exports WHERE COALESCE(namespace, '') = ?",
            [normalized_namespace],
        )
    else:
        if normalized_namespace:
            return
        cursor = conn.execute("SELECT relation_name FROM sf_alpha_exports")
    relations = [str(row[0]) for row in cursor.fetchall()]
    relations.extend(_list_helper_relations(conn, namespace=normalized_namespace))
    seen: set[str] = set()
    for relation in relations:
        if relation in seen:
            continue
        seen.add(relation)
        _drop_relation(conn, relation)
    if _table_has_column(conn, "main", "sf_alpha_exports", "namespace"):
        conn.execute("DELETE FROM sf_alpha_exports WHERE COALESCE(namespace, '') = ?", [normalized_namespace])
        conn.execute("DELETE FROM sf_alpha_imports WHERE COALESCE(namespace, '') = ?", [normalized_namespace])
    else:
        conn.execute("DELETE FROM sf_alpha_exports")
        conn.execute("DELETE FROM sf_alpha_imports")


def _store_export_metadata(
    conn: Any,
    *,
    source_label: str,
    source_fingerprint: str | None,
    objects: Sequence[tuple[str, str, str]],
    namespace: str | None = None,
) -> None:
    normalized_namespace = _normalize_namespace(namespace)
    conn.execute(
        "DELETE FROM sf_alpha_exports WHERE COALESCE(namespace, '') = ?",
        [normalized_namespace],
    )
    conn.execute(
        "DELETE FROM sf_alpha_imports WHERE COALESCE(namespace, '') = ?",
        [normalized_namespace],
    )
    conn.execute(
        """
        INSERT INTO sf_alpha_imports (namespace, source_label, source_fingerprint, imported_at)
        VALUES (?, ?, ?, ?)
        """,
        [normalized_namespace, source_label, source_fingerprint, datetime.now(timezone.utc)],
    )
    rows = list(objects)
    if not rows:
        return
    conn.executemany(
        """
        INSERT INTO sf_alpha_exports (namespace, export_name, kind, relation_name)
        VALUES (?, ?, ?, ?)
        """,
        [(normalized_namespace, export_name, kind, relation_name) for export_name, kind, relation_name in rows],
    )


def _drop_relation(conn: Any, relation_name: str) -> None:
    conn.execute(f"DROP TABLE IF EXISTS {relation_name}")


def _relation_exists(conn: Any, relation_name: str) -> bool:
    schema_name, _, table_name = relation_name.partition(".")
    if not table_name:
        schema_name, table_name = "main", schema_name
    cursor = conn.execute(
        """
        SELECT 1
        FROM information_schema.tables
        WHERE lower(table_schema) = lower(?)
          AND lower(table_name) = lower(?)
        LIMIT 1
        """,
        [schema_name, table_name],
    )
    return cursor.fetchone() is not None


def _table_columns(conn: Any, schema_name: str, table_name: str) -> list[str]:
    cursor = conn.execute(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE lower(table_schema) = lower(?)
          AND lower(table_name) = lower(?)
        ORDER BY ordinal_position
        """,
        [schema_name, table_name],
    )
    return [str(row[0]) for row in cursor.fetchall()]


def _list_helper_relations(conn: Any, *, namespace: str | None = None) -> list[str]:
    normalized_namespace = _normalize_namespace(namespace)
    helper_prefix = _helper_relation_prefix(normalized_namespace)
    cursor = conn.execute(
        """
        SELECT table_schema, table_name
        FROM information_schema.tables
        WHERE lower(table_schema) = 'main'
          AND lower(table_name) LIKE ?
        """,
        [helper_prefix.lower().replace(".", "") + "%"],
    )
    return [f"{row[0]}.{row[1]}" for row in cursor.fetchall()]


def _raw_relation_name(raw_name: str, *, namespace: str | None = None) -> str:
    normalized_namespace = _normalize_namespace(namespace)
    upper = str(raw_name).strip().upper()
    if "." in upper:
        schema_name, table_name = upper.split(".", 1)
    else:
        schema_name, table_name = "APP", upper
    if normalized_namespace:
        safe_namespace = _safe_namespace_component(normalized_namespace)
        safe_table = f"{schema_name.lower()}_{table_name.lower()}"
        return f"main.sf_{safe_namespace}_raw_{safe_table}"
    return f"{schema_name.lower()}.{table_name.lower()}"


def _normalize_tab_name(tab_name: str) -> str:
    name = str(tab_name).strip()
    if not name.lower().endswith(".csv"):
        name = f"{name}.csv"
    return name.lower()


def _normalize_export_name(kind: str, export_name: Any) -> str:
    if str(kind).strip().lower() == "raw":
        return str(export_name).strip().upper()
    return _normalize_tab_name(str(export_name))


def _tab_relation_name(tab_name: str, *, namespace: str | None = None) -> str:
    stem = Path(tab_name).stem
    safe = "".join(ch if ch.isalnum() else "_" for ch in stem)
    normalized_namespace = _normalize_namespace(namespace)
    if normalized_namespace:
        return f"main.sf_{_safe_namespace_component(normalized_namespace)}_tab_{safe}"
    return f"main.sf_tab_{safe}"


def _helper_relation_name(helper_name: str, *, namespace: str | None = None) -> str:
    safe = "".join(ch if ch.isalnum() else "_" for ch in str(helper_name).strip().lower())
    normalized_namespace = _normalize_namespace(namespace)
    if normalized_namespace:
        return f"main.sf_{_safe_namespace_component(normalized_namespace)}_helper_{safe}"
    return f"main.sf_helper_{safe}"


def _helper_relation_prefix(namespace: str | None) -> str:
    normalized_namespace = _normalize_namespace(namespace)
    if normalized_namespace:
        return f"sf_{_safe_namespace_component(normalized_namespace)}_helper_"
    return "sf_helper_"


def _safe_namespace_component(namespace: str) -> str:
    safe = "".join(ch if ch.isalnum() else "_" for ch in str(namespace).strip().lower())
    return safe or "default"


def _normalize_namespace(namespace: str | None) -> str:
    return str(namespace or "").strip().lower()


def _table_has_column(conn: Any, schema_name: str, table_name: str, column_name: str) -> bool:
    return str(column_name).lower() in {column.lower() for column in _table_columns(conn, schema_name, table_name)}


def _list_namespaces(conn: Any) -> list[str]:
    if not _relation_exists(conn, "main.sf_alpha_imports"):
        return [""]
    if not _table_has_column(conn, "main", "sf_alpha_imports", "namespace"):
        return [""]
    cursor = conn.execute(
        "SELECT DISTINCT COALESCE(namespace, '') FROM sf_alpha_imports ORDER BY COALESCE(namespace, '')"
    )
    return [str(row[0] or "") for row in cursor.fetchall()]


def iter_cursor_rows(cursor: Any, batch_size: int = _FETCH_BATCH_SIZE) -> Iterator[tuple[Any, ...]]:
    fetchmany = getattr(cursor, "fetchmany", None)
    if not callable(fetchmany):
        for row in cursor.fetchall():
            yield row
        return
    while True:
        rows = fetchmany(batch_size)
        if not rows:
            break
        for row in rows:
            yield row


def _derby_blob_bytes(value: Any) -> bytes | None:
    get_bytes = getattr(value, "getBytes", None)
    length_fn = getattr(value, "length", None)
    if not callable(get_bytes) or not callable(length_fn):
        return None
    try:
        length = int(length_fn())
    except Exception:
        return None
    try:
        return bytes(get_bytes(1, length))
    except Exception:
        return None


def _derby_clob_text(value: Any) -> str | None:
    get_substring = getattr(value, "getSubString", None)
    length_fn = getattr(value, "length", None)
    if not callable(get_substring) or not callable(length_fn):
        return None
    try:
        length = int(length_fn())
    except Exception:
        return None
    try:
        return str(get_substring(1, length))
    except Exception:
        return None


def _java_scalar_value(value: Any) -> Any:
    class_name = _java_class_name(value)
    if class_name is None:
        return None
    if class_name == "java.lang.Boolean":
        boolean_value = getattr(value, "booleanValue", None)
        if callable(boolean_value):
            try:
                return bool(boolean_value())
            except Exception:
                return None
        return None
    if class_name in {
        "java.lang.Integer",
        "java.lang.Long",
        "java.lang.Short",
        "java.lang.Byte",
        "java.math.BigInteger",
    }:
        try:
            return int(value)
        except Exception:
            return None
    if class_name in {
        "java.lang.Double",
        "java.lang.Float",
        "java.math.BigDecimal",
    }:
        try:
            return float(value)
        except Exception:
            return None
    if class_name in {"java.lang.String", "java.lang.Character"}:
        try:
            return str(value)
        except Exception:
            return None
    return None


def _java_object_fallback(value: Any) -> str | None:
    class_name = _java_class_name(value)
    if class_name is None:
        return None
    try:
        return str(value)
    except Exception:
        return None


def _java_class_name(value: Any) -> str | None:
    class_name = getattr(value, "__sf_java_class_name__", None)
    if isinstance(class_name, str) and class_name:
        return class_name
    type_text = str(type(value))
    prefix = "<java class '"
    suffix = "'>"
    if type_text.startswith(prefix) and type_text.endswith(suffix):
        return type_text[len(prefix) : -len(suffix)]
    return None


def _quote_identifier(value: str) -> str:
    return '"' + str(value).replace('"', '""') + '"'


def _resolve_export_tabs(backend: Any, tabs: Sequence[str] | str | None) -> tuple[str, ...]:
    if tabs is None:
        return DEFAULT_DUCKDB_TABS
    if isinstance(tabs, str):
        if tabs.strip().lower() != "all":
            return (_normalize_tab_name(tabs),)
        return tuple(_normalize_tab_name(name) for name in backend.list_tabs())
    return tuple(_normalize_tab_name(name) for name in tabs)


def _import_duckdb():
    try:
        import duckdb
    except ModuleNotFoundError as exc:  # pragma: no cover - dependency error path
        raise ImportError("duckdb is required for DuckDB export support.") from exc
    return duckdb


def _infer_source_fingerprint_from_backend(backend: Any) -> str | None:
    db_path = getattr(backend, "db_path", None)
    if db_path is None:
        return None
    try:
        return _source_fingerprint(Path(db_path))
    except Exception:
        return None


def _source_fingerprint(path: Path) -> str:
    target = path.resolve()
    if target.is_file():
        stat = target.stat()
        return f"file:{stat.st_size}:{stat.st_mtime_ns}"

    latest_mtime = int(target.stat().st_mtime_ns)
    total_size = 0
    file_count = 0
    for child in target.rglob("*"):
        if not child.is_file():
            continue
        stat = child.stat()
        file_count += 1
        total_size += int(stat.st_size)
        latest_mtime = max(latest_mtime, int(stat.st_mtime_ns))
    return f"dir:{file_count}:{total_size}:{latest_mtime}"

