"""
Measure the statistics of the JOB (Join Order Benchmark) catalog on your IMDB data.

The instantiator generates JOB jobs from instantiator/src/main/resources/job/job_catalog.json.
Selectivities, distinct counts and row counts in that file depend on the IMDB snapshot, so they
ship as null. This script reads the 21 IMDB CSV files, evaluates every catalog predicate with
exactly the semantics of the generated Wayang code (same CSV parsing, same NULL handling) and
writes the measured values back into the catalog.

Usage:
    python JOB_MetaDataAnalyzer.py /path/to/imdb_csv_dir [--catalog PATH] [--output PATH] [--tables t1,t2]

Only the Python standard library is needed. One pass per table; cast_info (~36M rows) takes a while.
"""
import argparse
import datetime
import json
import os
import re
import sys

DEFAULT_CATALOG = os.path.normpath(os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "..", "..", "instantiator", "src", "main", "resources", "job",
    "job_catalog.json"))

# Columns in file order, as in instantiator/.../job/JOBSchema.scala (keep both in sync).
# (name, is_int, nullable)
I, T = True, False
SCHEMA = {
    "aka_name": [("id", I, 0), ("person_id", I, 0), ("name", T, 0), ("imdb_index", T, 1), ("name_pcode_cf", T, 1),
                 ("name_pcode_nf", T, 1), ("surname_pcode", T, 1), ("md5sum", T, 1)],
    "aka_title": [("id", I, 0), ("movie_id", I, 0), ("title", T, 0), ("imdb_index", T, 1), ("kind_id", I, 0),
                  ("production_year", I, 1), ("phonetic_code", T, 1), ("episode_of_id", I, 1), ("season_nr", I, 1),
                  ("episode_nr", I, 1), ("note", T, 1), ("md5sum", T, 1)],
    "cast_info": [("id", I, 0), ("person_id", I, 0), ("movie_id", I, 0), ("person_role_id", I, 1), ("note", T, 1),
                  ("nr_order", I, 1), ("role_id", I, 0)],
    "char_name": [("id", I, 0), ("name", T, 0), ("imdb_index", T, 1), ("imdb_id", I, 1), ("name_pcode_nf", T, 1),
                  ("surname_pcode", T, 1), ("md5sum", T, 1)],
    "comp_cast_type": [("id", I, 0), ("kind", T, 0)],
    "company_name": [("id", I, 0), ("name", T, 0), ("country_code", T, 1), ("imdb_id", I, 1), ("name_pcode_nf", T, 1),
                     ("name_pcode_sf", T, 1), ("md5sum", T, 1)],
    "company_type": [("id", I, 0), ("kind", T, 0)],
    "complete_cast": [("id", I, 0), ("movie_id", I, 1), ("subject_id", I, 0), ("status_id", I, 0)],
    "info_type": [("id", I, 0), ("info", T, 0)],
    "keyword": [("id", I, 0), ("keyword", T, 0), ("phonetic_code", T, 1)],
    "kind_type": [("id", I, 0), ("kind", T, 0)],
    "link_type": [("id", I, 0), ("link", T, 0)],
    "movie_companies": [("id", I, 0), ("movie_id", I, 0), ("company_id", I, 0), ("company_type_id", I, 0),
                        ("note", T, 1)],
    "movie_info": [("id", I, 0), ("movie_id", I, 0), ("info_type_id", I, 0), ("info", T, 0), ("note", T, 1)],
    "movie_info_idx": [("id", I, 0), ("movie_id", I, 0), ("info_type_id", I, 0), ("info", T, 0), ("note", T, 1)],
    "movie_keyword": [("id", I, 0), ("movie_id", I, 0), ("keyword_id", I, 0)],
    "movie_link": [("id", I, 0), ("movie_id", I, 0), ("linked_movie_id", I, 0), ("link_type_id", I, 0)],
    "name": [("id", I, 0), ("name", T, 0), ("imdb_index", T, 1), ("imdb_id", I, 1), ("gender", T, 1),
             ("name_pcode_cf", T, 1), ("name_pcode_nf", T, 1), ("surname_pcode", T, 1), ("md5sum", T, 1)],
    "person_info": [("id", I, 0), ("person_id", I, 0), ("info_type_id", I, 0), ("info", T, 0), ("note", T, 1)],
    "role_type": [("id", I, 0), ("role", T, 0)],
    "title": [("id", I, 0), ("title", T, 0), ("imdb_index", T, 1), ("kind_id", I, 0), ("production_year", I, 1),
              ("imdb_id", I, 1), ("phonetic_code", T, 1), ("episode_of_id", I, 1), ("season_nr", I, 1),
              ("episode_nr", I, 1), ("series_years", T, 1), ("md5sum", T, 1)],
}

# ---- same semantics as the generated <Job>Parsers object -------------------------------------

NULL_INT = -(2 ** 63)  # Long.MinValue
_INT_RE = re.compile(r"[+-]?[0-9]+\Z")


def _java_trim(s):
    # String.trim removes chars <= ' ' at both ends
    i, j = 0, len(s)
    while i < j and s[i] <= " ":
        i += 1
    while j > i and s[j - 1] <= " ":
        j -= 1
    return s[i:j]


def parse_int(s):
    if s is None or s == "":
        return NULL_INT
    t = _java_trim(s)
    if not _INT_RE.match(t):
        return NULL_INT
    v = int(t)
    return v if -(2 ** 63) <= v < 2 ** 63 else NULL_INT


def split(line, n):
    """CSV line -> exactly n fields; None = NULL (empty unquoted field or missing trailing field)."""
    if '"' not in line:
        parts = line.split(",", n)[:n]
        out = [p if p != "" else None for p in parts]
        return out + [None] * (n - len(out))
    out = [None] * n
    length = len(line)
    i = col = 0
    while col < n and i <= length:
        if i < length and line[i] == '"':
            buf = []
            i += 1
            open_ = True
            while open_ and i < length:
                c = line[i]
                if c == "\\" and i + 1 < length:
                    buf.append(line[i + 1]); i += 2
                elif c == '"' and i + 1 < length and line[i + 1] == '"':
                    buf.append('"'); i += 2
                elif c == '"':
                    open_ = False; i += 1
                else:
                    buf.append(c); i += 1
            while i < length and line[i] != ",":
                i += 1
            out[col] = "".join(buf)
        else:
            j = line.find(",", i)
            end = length if j < 0 else j
            out[col] = None if end == i else line[i:end]
            i = end
        col += 1
        i += 1
    return out


def like(s, p):
    """SQL LIKE with '%' and '_' (catalog patterns only use '%')."""
    si = pi = 0
    star_p, star_s = -1, 0
    while si < len(s):
        if pi < len(p) and p[pi] == "%":
            star_p, star_s = pi, si
            pi += 1
        elif pi < len(p) and (p[pi] == "_" or p[pi] == s[si]):
            si += 1; pi += 1
        elif star_p >= 0:
            pi = star_p + 1; star_s += 1; si = star_s
        else:
            return False
    while pi < len(p) and p[pi] == "%":
        pi += 1
    return pi == len(p)


def _u16(s):
    # Java compares strings by UTF-16 code units
    return s.encode("utf-16-be", "surrogatepass")


def make_predicate(f, is_int):
    op = f["op"].lower()
    v = f.get("value")
    vals = [] if v is None else (v if isinstance(v, list) else [v])
    if is_int:
        vals = [int(str(x).strip()) for x in vals]
        iv = set(vals)
        nn = lambda x: x != NULL_INT
        ops = {
            "=": lambda x: nn(x) and x == vals[0], "!=": lambda x: nn(x) and x != vals[0],
            "<": lambda x: nn(x) and x < vals[0], "<=": lambda x: nn(x) and x <= vals[0],
            ">": lambda x: nn(x) and x > vals[0], ">=": lambda x: nn(x) and x >= vals[0],
            "between": lambda x: nn(x) and vals[0] <= x <= vals[1],
            "in": lambda x: x in iv,
            "mod": lambda x: nn(x) and _java_mod(x, vals[0]) == vals[1],
            "is null": lambda x: x == NULL_INT, "is not null": lambda x: x != NULL_INT,
        }
    else:
        vals = [str(x) for x in vals]
        k = [_u16(x) for x in vals]
        sv = set(vals)
        ops = {
            "=": lambda x: x is not None and x == vals[0], "!=": lambda x: x is not None and x != vals[0],
            "<": lambda x: x is not None and _u16(x) < k[0], "<=": lambda x: x is not None and _u16(x) <= k[0],
            ">": lambda x: x is not None and _u16(x) > k[0], ">=": lambda x: x is not None and _u16(x) >= k[0],
            "between": lambda x: x is not None and k[0] <= _u16(x) <= k[1],
            "in": lambda x: x in sv,
            "like": lambda x: x is not None and like(x, vals[0]),
            "not like": lambda x: x is not None and not like(x, vals[0]),
            "is null": lambda x: x is None, "is not null": lambda x: x is not None,
        }
    if op not in ops:
        raise ValueError(f"operator '{op}' not supported for {'integer' if is_int else 'text'} column")
    return ops[op]


def _java_mod(x, m):
    # Java/Scala % truncates towards zero
    r = abs(x) % abs(m)
    return -r if x < 0 else r


# ---- analysis ------------------------------------------------------------------------------

def analyze_table(path, table, entry):
    cols = SCHEMA[table]
    n = len(cols)
    idx = {c[0]: i for i, c in enumerate(cols)}
    preds = []
    for f in entry.get("filters", []):
        i = idx[f["field"]]
        preds.append((i, cols[i][1], make_predicate(f, cols[i][1])))
    groups = []
    for g in entry.get("distinct", {}):
        i = idx[g]
        is_int, nullable = cols[i][1], cols[i][2]
        groups.append((i, is_int, nullable, set()))

    rows = 0
    hits = [0] * len(preds)
    # universal newlines: \n, \r and \r\n end a line, like Java's BufferedReader in Wayang's text source
    with open(path, encoding="utf-8", errors="replace", newline=None) as fh:
        for line in fh:
            if line.endswith("\n"):
                line = line[:-1]
            raw = split(line, n)
            rows += 1
            for k, (i, is_int, pred) in enumerate(preds):
                if pred(parse_int(raw[i]) if is_int else raw[i]):
                    hits[k] += 1
            for i, is_int, nullable, seen in groups:
                if is_int:
                    seen.add(parse_int(raw[i]))
                else:  # generated key: NULL text -> ""
                    seen.add(raw[i] if raw[i] is not None else "")
            if rows % 5_000_000 == 0:
                print(f"|   {table}: {rows:,} rows", flush=True)

    return rows, [h / rows if rows else 0.0 for h in hits], {cols[i][0]: len(s) for i, _, _, s in groups}


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("data_dir", help="directory with the IMDB CSV files (title.csv, cast_info.csv, ...)")
    ap.add_argument("--catalog", default=DEFAULT_CATALOG, help="catalog to read (default: bundled resource)")
    ap.add_argument("--output", help="where to write the updated catalog (default: overwrite --catalog)")
    ap.add_argument("--tables", help="comma-separated subset of tables to analyze")
    args = ap.parse_args(argv)

    with open(args.catalog, encoding="utf-8") as fh:
        catalog = json.load(fh)

    tables = args.tables.split(",") if args.tables else sorted(catalog["tables"])
    missing = [t for t in tables if not os.path.exists(os.path.join(args.data_dir, t + ".csv"))]
    if missing:
        sys.exit(f"Missing CSV files in {args.data_dir}: {', '.join(t + '.csv' for t in missing)}")

    for t in tables:
        if t not in SCHEMA:
            sys.exit(f"Unknown JOB table '{t}'")
        entry = catalog["tables"][t]
        print(f"| Analyzing '{t}'", flush=True)
        rows, sels, distinct = analyze_table(os.path.join(args.data_dir, t + ".csv"), t, entry)
        entry["rows"] = rows
        entry.pop("rowsBasis", None)
        for f, s in zip(entry["filters"], sels):
            f["selectivity"] = round(s, 8)
            f["basis"] = "measured"
        entry["distinct"] = distinct
        print(f"|   rows={rows:,} distinct={distinct}")
        for f in entry["filters"]:
            print(f"|   {f['selectivity']:.6f}  {f['field']} {f['op']} {f.get('value', '')}")

    catalog["measuredOn"] = {"dataDir": os.path.abspath(args.data_dir),
                             "date": datetime.date.today().isoformat(),
                             "tables": sorted(set(tables) | set((catalog.get("measuredOn") or {}).get("tables", [])))}
    out = args.output or args.catalog
    with open(out, "w", encoding="utf-8") as fh:
        json.dump(catalog, fh, indent=2, ensure_ascii=False)
        fh.write("\n")
    print(f"| Catalog written to {out}")
    print("| Table cardinalities (for TableMetaData.data_cardinality):")
    print(json.dumps({t: catalog["tables"][t]["rows"] for t in sorted(catalog["tables"])}, indent=4))


if __name__ == "__main__":
    main()
