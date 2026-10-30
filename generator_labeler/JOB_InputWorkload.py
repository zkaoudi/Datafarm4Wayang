"""
Build the DataFarm input workload for the Join Order Benchmark (JOB).

DataFarm learns the shape of the jobs it generates (operator sequences, number of joins, plan
depth) from an input workload of execution plans. This script turns each of the 113 JOB queries
into an execution plan in the format of data/input_workload_exec_plan, written the way a
hand-coded Flink DataSet job for the query would look (like the bundled TPC_H_Q*.json plans):

    per table:   Data Source -> Filter        (Filter only if the query filters that table)
    joins:       left-deep chain of Join operators
    aggregate:   Reduce -> Reduce             (the MIN(...) select list: combiner + reduce)
    output:      Data Sink

Join order: start with the first table of the FROM clause and repeatedly add the first FROM table
that joins with the tables joined so far (no cross products). When a new table joins several
already joined tables (JOB's transitive movie_id/person_id predicates), one multi-key Join is used.

The plans are derived from the query text; they were not produced by running Flink, so they carry
no runtimes, estimates or costs (netRunTime = -1). Only the operator graph is used by DataFarm.

Usage:
    python JOB_InputWorkload.py /path/to/join-order-benchmark /path/to/output_folder

The JOB queries are at https://github.com/gregrahn/join-order-benchmark (files 1a.sql ... 33c.sql).
"""
import glob
import json
import os
import re
import sys


def split_conjuncts(where):
    """Split a WHERE clause on top-level AND (outside parentheses and quotes); keeps BETWEEN x AND y."""
    parts, buf, depth, in_quote = [], [], 0, False
    i = 0
    while i < len(where):
        c = where[i]
        if c == "'":
            in_quote = not in_quote
        if not in_quote:
            if c == "(":
                depth += 1
            elif c == ")":
                depth -= 1
            elif (depth == 0 and where[i:i + 3].upper() == "AND"
                  and (i == 0 or not where[i - 1].isalnum())
                  and (i + 3 >= len(where) or not where[i + 3].isalnum())):
                prev = "".join(buf)
                if not re.search(r"\bBETWEEN\s+\S+\s*$", prev, re.I):
                    parts.append(prev.strip())
                    buf = []
                    i += 3
                    continue
        buf.append(c)
        i += 1
    parts.append("".join(buf).strip())
    return [p for p in parts if p]


def parse_query(sql):
    """-> (tables [(table, alias)], filters {alias: [predicate]}, joins [((a, col), (b, col))], select)."""
    s = re.sub(r"--[^\n]*", " ", sql)
    s = re.sub(r"\s+", " ", s).strip().rstrip(";")
    m = re.search(r"\bSELECT\b(.*?)\bFROM\b(.*?)\bWHERE\b(.*)$", s, re.I)
    if not m:
        raise ValueError("expected SELECT ... FROM ... WHERE ...")
    select, frm, where = m.groups()
    if re.search(r"\bGROUP\s+BY\b|\bORDER\s+BY\b", s, re.I):
        raise ValueError("GROUP BY / ORDER BY are not handled (JOB queries have neither)")
    tables = re.findall(r"(\w+)\s+AS\s+(\w+)", frm, re.I)
    aliases = {a for _, a in tables}
    filters = {a: [] for _, a in tables}
    joins = []
    for c in split_conjuncts(where):
        j = re.fullmatch(r"\(?\s*(\w+)\.(\w+)\s*=\s*(\w+)\.(\w+)\s*\)?", c)
        if j and j.group(1) in aliases and j.group(3) in aliases and j.group(1) != j.group(3):
            joins.append(((j.group(1), j.group(2)), (j.group(3), j.group(4))))
            continue
        refs = {a for a in re.findall(r"\b(\w+)\.\w+", re.sub(r"'[^']*'", "''", c)) if a in aliases}
        if len(refs) != 1:
            raise ValueError(f"predicate over {sorted(refs)} is neither a filter nor an equi-join: {c}")
        filters[refs.pop()].append(c)
    return tables, filters, joins, select.strip()


def join_order(tables, joins):
    """Left-deep order: first FROM table, then always the first FROM table connected to the joined set."""
    order = [tables[0][1]]
    rest = [a for _, a in tables[1:]]
    while rest:
        nxt = next((a for a in rest if any({x[0], y[0]} == {a, b} for x, y in joins for b in order)), None)
        if nxt is None:
            raise ValueError(f"join graph is not connected: {order} vs {rest}")
        order.append(nxt)
        rest.remove(nxt)
    return order


def build_plan(name, sql):
    tables, filters, joins, select = parse_query(sql)
    table_of = {a: t for t, a in tables}
    nodes = []

    def add(pact, contents, preds=(), node_type="pact", driver=None, ship="Forward"):
        node = {"id": len(nodes), "type": node_type, "pact": pact, "contents": contents, "parallelism": "1"}
        if preds:
            node["predecessors"] = [{"id": p, "ship_strategy": ship, "exchange_mode": "PIPELINED"} for p in preds]
        if driver:
            node["driver_strategy"] = driver
        nodes.append(node)
        return node["id"]

    def scan(alias):
        nid = add("Data Source", f"{table_of[alias]} AS {alias}", node_type="source")
        if filters[alias]:
            nid = add("Filter", " AND ".join(filters[alias]), [nid], driver="FlatMap")
        return nid

    order = join_order(tables, joins)
    current = scan(order[0])
    joined = {order[0]}
    for a in order[1:]:
        right = scan(a)
        keys = [f"{x[0]}.{x[1]} = {y[0]}.{y[1]}" for x, y in joins
                if (x[0] == a and y[0] in joined) or (y[0] == a and x[0] in joined)]
        current = add("Join", "join on " + " AND ".join(keys), [current, right],
                      driver="Hybrid Hash (build: second input)", ship="Hash Partition")
        joined.add(a)
    current = add("Reduce", f"combine: {select}", [current], driver="Reduce")
    current = add("Reduce", f"reduce: {select}", [current], driver="Reduce", ship="Forward")
    add("Data Sink", "org.apache.flink.api.java.io.DiscardingOutputFormat", [current], node_type="sink")

    used_joins = sum(1 for n in nodes if n["pact"] == "Join")
    assert used_joins == len(tables) - 1
    return {
        "source": f"Derived from JOB query {name} by generator_labeler/JOB_InputWorkload.py "
                  "(operator graph only; not produced by Flink).",
        "query": name,
        "netRunTime": -1,
        "executionPlan": {"nodes": nodes},
    }


def main(argv):
    if len(argv) != 3:
        sys.exit("Usage: python JOB_InputWorkload.py <join-order-benchmark folder> <output folder>")
    src, dest = argv[1], argv[2]
    files = sorted(glob.glob(os.path.join(src, "[0-9]*[a-z].sql")),
                   key=lambda f: (int(re.match(r"\d+", os.path.basename(f)).group()), os.path.basename(f)))
    if len(files) != 113:
        print(f"WARN - expected the 113 JOB queries, found {len(files)} in {src}")
    os.makedirs(dest, exist_ok=True)
    for f in files:
        name = os.path.basename(f)[:-4]
        with open(f) as fh:
            plan = build_plan(name, fh.read())
        with open(os.path.join(dest, f"JOB_{name}.json"), "w") as fh:
            json.dump(plan, fh, indent=1)
            fh.write("\n")
    print(f"Wrote {len(files)} JOB plans to {dest}")


if __name__ == "__main__":
    main(sys.argv)
