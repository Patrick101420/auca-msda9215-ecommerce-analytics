"""
HBase query examples over user_activity and product_daily (happybase / Thrift).
Each query prints its result and latency. The equivalent HBase shell command is in the
docstring of each function and in hbase/queries.hbase.

Usage: python hbase/queries.py [user_id] [product_id]
"""
import sys
import time
from datetime import datetime

import happybase

REV_BASE = 9_999_999_999
conn = happybase.Connection("localhost", 9090, timeout=120_000)
ua = conn.table("user_activity")
pd_ = conn.table("product_daily")


def rev(iso):
    return f"{REV_BASE - int(datetime.fromisoformat(iso).timestamp()):010d}"


def timed(title):
    def deco(fn):
        def run(*a, **k):
            t0 = time.perf_counter()
            out = fn(*a, **k)
            print(f"\n## {title}  [{(time.perf_counter() - t0) * 1000:.1f} ms]")
            return out
        return run
    return deco


@timed("Q1 - last 5 sessions of a user (prefix scan, newest first, summary family only)")
def last_sessions(user, n=5):
    """scan 'user_activity', {ROWPREFIXFILTER => 'user_000042#', COLUMNS => ['s'], LIMIT => 5}"""
    return list(ua.scan(row_prefix=f"{user}#".encode(), columns=[b"s"], limit=n))


@timed("Q2 - RANGE SCAN: a user's sessions in a date window (start/stop rows from reversed ts)")
def sessions_between(user, frm, to):
    """Newest-first ordering means the LATER date gives the START row.
    scan 'user_activity', {STARTROW => 'user_000042#<rev(to)>', STOPROW => 'user_000042#<rev(from)>'}"""
    return list(ua.scan(row_start=f"{user}#{rev(to)}".encode(), row_stop=f"{user}#{rev(frm)}".encode(),
                        columns=[b"s:start", b"s:status", b"s:dur"]))


@timed("Q3 - GET one session's full click-stream (page-view family)")
def journey(rowkey):
    """get 'user_activity', '<rowkey>', {COLUMN => 'pv'}"""
    return ua.row(rowkey, columns=[b"pv", b"c"])


@timed("Q4 - VALUE SCAN: a user's converted sessions (server-side SingleColumnValueFilter)")
def converted(user):
    """scan 'user_activity', {ROWPREFIXFILTER => 'user_000042#',
        FILTER => "SingleColumnValueFilter('s','status',=,'binary:converted',true,true)"}"""
    f = "SingleColumnValueFilter('s', 'status', =, 'binary:converted', true, true)"
    return list(ua.scan(row_prefix=f"{user}#".encode(), filter=f, columns=[b"s:start", b"s:status", b"c"]))


@timed("Q5 - HYBRID SCAN: row-key range across users AND one column family + value filter")
def hybrid(u_from, u_to):
    """Mobile sessions of users in [u_from, u_to), reading only family 's'.
    scan 'user_activity', {STARTROW => 'user_000100', STOPROW => 'user_000200', COLUMNS => ['s'],
        FILTER => "SingleColumnValueFilter('s','dev',=,'binary:mobile',true,true)"}"""
    f = "SingleColumnValueFilter('s', 'dev', =, 'binary:mobile', true, true)"
    return list(ua.scan(row_start=u_from.encode(), row_stop=u_to.encode(), columns=[b"s"], filter=f))


@timed("Q6 - product time-series RANGE SCAN: one product, one month")
def product_month(pid, yyyymm):
    """scan 'product_daily', {STARTROW => 'prod_00123#20261001', STOPROW => 'prod_00123#20261101'}"""
    y, m = int(yyyymm[:4]), int(yyyymm[4:])
    nxt = f"{y + (m == 12)}{(m % 12) + 1:02d}"
    return list(pd_.scan(row_start=f"{pid}#{yyyymm}01".encode(), row_stop=f"{pid}#{nxt}01".encode(), columns=[b"m"]))


if __name__ == "__main__":
    user = sys.argv[1] if len(sys.argv) > 1 else "user_000420"
    pid = sys.argv[2] if len(sys.argv) > 2 else None

    rows = last_sessions(user)
    for k, d in rows:
        print(" ", k.decode(), {c.decode(): v.decode() for c, v in d.items()})

    starts = [d[b"s:start"].decode() for _, d in rows]
    if rows:
        # window = the 14 days ending at the user's most recent session
        to = starts[0]
        frm = datetime.fromisoformat(to).replace(hour=0, minute=0, second=0)
        frm = frm.fromtimestamp(frm.timestamp() - 14 * 86400).isoformat()
        r2 = sessions_between(user, frm, to)
        print(f"  {len(r2)} sessions between {frm} and {to} (inclusive of the start, exclusive of the end row)")
        for k, d in r2[:5]:
            print(" ", k.decode(), d[b"s:start"].decode(), d[b"s:status"].decode())

        j = journey(rows[0][0])
        for c in sorted(j):
            print(" ", c.decode(), "=", j[c].decode())

    r4 = converted(user)
    print(f"  {len(r4)} converted sessions; first 3:")
    for k, d in r4[:3]:
        print(" ", k.decode(), {c.decode(): v.decode() for c, v in d.items()})

    r5 = hybrid("user_000100", "user_000200")
    print(f"  {len(r5)} mobile sessions for users 100-199 (sampled users only: 100,110,...,190)")

    if pid is None:
        # busiest product in the table by scanning a small slice
        pid = "prod_00042"
    month = max(d[b"s:start"].decode()[:7].replace("-", "") for _, d in rows) if rows else "202610"
    r6 = product_month(pid, month)
    print(f"  {pid} {month}: {len(r6)} days with activity")
    for k, d in r6[:7]:
        print(" ", k.decode(), {c.decode(): v.decode() for c, v in d.items()})
