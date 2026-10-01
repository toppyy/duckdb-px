#!/usr/bin/env python3
"""Compare the performance of two versions of the extension.

    make perf                              the current branch against main
    make perf PERF_REFS="BASE HEAD"        two refs, or two commits
    make perf PERF_OUT=perf.json           also write the samples to a file

For the full command line, with --repeats and friends, call the script directly:
python3 scripts/perf.py --help

It builds each ref with `make`, which is cheap because DuckDB is already built
and only src/ is recompiled, and then runs the same queries against both builds
several times, alternating between them so that the drift of the machine lands
on both. The reported figure is the fastest run of each, because benchmark
noise only ever adds time.

This reports. It does not fail on a regression: read the numbers.
"""

import argparse
import glob
import hashlib
import json
import os
import shutil
import subprocess
import sys
import tempfile
import time

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA = os.path.join(REPO, "test", "data", "large",
                    "statfin_tyonv_pxt_12ti.px")
BINARY = os.path.join(REPO, "build", "release", "duckdb")

# 201 MB, 77.1M observations, two variables, INTEGER values. One full scan takes
# about nine seconds, which is long enough to measure without the wall clock
# dominating it.
#
# Alue is the first variable, so a filter on it can be answered by skipping the
# blocks that cannot match. Tiedot is not, so a filter on it cannot, and that is
# the case to beat once pushdown covers more than the first variable.
CASES = [
    ("scan",
     "SELECT count(*) AS rows, count(value) AS values, sum(value) AS total "
     "FROM read_px('{data}')"),
    ("filter_first",
     "SELECT count(*) AS rows FROM read_px('{data}') "
     "WHERE Alue = 'KU564'"),
    ("filter_first_multiple",
        "SELECT count(*) AS rows FROM read_px('{data}') "
        "WHERE Alue IN ('KU009','KU091','KU564')"),
    ("filter_first_nomatch",
     "SELECT count(*) AS rows FROM read_px('{data}') "
     "WHERE Alue = 'ZZZZZZ'"),
    ("filter_second",
     "SELECT count(*) AS rows FROM read_px('{data}') "
     "WHERE Tiedot = 'x'"),
]


def fail(message):
    print("error: %s" % message, file=sys.stderr)
    sys.exit(1)


def git(*args):
    return subprocess.run(["git", "-C", REPO] + list(args),
                          capture_output=True, text=True)


def here():
    """The branch we are on, or the sha, so that we can come back to it."""
    result = git("symbolic-ref", "-q", "--short", "HEAD")
    if result.returncode == 0 and result.stdout.strip():
        return result.stdout.strip()
    return git("rev-parse", "HEAD").stdout.strip()


def checkout(ref):
    result = git("checkout", "--detach", ref)
    if result.returncode != 0:
        fail("could not check out %s:\n%s" % (ref, result.stderr.strip()))


def drop_extension_objects():
    """Make sure make rebuilds the extension from the sources just checked out.

    Git only rewrites the files that differ between two refs, so a source that
    both refs happen to share keeps its old mtime and make is free to reuse the
    object it was compiled into for the previous ref. When that happens both
    builds come out as the same binary and the comparison says nothing at all.
    Throwing the objects away costs a few recompiles of extension code and takes
    the doubt away. DuckDB itself is not touched, so it is not rebuilt.
    """
    root = os.path.join(REPO, "build", "release", "extension", "px")
    for obj in glob.glob(os.path.join(root, "CMakeFiles", "*.dir", "**", "*.o")):
        os.remove(obj)
    for name in ("libpx_extension.a", "px.duckdb_extension"):
        path = os.path.join(root, name)
        if os.path.exists(path):
            os.remove(path)


def digest(path):
    """The identity of a build, to catch two builds that are really one."""
    sha = hashlib.sha256()
    with open(path, "rb") as handle:
        for block in iter(lambda: handle.read(1 << 20), b""):
            sha.update(block)
    return sha.hexdigest()


def build(ref, sha, dest):
    """Build exactly sha and keep the binary, under the name ref for reporting."""
    # The sha, not ref: the baseline build leaves the repository detached at the
    # baseline, so a ref of HEAD would resolve to the baseline by then.
    checkout(sha)

    drop_extension_objects()
    print("building %s (%s) ..." % (ref, sha[:7]), file=sys.stderr)
    # No capture: the build log goes to the terminal, which is where a compile
    # error belongs.
    result = subprocess.run(["make"], cwd=REPO)
    if result.returncode != 0:
        fail("%s did not build, see above" % ref)
    if not os.path.exists(BINARY):
        fail("the build of %s produced no %s" % (ref, BINARY))

    shutil.copy(BINARY, dest)


def warm(path):
    """Read the file once so the first measured query is not the disk."""
    with open(path, "rb") as handle:
        while handle.read(1 << 22):
            pass


def measure(binary, sql, timeout):
    """Seconds for one query in a fresh process, and what it returned."""
    # One thread: the scan holds a mutex on the single shared reader anyway, so
    # the other threads only add scheduling noise around the aggregate.
    script = "PRAGMA threads=1;\n%s" % sql
    start = time.perf_counter()
    try:
        process = subprocess.run([binary, "-noheader", "-list", "-c", script],
                                 capture_output=True, text=True,
                                 timeout=timeout)
    except subprocess.TimeoutExpired:
        fail("a query took longer than %ss" % timeout)
    elapsed = time.perf_counter() - start

    if process.returncode != 0:
        fail("query failed:\n%s" % process.stderr.strip()[:400])
    return elapsed, process.stdout.strip()


def seconds(value):
    return "%.3fs" % value


def main():
    parser = argparse.ArgumentParser(
        description="Compare the performance of two versions of the extension")
    parser.add_argument("refs", nargs="*",
                        help="baseline and candidate. One ref is taken as the "
                             "baseline for HEAD. Default main and HEAD.")
    parser.add_argument("--repeats", type=int, default=3,
                        help="runs of each query per build, default 3")
    parser.add_argument("--timeout", type=float, default=600.0)
    parser.add_argument("--data", default=DATA)
    parser.add_argument("--out", help="write the samples to this JSON file")
    args = parser.parse_args()

    if len(args.refs) == 2:
        base_ref, cand_ref = args.refs
    elif len(args.refs) == 1:
        # `perf 0.0.2` means "how is what I have now against 0.0.2", which is
        # the question a single ref is asked.
        base_ref, cand_ref = args.refs[0], "HEAD"
    elif not args.refs:
        base_ref, cand_ref = "main", "HEAD"
    else:
        fail("expected at most two refs, got %d" % len(args.refs))

    if args.repeats < 1:
        fail("--repeats must be at least 1")
    if not os.path.exists(args.data):
        fail("%s is missing.\n"
             "       bash test/init_test_large_data.sh" % args.data)

    if not os.path.exists(os.path.join(REPO, "build", "release")):
        fail("build/release does not exist, run make first")

    base_sha = git("rev-parse", base_ref).stdout.strip()
    cand_sha = git("rev-parse", cand_ref).stdout.strip()
    if not base_sha:
        fail("cannot resolve the baseline %r" % base_ref)
    if not cand_sha:
        fail("cannot resolve the candidate %r" % cand_ref)

    print("baseline  %-14s %s" % (base_ref, base_sha[:7]))
    print("candidate %-14s %s" % (cand_ref, cand_sha[:7]))
    print()
    if base_sha == cand_sha:
        print("%s and %s are the same commit, so there is nothing to compare. "
              "Run this from a branch, or name another ref." % (base_ref, cand_ref))
        return

    queries = [(name, sql.format(data=args.data)) for name, sql in CASES]

    samples = {base_ref: {name: [] for name, _ in queries},
               cand_ref: {name: [] for name, _ in queries}}
    results = {base_ref: {}, cand_ref: {}}

    workdir = tempfile.mkdtemp(prefix="perf-")
    start_ref = here()
    try:
        binaries = {base_ref: os.path.join(workdir, "baseline"),
                    cand_ref: os.path.join(workdir, "candidate")}
        build(base_ref, base_sha, binaries[base_ref])
        build(cand_ref, cand_sha, binaries[cand_ref])

        git("checkout", start_ref)
        print()

        base_digest = digest(binaries[base_ref])
        cand_digest = digest(binaries[cand_ref])
        if base_digest == cand_digest:
            print("warning: both builds are byte for byte the same binary, so "
                  "the two columns below are the same program measured twice. "
                  "The extension did not rebuild.", file=sys.stderr)
            print(file=sys.stderr)

        warm(args.data)

        total = len(queries) * args.repeats * 2
        done = 0
        for name, sql in queries:
            for repetition in range(args.repeats):
                for ref in (base_ref, cand_ref):
                    elapsed, output = measure(binaries[ref], sql,
                                              args.timeout)
                    samples[ref][name].append(elapsed)
                    seen = results[ref].get(name)
                    if seen is None:
                        results[ref][name] = output
                    elif seen != output:
                        print("warning: %s/%s returned different data "
                              "between runs" % (ref, name), file=sys.stderr)
                    done += 1
            best = min(min(samples[r][name]) for r in (base_ref, cand_ref))
            print("  [%2d/%2d] %-20s %s"
                  % (done, total, name, seconds(best)),
                  file=sys.stderr, flush=True)
    finally:
        git("checkout", start_ref)
        shutil.rmtree(workdir, ignore_errors=True)

    print()
    print("%-20s %10s %10s %9s" % ("case", base_ref, cand_ref, "change"))
    print("-" * 54)
    rows = []
    for name, _ in queries:
        base_min = min(samples[base_ref][name])
        cand_min = min(samples[cand_ref][name])
        change = (cand_min / base_min - 1.0) * 100.0
        rows.append((name, base_min, cand_min, change))
        print("%-20s %10s %10s %+8.1f%%"
              % (name, seconds(base_min), seconds(cand_min), change))

    print()
    slower = [r for r in rows if r[3] > 10.0]
    if not slower:
        print("nothing got more than 10% slower.")
    else:
        print("more than 10% slower: %s"
              % ", ".join("%s %+.1f%%" % (r[0], r[3]) for r in slower))

    mismatched = [name for name, _ in queries
                  if results[base_ref][name] != results[cand_ref][name]]
    if mismatched:
        print()
        print("warning: these cases returned different data between the two "
              "builds, so their timings are not comparable: %s"
              % ", ".join(mismatched))

    if args.out:
        document = {
            "baseline": {"ref": base_ref, "sha": base_sha[:7]},
            "candidate": {"ref": cand_ref, "sha": cand_sha[:7]},
            "data": args.data,
            "repeats": args.repeats,
            "cases": [{"id": name,
                       "baseline_samples": samples[base_ref][name],
                       "candidate_samples": samples[cand_ref][name],
                       "baseline_min": base_min,
                       "candidate_min": cand_min,
                       "change": change / 100.0,
                       "results_match":
                           results[base_ref][name] == results[cand_ref][name]}
                      for name, base_min, cand_min, change in rows],
        }
        with open(args.out, "w") as handle:
            json.dump(document, handle, indent=2)
            handle.write("\n")
        print()
        print("wrote %s" % args.out)


if __name__ == "__main__":
    main()