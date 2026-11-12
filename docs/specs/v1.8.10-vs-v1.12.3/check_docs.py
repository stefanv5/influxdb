#!/usr/bin/env python3
"""Dependency-free structural checker for the v1.8.10-vs-v1.12.3 study.

Run from the repository root:
    python docs/specs/v1.8.10-vs-v1.12.3/check_docs.py

Checks: UTF-8/no BOM, CRLF, code fences, Mermaid non-empty blocks,
version-qualified source references (path exists, line in range),
relative Markdown links, required sections, duplicate H2 headings.
Exit code 1 if any error is found.
"""
import os
import re
import subprocess
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.abspath(os.path.join(HERE, "..", "..", ".."))
REQUIRED = [
    "结论速览", "变更地图", "逐项差异", "优化与收益",
    "行为变化与兼容性", "具体案例", "仍存在的风险", "证据索引",
]
REF_RE = re.compile(r"v1\.(8\.10|12\.3):([A-Za-z0-9_./\-]+\.(?:go|md|yml|yaml|toml|sh|bash|py|tmpl))(?::(\d+))?")
LINK_RE = re.compile(r"\[[^\]]*\]\(([^)#]+?)(#[^)]*)?\)")
NL = chr(10)

errors = []
warnings = []
old_lines_cache = {}
new_lines_cache = {}


def err(file, line, msg):
    errors.append(f"{file}:{line}: {msg}")


def old_lines(path):
    if path not in old_lines_cache:
        r = subprocess.run(["git", "show", f"v1.8.10:{path}"], capture_output=True)
        old_lines_cache[path] = (
            len(r.stdout.decode("utf-8", "ignore").split(NL)) if r.returncode == 0 else -1
        )
    return old_lines_cache[path]


def new_lines(path):
    if path not in new_lines_cache:
        p = os.path.join(ROOT, path)
        new_lines_cache[path] = (
            len(open(p, encoding="utf-8", errors="ignore").read().split(NL))
            if os.path.exists(p) else -1
        )
    return new_lines_cache[path]


files = sorted(f for f in os.listdir(HERE) if f.endswith(".md"))
for fn in files:
    p = os.path.join(HERE, fn)
    raw = open(p, "rb").read()
    if raw.startswith(b"\xef\xbb\xbf"):
        errors.append(f"{fn}: UTF-8 BOM")
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError as e:
        errors.append(f"{fn}: not UTF-8: {e}")
        continue
    if text.count(NL) != text.count(chr(13) + NL):
        err(fn, 1, f"bare LF lines: {text.count(NL) - text.count(chr(13) + NL)}")
    md = text.replace(chr(13) + NL, NL)

    fences = len(re.findall(r"^```", md, re.M))
    if fences % 2:
        err(fn, 1, f"odd code fences: {fences}")
    for i, block in enumerate(re.findall(r"```mermaid\n([\s\S]*?)```", md), 1):
        if not block.strip():
            err(fn, 1, f"empty mermaid block #{i}")

    for m in REF_RE.finditer(md):
        ver, path, ln = m.group(1), m.group(2), m.group(3)
        if path.startswith(".../"):
            continue
        line_start = md.rfind(NL, 0, m.start()) + 1
        if "cat-file -e" in md[line_start:m.start()]:
            continue
        n = old_lines(path) if ver == "8.10" else new_lines(path)
        if n < 0:
            err(fn, md[:m.start()].count(NL) + 1, f"{path} missing in v{ver}")
        elif ln and int(ln) > n:
            err(fn, md[:m.start()].count(NL) + 1, f"{path}:{ln} out of range (max {n})")

    for m in LINK_RE.finditer(md):
        target = m.group(1)
        if target.startswith(("http://", "https://", "mailto:")):
            continue
        target = target.split("#")[0]
        if not target:
            continue
        if not os.path.exists(os.path.normpath(os.path.join(HERE, target))):
            err(fn, md[:m.start()].count(NL) + 1, f"broken link: {target}")

    if re.match(r"0[1-7]_", fn):
        for section in REQUIRED:
            if section not in md:
                err(fn, 1, f"missing section: {section}")

    h2 = re.findall(r"^## (.+)$", md, re.M)
    dup = sorted({h for h in h2 if h2.count(h) > 1})
    if dup:
        warnings.append(f"{fn}: duplicate H2: {', '.join(dup)}")

    # Hard-coded docs commit counts go stale; recommend "若干".
    for i, line in enumerate(md.split(NL), 1):
        if re.search(r"\d+ 个 docs 提交", line):
            warnings.append(f"{fn}:{i}: hard-coded docs commit count")

print(f"checked {len(files)} files")
for w in warnings:
    print("WARN", w)
for e in errors:
    print("ERROR", e)
print(f"errors={len(errors)} warnings={len(warnings)}")
sys.exit(1 if errors else 0)
