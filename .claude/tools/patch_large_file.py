#!/usr/bin/env python3
"""Fast search and patch for very large single files (e.g. programs/server/play.html).

Avoid editor StrReplace on multi-MB files — it is slow and often times out.

Examples:
  # Search (line numbers + short context)
  .claude/tools/patch_large_file.py grep programs/server/play.html '_appendCopyButton'

  # Show lines around a match
  .claude/tools/patch_large_file.py context programs/server/play.html 4561 -C 3

  # Replace exact text once (fails if 0 or >1 matches)
  .claude/tools/patch_large_file.py replace programs/server/play.html \\
      'if (value === null || value === 0) return;' \\
      'if (value === null || value === \"\") return;'

  # Replace from a file (first line = old, rest = new, separated by --- on its own line)
  .claude/tools/patch_large_file.py apply programs/server/play.html patch.txt

  # Replace a single 1-based line number
  .claude/tools/patch_large_file.py line programs/server/play.html 4561 \\
      '        if (value === null || value === \"\") return;'
"""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path


def repo_root() -> Path:
    here = Path(__file__).resolve()
    # .claude/tools/patch_large_file.py -> repo root
    return here.parents[2]


def resolve_path(path: str) -> Path:
    p = Path(path)
    if p.is_absolute():
        return p
    return repo_root() / p


def read_lines(path: Path) -> list[str]:
    with path.open(encoding="utf-8", errors="replace") as f:
        return f.readlines()


def write_lines(path: Path, lines: list[str]) -> None:
    with path.open("w", encoding="utf-8", newline="") as f:
        f.writelines(lines)


def cmd_grep(path: Path, pattern: str, ignore_case: bool, limit: int) -> int:
    flags = re.IGNORECASE if ignore_case else 0
    rx = re.compile(pattern, flags)
    shown = 0
    with path.open(encoding="utf-8", errors="replace") as f:
        for i, line in enumerate(f, 1):
            if rx.search(line):
                sys.stdout.write(f"{i}:{line}")
                shown += 1
                if shown >= limit:
                    break
    if shown == 0:
        print(f"(no matches for /{pattern}/ in {path})", file=sys.stderr)
        return 1
    if shown == limit:
        print(f"(stopped after {limit} matches)", file=sys.stderr)
    return 0


def cmd_context(path: Path, line_no: int, before: int, after: int) -> int:
    lines = read_lines(path)
    if line_no < 1 or line_no > len(lines):
        print(f"line {line_no} out of range (1..{len(lines)})", file=sys.stderr)
        return 1
    start = max(0, line_no - 1 - before)
    end = min(len(lines), line_no + after)
    for i in range(start, end):
        prefix = ">" if i + 1 == line_no else " "
        sys.stdout.write(f"{prefix}{i + 1:6d}|{lines[i]}")
    return 0


def cmd_replace(path: Path, old: str, new: str, count: int) -> int:
    text = path.read_text(encoding="utf-8")
    n = text.count(old)
    if n == 0:
        print(f"error: pattern not found in {path}", file=sys.stderr)
        return 1
    if count >= 0 and n != count:
        print(f"error: expected {count} occurrence(s), found {n}", file=sys.stderr)
        return 1
    path.write_text(text.replace(old, new, count if count >= 0 else -1), encoding="utf-8")
    print(f"replaced {n if count < 0 else count} occurrence(s) in {path}")
    return 0


def cmd_line(path: Path, line_no: int, new_line: str) -> int:
    lines = read_lines(path)
    if line_no < 1 or line_no > len(lines):
        print(f"line {line_no} out of range (1..{len(lines)})", file=sys.stderr)
        return 1
    if not new_line.endswith("\n"):
        new_line += "\n"
    lines[line_no - 1] = new_line
    write_lines(path, lines)
    print(f"updated line {line_no} in {path}")
    return 0


def cmd_apply(path: Path, patch_file: Path, count: int) -> int:
    body = patch_file.read_text(encoding="utf-8")
    if "\n---\n" not in body:
        print("patch file must contain old and new blocks separated by a lone --- line", file=sys.stderr)
        return 1
    old, new = body.split("\n---\n", 1)
    return cmd_replace(path, old, new, count)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_grep = sub.add_parser("grep", help="search with line numbers")
    p_grep.add_argument("file")
    p_grep.add_argument("pattern")
    p_grep.add_argument("-i", "--ignore-case", action="store_true")
    p_grep.add_argument("-n", "--limit", type=int, default=40)

    p_ctx = sub.add_parser("context", help="print lines around a line number")
    p_ctx.add_argument("file")
    p_ctx.add_argument("line", type=int)
    p_ctx.add_argument("-C", type=int, default=5)

    p_rep = sub.add_parser("replace", help="replace exact substring")
    p_rep.add_argument("file")
    p_rep.add_argument("old")
    p_rep.add_argument("new")
    p_rep.add_argument("--count", type=int, default=1, help="required match count (-1 = all)")

    p_line = sub.add_parser("line", help="replace one line by number")
    p_line.add_argument("file")
    p_line.add_argument("line_no", type=int)
    p_line.add_argument("text")

    p_apply = sub.add_parser("apply", help="replace using patch file (old\\n---\\nnew)")
    p_apply.add_argument("file")
    p_apply.add_argument("patch_file")
    p_apply.add_argument("--count", type=int, default=1)

    args = parser.parse_args()
    path = resolve_path(args.file)

    if not path.is_file():
        print(f"error: not a file: {path}", file=sys.stderr)
        return 1

    if args.cmd == "grep":
        return cmd_grep(path, args.pattern, args.ignore_case, args.limit)
    if args.cmd == "context":
        return cmd_context(path, args.line, args.C, args.C)
    if args.cmd == "replace":
        return cmd_replace(path, args.old, args.new, args.count)
    if args.cmd == "line":
        return cmd_line(path, args.line_no, args.text)
    if args.cmd == "apply":
        return cmd_apply(path, resolve_path(args.patch_file), args.count)
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
