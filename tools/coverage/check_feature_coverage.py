#!/usr/bin/env python3
"""Check UT coverage of feature-owned executable diff lines.

The script intentionally keeps the raw Cobertura input unchanged. It maps
executable lines changed since a baseline to feature commits with git blame,
then emits raw and waiver-adjusted reports for each subsystem and their union.
"""

import argparse
import fnmatch
import hashlib
import html
import json
import pathlib
import re
import shutil
import subprocess
import sys
import xml.etree.ElementTree as ET


HUNK_RE = re.compile(
    r"^@@ -\d+(?:,\d+)? \+(\d+)(?:,(\d+))? @@")
BLAME_RE = re.compile(r"^([0-9a-f]{40}) \d+ (\d+)(?: (\d+))?$")
BRANCH_RE = re.compile(r"\((\d+)/(\d+)\)")


def run_git(repo, *args):
    return subprocess.check_output(
        ["git", *args], cwd=repo, text=True,
        stderr=subprocess.STDOUT)


def load_json(path):
    with path.open(encoding="utf-8") as source:
        return json.load(source)


def resolve_commit(repo, commit):
    return run_git(repo, "rev-parse", commit).strip()


def changed_lines(repo, baseline):
    output = run_git(
        repo, "diff", "--unified=0", "--no-ext-diff", baseline, "--",
        "src/brpc", "src/butil")
    result = {}
    current_file = None
    for raw_line in output.splitlines():
        if raw_line.startswith("+++ b/"):
            current_file = raw_line[6:]
            result.setdefault(current_file, set())
            continue
        match = HUNK_RE.match(raw_line)
        if match and current_file:
            start = int(match.group(1))
            count = int(match.group(2) or "1")
            result[current_file].update(range(start, start + count))
    return result


def blame_lines(repo, filename):
    output = run_git(repo, "blame", "--line-porcelain", "--", filename)
    owners = {}
    for line in output.splitlines():
        match = BLAME_RE.match(line)
        if not match:
            continue
        commit = match.group(1)
        final_line = int(match.group(2))
        count = int(match.group(3) or "1")
        for offset in range(count):
            owners[final_line + offset] = commit
    return owners


def cobertura_lines(path):
    root = ET.parse(path).getroot()
    result = {}
    for klass in root.findall(".//class"):
        filename = klass.attrib.get("filename", "")
        if filename.startswith("./"):
            filename = filename[2:]
        lines = result.setdefault(filename, {})
        for node in klass.findall("./lines/line"):
            line = int(node.attrib["number"])
            hits = int(float(node.attrib.get("hits", "0")))
            branch_covered = 0
            branch_total = 0
            branch_match = BRANCH_RE.search(
                node.attrib.get("condition-coverage", ""))
            if branch_match:
                branch_covered = int(branch_match.group(1))
                branch_total = int(branch_match.group(2))
            previous = lines.get(line, {
                "hits": 0, "branch_covered": 0, "branch_total": 0,
            })
            lines[line] = {
                "hits": max(previous["hits"], hits),
                "branch_covered": max(
                    previous["branch_covered"], branch_covered),
                "branch_total": max(
                    previous["branch_total"], branch_total),
            }
    return result


def matches_any(path, patterns):
    return any(fnmatch.fnmatch(path, pattern) for pattern in patterns)


def source_context_hash(repo, filename, line):
    path = repo / filename
    source_lines = path.read_text(
        encoding="utf-8", errors="replace").splitlines()
    if line < 1 or line > len(source_lines):
        raise ValueError(f"{filename}:{line} is outside the source file")
    start = max(0, line - 2)
    end = min(len(source_lines), line + 1)
    context = "\n".join(source_lines[start:end]).encode("utf-8")
    return hashlib.sha256(context).hexdigest()


def line_label(item):
    return f"{item['file']}:{item['line']}"


def percentage(covered, total):
    return 100.0 if total == 0 else covered * 100.0 / total


def summarize(lines, waived):
    raw_total = len(lines)
    raw_covered = sum(item["hits"] > 0 for item in lines)
    adjusted = [
        item for item in lines
        if (item["file"], item["line"]) not in waived
    ]
    adjusted_covered = sum(item["hits"] > 0 for item in adjusted)
    raw_branch_covered = sum(item["branch_covered"] for item in lines)
    raw_branch_total = sum(item["branch_total"] for item in lines)
    adjusted_branch_covered = sum(
        item["branch_covered"] for item in adjusted)
    adjusted_branch_total = sum(item["branch_total"] for item in adjusted)
    return {
        "raw": {
            "covered": raw_covered,
            "total": raw_total,
            "percent": percentage(raw_covered, raw_total),
            "missing": [
                line_label(item) for item in lines if item["hits"] == 0
            ],
            "branch_covered": raw_branch_covered,
            "branch_total": raw_branch_total,
            "branch_percent": percentage(
                raw_branch_covered, raw_branch_total),
        },
        "adjusted": {
            "covered": adjusted_covered,
            "total": len(adjusted),
            "percent": percentage(adjusted_covered, len(adjusted)),
            "missing": [
                line_label(item) for item in adjusted if item["hits"] == 0
            ],
            "branch_covered": adjusted_branch_covered,
            "branch_total": adjusted_branch_total,
            "branch_percent": percentage(
                adjusted_branch_covered, adjusted_branch_total),
        },
    }


def write_html(path, title, report, adjusted):
    rows = []
    key = "adjusted" if adjusted else "raw"
    for name, data in report["subsystems"].items():
        metrics = data[key]
        rows.append(
            "<tr><td>{}</td><td>{}/{}</td><td>{:.2f}%</td>"
            "<td>{}/{}</td><td>{:.2f}%</td>"
            "<td><pre>{}</pre></td></tr>".format(
                html.escape(name),
                metrics["covered"],
                metrics["total"],
                metrics["percent"],
                metrics["branch_covered"],
                metrics["branch_total"],
                metrics["branch_percent"],
                html.escape("\n".join(metrics["missing"]))))
    path.write_text(
        "<!doctype html><meta charset='utf-8'><title>{0}</title>"
        "<h1>{0}</h1><table border='1' cellspacing='0' cellpadding='5'>"
        "<tr><th>范围</th><th>覆盖行/总行</th><th>行覆盖率</th>"
        "<th>覆盖分支/总分支</th><th>分支覆盖率（辅助）</th>"
        "<th>未覆盖行</th></tr>{1}</table>".format(
            html.escape(title), "".join(rows)),
        encoding="utf-8")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--coverage", required=True, type=pathlib.Path)
    parser.add_argument("--scope", required=True, type=pathlib.Path)
    parser.add_argument("--waivers", required=True, type=pathlib.Path)
    parser.add_argument("--output", required=True, type=pathlib.Path)
    parser.add_argument("--repo", type=pathlib.Path)
    args = parser.parse_args()

    repo = (args.repo or pathlib.Path(__file__).resolve().parents[2]).resolve()
    scope = load_json(args.scope)
    waiver_config = load_json(args.waivers)
    baseline = scope["baseline"]
    threshold = float(scope.get("threshold", 90.0))
    changed = changed_lines(repo, baseline)
    coverage = cobertura_lines(args.coverage)

    subsystem_commits = {}
    for name, config in scope["subsystems"].items():
        subsystem_commits[name] = {
            resolve_commit(repo, commit)
            for commit in config.get("commits", [])
        }

    lines_by_subsystem = {
        name: [] for name in scope["subsystems"]
    }
    blame_cache = {}
    for filename, diff_lines in sorted(changed.items()):
        executable = coverage.get(filename, {})
        if not executable:
            continue
        owners = blame_cache.setdefault(
            filename, blame_lines(repo, filename))
        for line in sorted(diff_lines & set(executable)):
            owner = owners.get(line, "0" * 40)
            for name, config in scope["subsystems"].items():
                owned_commit = owner in subsystem_commits[name]
                owned_worktree = (
                    owner == "0" * 40 and
                    matches_any(
                        filename,
                        config.get("working_tree_globs", [])))
                if owned_commit or owned_worktree:
                    lines_by_subsystem[name].append({
                        "file": filename,
                        "line": line,
                        "hits": executable[line]["hits"],
                        "branch_covered":
                            executable[line]["branch_covered"],
                        "branch_total": executable[line]["branch_total"],
                        "owner": owner,
                    })

    known_lines = {
        name: {(item["file"], item["line"]) for item in lines}
        for name, lines in lines_by_subsystem.items()
    }
    waived = {name: set() for name in lines_by_subsystem}
    waiver_details = []
    errors = []
    for entry in waiver_config.get("waivers", []):
        required = {
            "id", "subsystem", "symbol", "reason", "safety",
            "alternative_validation", "residual_risk", "locations",
        }
        missing_fields = sorted(required - set(entry))
        if missing_fields:
            errors.append(
                f"{entry.get('id', '<unknown>')}: missing fields "
                f"{', '.join(missing_fields)}")
            continue
        subsystem = entry["subsystem"]
        if subsystem not in lines_by_subsystem:
            errors.append(
                f"{entry['id']}: unknown subsystem {subsystem}")
            continue
        for location in entry["locations"]:
            key = (location["file"], int(location["line"]))
            expected_hash = location.get("context_sha256", "")
            try:
                actual_hash = source_context_hash(
                    repo, key[0], key[1])
            except ValueError as error:
                errors.append(f"{entry['id']}: {error}")
                continue
            if actual_hash != expected_hash:
                errors.append(
                    f"{entry['id']}: stale context hash for "
                    f"{key[0]}:{key[1]}")
                continue
            if key not in known_lines[subsystem]:
                errors.append(
                    f"{entry['id']}: {key[0]}:{key[1]} is not an "
                    f"executable changed line owned by {subsystem}")
                continue
            hit_count = next(
                item["hits"] for item in lines_by_subsystem[subsystem]
                if (item["file"], item["line"]) == key)
            if hit_count > 0:
                errors.append(
                    f"{entry['id']}: {key[0]}:{key[1]} is covered and "
                    "must not be waived")
                continue
            waived[subsystem].add(key)
        waiver_details.append(entry)

    union_lines = {}
    union_waived = set()
    for name, lines in lines_by_subsystem.items():
        for item in lines:
            union_lines[(item["file"], item["line"])] = item
        union_waived.update(waived[name])
    lines_by_subsystem["aggregate"] = sorted(
        union_lines.values(), key=lambda item: (item["file"], item["line"]))
    waived["aggregate"] = union_waived

    report = {
        "baseline": baseline,
        "threshold": threshold,
        "coverage_input": str(args.coverage.resolve()),
        "subsystems": {
            name: summarize(lines, waived[name])
            for name, lines in lines_by_subsystem.items()
        },
        "waivers": waiver_details,
        "errors": errors,
    }

    args.output.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(args.coverage, args.output / "raw-coverage.xml")
    (args.output / "coverage-report.json").write_text(
        json.dumps(report, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8")
    write_html(
        args.output / "raw-report.html",
        "新增功能 UT 原始覆盖率", report, False)
    write_html(
        args.output / "adjusted-report.html",
        "新增功能 UT 调整后覆盖率", report, True)

    markdown = [
        "# 新增功能 UT 覆盖率",
        "",
        f"- 基线：`{baseline}`",
        f"- 门禁：调整后差异行覆盖率 ≥ {threshold:.0f}%",
        "",
        "| 范围 | 原始覆盖率 | 调整后覆盖率 | 豁免行数 |",
        "| --- | ---: | ---: | ---: |",
    ]
    for name, data in report["subsystems"].items():
        raw = data["raw"]
        adjusted = data["adjusted"]
        markdown.append(
            f"| {name} | {raw['covered']}/{raw['total']} "
            f"({raw['percent']:.2f}%) | "
            f"{adjusted['covered']}/{adjusted['total']} "
            f"({adjusted['percent']:.2f}%) | "
            f"{raw['total'] - adjusted['total']} |")
    markdown.extend(["", "## 调整后未覆盖行", ""])
    for name, data in report["subsystems"].items():
        markdown.append(
            f"- {name}: " +
            (", ".join(data["adjusted"]["missing"]) or "无"))
    markdown.extend(["", "## 分支覆盖率（辅助指标，不参与门禁）", ""])
    for name, data in report["subsystems"].items():
        raw = data["raw"]
        adjusted = data["adjusted"]
        markdown.append(
            f"- {name}: 原始 {raw['branch_covered']}/"
            f"{raw['branch_total']} ({raw['branch_percent']:.2f}%)；"
            f"调整后 {adjusted['branch_covered']}/"
            f"{adjusted['branch_total']} "
            f"({adjusted['branch_percent']:.2f}%)")
    if errors:
        markdown.extend(["", "## 配置错误", ""])
        markdown.extend(f"- {error}" for error in errors)
    (args.output / "summary.md").write_text(
        "\n".join(markdown) + "\n", encoding="utf-8")

    failed = bool(errors)
    for name, data in report["subsystems"].items():
        if data["adjusted"]["percent"] + 1e-9 < threshold:
            failed = True
    print("\n".join(markdown))
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
