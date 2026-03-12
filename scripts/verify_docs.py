#!/usr/bin/env python3

from __future__ import annotations

import re
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent
CANONICAL_DOCS = [
    "docs/00-快速开始.md",
    "docs/01-架构设计.md",
    "docs/02-API参考.md",
    "docs/03-使用指南.md",
    "docs/04-示例代码.md",
    "docs/05-性能测试.md",
    "docs/06-高级主题.md",
    "docs/07-常见问题.md",
]
DOCS_INDEX = "docs/README.md"
README_DOC_LINKS = [
    "docs/README.md",
    *CANONICAL_DOCS,
]
EXAMPLE_NAMES = [
    "E1-async_query",
    "E2-sync_query",
    "E3-async_pool",
    "E4-sync_prepared_tx",
    "E5-async_pipeline",
]
ASYNC_SEMANTICS_FILES = [
    "README.md",
    "docs/00-快速开始.md",
    "docs/02-API参考.md",
    "docs/06-高级主题.md",
    "docs/07-常见问题.md",
]
LINK_RE = re.compile(r"\[([^\]]+)\]\(([^)]+)\)")
STALE_AWAITABLE_RE = re.compile(r"auto&\s+\w+_aw\s*=")
NONEXISTENT_TIMEOUT_RE = re.compile(r"(?<!-)--timeout(?!-sec)")
UNSUPPORTED_BENCH_HELP_RE = re.compile(r"B[12]-(?:SyncPressure|AsyncPressure)\s+--help")


def read_text(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def iter_markdown_lines(path: Path):
    text = read_text(path)
    in_code_fence = False
    for lineno, line in enumerate(text.splitlines(), start=1):
        if line.startswith("```"):
            in_code_fence = not in_code_fence
        yield lineno, line, in_code_fence


def first_h1(path: Path) -> str | None:
    for _, line, in_code_fence in iter_markdown_lines(path):
        if in_code_fence:
            continue
        if line.startswith("# "):
            return line[2:].strip()
    return None


def collect_local_link_errors(path: Path) -> list[str]:
    errors: list[str] = []
    for lineno, line, in_code_fence in iter_markdown_lines(path):
        if in_code_fence:
            continue
        for _, target in LINK_RE.findall(line):
            if target.startswith(("http://", "https://", "mailto:")):
                continue
            if target.startswith("#"):
                continue
            link_target = target.split("#", 1)[0]
            if not link_target:
                continue
            resolved = (path.parent / link_target).resolve()
            if not resolved.exists():
                errors.append(f"{path.relative_to(ROOT)}:{lineno}: broken link -> {target}")
    return errors


def require_contains(errors: list[str], path: Path, expected: str) -> None:
    text = read_text(path)
    if expected not in text:
        errors.append(f"{path.relative_to(ROOT)}: missing required text `{expected}`")


def require_absent_regex(errors: list[str], path: Path, pattern: re.Pattern[str], message: str) -> None:
    text = read_text(path)
    match = pattern.search(text)
    if match:
        line = text[:match.start()].count("\n") + 1
        errors.append(f"{path.relative_to(ROOT)}:{line}: {message}")


def validate_headings(errors: list[str]) -> None:
    for relpath in CANONICAL_DOCS:
        path = ROOT / relpath
        if not path.exists():
            errors.append(f"missing required document `{relpath}`")
            continue
        expected_h1 = Path(relpath).stem
        actual_h1 = first_h1(path)
        if actual_h1 != expected_h1:
            errors.append(
                f"{relpath}: expected first H1 `{expected_h1}`, got `{actual_h1 or 'None'}`"
            )


def validate_links(errors: list[str]) -> None:
    files = [ROOT / "README.md", ROOT / DOCS_INDEX]
    files.extend(ROOT / relpath for relpath in CANONICAL_DOCS)
    for path in files:
        if not path.exists():
            errors.append(f"missing required markdown file `{path.relative_to(ROOT)}`")
            continue
        errors.extend(collect_local_link_errors(path))


def validate_docs_index(errors: list[str]) -> None:
    path = ROOT / DOCS_INDEX
    if not path.exists():
        errors.append(f"missing required document `{DOCS_INDEX}`")
        return
    for relpath in CANONICAL_DOCS:
        require_contains(errors, path, relpath.split("/", 1)[1])


def validate_readme_navigation(errors: list[str]) -> None:
    path = ROOT / "README.md"
    for relpath in README_DOC_LINKS:
        require_contains(errors, path, relpath)


def validate_example_provenance(errors: list[str]) -> None:
    example_doc = ROOT / "docs/04-示例代码.md"
    quickstart = ROOT / "docs/00-快速开始.md"
    readme = ROOT / "README.md"

    for env_var in [
        "GALAY_MYSQL_HOST",
        "GALAY_MYSQL_PORT",
        "GALAY_MYSQL_USER",
        "GALAY_MYSQL_PASSWORD",
        "GALAY_MYSQL_DB",
    ]:
        require_contains(errors, example_doc, env_var)

    for name in EXAMPLE_NAMES:
        include_path = f"examples/include/{name}.cc"
        import_path = f"examples/import/{name}.cc"
        include_target = f"{name}-Include"
        import_target = f"{name}-Import"
        include_run = f"./build/examples/{include_target}"
        import_run = f"./build-import/examples/{import_target}"

        require_contains(errors, example_doc, include_path)
        require_contains(errors, example_doc, include_target)
        require_contains(errors, example_doc, include_run)
        require_contains(errors, example_doc, import_path)
        require_contains(errors, example_doc, import_target)
        require_contains(errors, example_doc, import_run)

        require_contains(errors, quickstart, include_target)
        require_contains(errors, readme, include_target)


def validate_async_semantics(errors: list[str]) -> None:
    for relpath in ASYNC_SEMANTICS_FILES:
        path = ROOT / relpath
        require_absent_regex(
            errors,
            path,
            STALE_AWAITABLE_RE,
            "stale awaitable reference binding detected",
        )

    require_contains(errors, ROOT / "README.md", "Awaitable 值对象")
    require_contains(errors, ROOT / "README.md", "一次 `co_await`")
    require_contains(errors, ROOT / "docs/02-API参考.md", "一次 `co_await`")
    require_contains(errors, ROOT / "docs/07-常见问题.md", "Awaitable 值对象")


def validate_find_package_docs(errors: list[str]) -> None:
    for relpath in [
        "README.md",
        "docs/00-快速开始.md",
        "docs/01-架构设计.md",
        "docs/02-API参考.md",
        "docs/03-使用指南.md",
        "docs/07-常见问题.md",
    ]:
        path = ROOT / relpath
        require_contains(errors, path, "find_package(GalayMysql REQUIRED CONFIG)")
        require_contains(errors, path, "galay-mysql::galay-mysql")
        require_absent_regex(
            errors,
            path,
            re.compile(r"find_package\(galay-mysql REQUIRED CONFIG\)"),
            "stale package contract `find_package(galay-mysql REQUIRED CONFIG)`",
        )


def validate_benchmark_docs(errors: list[str]) -> None:
    path = ROOT / "docs/05-性能测试.md"
    require_contains(errors, path, "GALAY_MYSQL_BUILD_BENCHMARKS")
    require_contains(errors, path, "B1-SyncPressure")
    require_contains(errors, path, "B2-AsyncPressure")
    require_contains(errors, path, "--timeout-sec")
    require_contains(errors, path, "2026-03-10")
    require_contains(errors, path, "未重新产出新的性能数值")
    require_absent_regex(errors, path, NONEXISTENT_TIMEOUT_RE, "nonexistent benchmark flag `--timeout`")
    require_absent_regex(errors, path, re.compile(r"\bB3\b"), "nonexistent benchmark target `B3`")
    require_absent_regex(errors, path, UNSUPPORTED_BENCH_HELP_RE, "unsupported benchmark `--help` usage")
    require_absent_regex(errors, ROOT / "README.md", UNSUPPORTED_BENCH_HELP_RE, "unsupported benchmark `--help` usage")
    require_absent_regex(errors, ROOT / "docs/00-快速开始.md", UNSUPPORTED_BENCH_HELP_RE, "unsupported benchmark `--help` usage")


def main() -> int:
    errors: list[str] = []
    validate_headings(errors)
    validate_links(errors)
    validate_docs_index(errors)
    validate_readme_navigation(errors)
    validate_example_provenance(errors)
    validate_async_semantics(errors)
    validate_find_package_docs(errors)
    validate_benchmark_docs(errors)

    if errors:
        print("documentation verification failed:")
        for error in errors:
            print(f"- {error}")
        return 1

    print("documentation verification passed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
