"""Data quality checks, written to registry.quality_checks.

Each check returns (passed: bool, detail: str). run_checks() executes a
list of them against a DataFrame and records every result. A crashed
check is recorded as a failure rather than silently skipped — a check
that cannot run is not a pass.
"""


def run_checks(loader, asset_key, df, checks):
    """Execute checks and upsert each result. Caller commits."""
    for name, fn in checks:
        try:
            passed, detail = fn(df)
        except Exception as exc:
            passed, detail = False, f"check crashed: {exc}"
        detail = detail.replace("'", "''")
        loader.execute(f"""
            INSERT INTO registry.quality_checks
                (asset_key, check_name, passed, detail, checked_at)
            VALUES
                ('{asset_key}', '{name}', {passed}, '{detail}', now())
            ON CONFLICT (asset_key, check_name) DO UPDATE SET
                passed     = EXCLUDED.passed,
                detail     = EXCLUDED.detail,
                checked_at = EXCLUDED.checked_at;
        """)


def has_rows(df):
    n = len(df)
    return n > 0, f"{n} rows"


def no_duplicate_keys(*key_cols):
    def check(df):
        dupes = int(df.duplicated(subset=list(key_cols)).sum())
        return dupes == 0, f"{dupes} duplicate rows on ({', '.join(key_cols)})"
    return check


def no_nulls(*cols):
    def check(df):
        bad = {c: int(df[c].isna().sum()) for c in cols if df[c].isna().any()}
        if bad:
            return False, "nulls found: " + ", ".join(f"{c}={n}" for c, n in bad.items())
        return True, f"no nulls in {', '.join(cols)}"
    return check


def non_negative(col):
    def check(df):
        n = int((df[col] < 0).sum())
        return n == 0, f"{n} negative values in {col}"
    return check
