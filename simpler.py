
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sys, re
from pathlib import Path
import pandas as pd

SRC = Path("out_resolve/apps_master.csv")
DST = Path("ids_simple.csv")

def read_df_loose(path: Path) -> pd.DataFrame:
    # まずはCSV（区切り自動判定）を寛容に読む
    try:
        return pd.read_csv(
            path,
            engine="python",
            sep=None,                 # 区切り自動推定
            dtype=str,
            keep_default_na=False,
            on_bad_lines="skip",      # 壊れ行はスキップ
            quotechar='"'
        )
    except Exception:
        # TSVとして再トライ
        return pd.read_csv(
            path,
            engine="python",
            sep="\t",
            dtype=str,
            keep_default_na=False,
            on_bad_lines="skip",
            quotechar='"'
        )

def normalize_ids(df: pd.DataFrame) -> pd.DataFrame:
    # 列名のゆらぎを吸収
    lower = {c.lower(): c for c in df.columns}
    col_track = lower.get("trackid") or lower.get("track_id")
    col_bundle = lower.get("bundleid") or lower.get("bundle_id")
    col_appkey = lower.get("app_key") or lower.get("appkey")
    col_qname  = lower.get("query_name") or lower.get("queryname")

    if not col_track and not col_bundle:
        raise ValueError(f"No trackId/bundleId columns found. Columns={list(df.columns)}")

    out_rows = []
    for _, r in df.iterrows():
        track_raw  = (r.get(col_track)  if col_track  else "") or ""
        bundle_raw = (r.get(col_bundle) if col_bundle else "") or ""
        app_key    = ((r.get(col_appkey) or "").strip() if col_appkey else "") or None
        query_name = ((r.get(col_qname)  or "").strip() if col_qname  else "") or None

        track_raw = str(track_raw).strip()
        bundle_raw = str(bundle_raw).strip()

        track_id = None
        if track_raw:
            # "123456789.0" 対策＆余計な文字の除去
            m = re.search(r"(\d{6,12})", track_raw)
            if m:
                try:
                    track_id = int(m.group(1))
                except Exception:
                    track_id = None

        if not track_id and not bundle_raw:
            continue

        out_rows.append({
            "app_key": app_key,
            "query_name": query_name,
            "trackId": track_id,
            "bundleId": bundle_raw or None
        })

    return pd.DataFrame(out_rows)

def fallback_linewise(path: Path) -> pd.DataFrame:
    # 最終手段：ヘッダが壊れていても、行テキストから trackId / bundleId を抽出
    rows = []
    track_re  = re.compile(r"\btrackId\b[^0-9]{0,10}(\d{6,12})", re.IGNORECASE)
    bundle_re = re.compile(r"\bbundleId\b[^A-Za-z0-9._-]{0,10}([A-Za-z0-9._-]+)", re.IGNORECASE)
    appkey_re = re.compile(r"\bapp_key\b[^A-Za-z0-9._-]{0,10}([^\s,;]+)", re.IGNORECASE)
    qname_re  = re.compile(r"\bquery_name\b[^A-Za-z0-9._-]{0,10}(.+)$", re.IGNORECASE)

    with path.open("r", encoding="utf-8", errors="replace") as f:
        for line in f:
            t = None
            b = None
            m1 = track_re.search(line)
            if m1:
                try:
                    t = int(m1.group(1))
                except Exception:
                    t = None
            m2 = bundle_re.search(line)
            if m2:
                b = m2.group(1).strip()
            if not t and not b:
                continue
            app_key = None
            query_name = None
            m3 = appkey_re.search(line)
            if m3:
                app_key = m3.group(1).strip()
            m4 = qname_re.search(line)
            if m4:
                query_name = m4.group(1).strip().strip('"')
            rows.append({"app_key": app_key, "query_name": query_name, "trackId": t, "bundleId": b or None})
    return pd.DataFrame(rows)

def main():
    try:
        df = read_df_loose(SRC)
        ids = normalize_ids(df)
    except Exception as e:
        print(f"[WARN] CSV/TSV parse failed or no ID columns detected: {e}\nFalling back to line-wise regex…")
        ids = fallback_linewise(SRC)

    if ids.empty:
        print("[ERROR] Could not extract any IDs. Please inspect the file around the reported bad lines.")
        sys.exit(1)

    # 重複除去（trackId優先）
    ids["trackId_str"] = ids["trackId"].apply(lambda x: str(int(x)) if pd.notna(x) and str(x).isdigit() else "")
    ids["bundleId_str"] = ids["bundleId"].fillna("")
    ids = ids.drop_duplicates(subset=["trackId_str", "bundleId_str"]).drop(columns=["trackId_str","bundleId_str"])

    ids.to_csv(DST, index=False)
    print(f"Wrote {DST} with {len(ids)} rows.")

if __name__ == "__main__":
    main()



# # python - <<'PY'
# import pandas as pd
# df = pd.read_csv("out_resolve/apps_master.csv", engine="python", sep=None, dtype=str, keep_default_na=False)
# keep = []
# for _, r in df.iterrows():
#     track = (r.get("trackId") or "").strip()
#     bundle = (r.get("bundleId") or "").strip()
#     app_key = (r.get("app_key") or "").strip()
#     query = (r.get("query_name") or "").strip()
#     if not track and not bundle: 
#         continue
#     keep.append({
#         "app_key": app_key or None,
#         "query_name": query or None,
#         "trackId": track or None,
#         "bundleId": bundle or None,
#     })
# pd.DataFrame(keep).to_csv("ids_simple.csv", index=False)
# print("wrote ids_simple.csv", len(keep))
# # PY
