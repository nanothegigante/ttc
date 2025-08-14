
# python summarize_screenshots.py \
#   --screenshots out_collect/screenshots \
#   --metadata out_collect/metadata.csv \
#   --countries gb jp \
#   --out screenshots_summary.csv



import argparse
import csv
from pathlib import Path
from typing import Dict, Any, List, Tuple, Optional
import pandas as pd

IMAGE_EXTS = {".jpg", ".jpeg", ".png", ".webp"}

def count_images_in_dir(d: Path) -> int:
    if not d.exists() or not d.is_dir():
        return 0
    cnt = 0
    for p in d.rglob("*"):
        if p.is_file() and p.suffix.lower() in IMAGE_EXTS:
            cnt += 1
    return cnt

def split_device_counts(track_dir: Path) -> Dict[str, int]:
    """iphone/ipad/appletv サブフォルダがある想定（なければ直下をiphone扱いしないで total にのみ加算）。"""
    counts = {"iphone": 0, "ipad": 0, "appletv": 0, "total": 0}
    if not track_dir.exists():
        return counts

    # サブフォルダ別
    for sub in ["iphone", "ipad", "appletv"]:
        cdir = track_dir / sub
        if cdir.exists():
            c = sum(1 for p in cdir.rglob("*") if p.is_file() and p.suffix.lower() in IMAGE_EXTS)
            counts[sub] = c

    # サブフォルダ以外にも画像があるかも（後方互換）
    other = 0
    for p in track_dir.iterdir():
        if p.is_file() and p.suffix.lower() in IMAGE_EXTS:
            other += 1

    counts["total"] = counts["iphone"] + counts["ipad"] + counts["appletv"] + other
    return counts

def read_metadata(meta_path: Path) -> pd.DataFrame:
    # できるだけ寛容に読む
    df = pd.read_csv(meta_path, engine="python", sep=None, dtype=str, keep_default_na=False)
    # 列が無い場合は空を補充
    for col in ["trackId", "app_key", "trackName", "sellerName", "country"]:
        if col not in df.columns:
            df[col] = ""
    # trackId を整数文字列に正規化
    def norm_tid(x: str) -> str:
        x = (x or "").strip()
        # "123456789.0" のような形もあるかも
        if x.endswith(".0"):
            x = x[:-2]
        return x
    df["trackId_norm"] = df["trackId"].apply(norm_tid)
    return df

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--screenshots", required=True, help="screenshots ルートディレクトリ（例: out_collect/screenshots）")
    ap.add_argument("--metadata", required=True, help="metadata.csv のパス（appstore_collect.py の出力）")
    ap.add_argument("--countries", nargs="+", default=["gb","jp"], help="チェック対象の国コード（ディレクトリ名と一致）")
    ap.add_argument("--out", default="screenshots_summary.csv")
    args = ap.parse_args()

    ss_root = Path(args.screenshots)
    meta_df = read_metadata(Path(args.metadata))

    # trackId -> 名前等のマップ（国に依らず代表1個を採用）
    # app_key はあれば採用。無ければ空。
    id_to_name: Dict[str, Dict[str, str]] = {}
    for _, r in meta_df.iterrows():
        tid = r["trackId_norm"]
        if not tid:
            continue
        if tid not in id_to_name:
            id_to_name[tid] = {
                "app_key": r.get("app_key", ""),
                "trackName": r.get("trackName", ""),
                "sellerName": r.get("sellerName", "")
            }

    # スクショ有の trackId を国別に収集
    per_country_track_ids: Dict[str, Dict[str, Dict[str, int]]] = {}  # country -> trackId -> device_counts
    for c in args.countries:
        cdir = ss_root / c.lower()
        per_country_track_ids[c.lower()] = {}
        if not cdir.exists():
            continue
        # 直下の trackId ディレクトリを列挙
        for track_dir in cdir.iterdir():
            if not track_dir.is_dir():
                continue
            tid = track_dir.name.strip()
            # 数字のみのフォルダ名を優先的に扱う（念のため非数値も許容）
            # if not tid.isdigit():  # 必要なら有効化
            #     continue
            counts = split_device_counts(track_dir)
            if counts["total"] > 0:
                per_country_track_ids[c.lower()][tid] = counts

    # すべての国の trackId を統合
    all_tids = set()
    for c in args.countries:
        all_tids |= set(per_country_track_ids.get(c.lower(), {}).keys())

    rows: List[Dict[str, Any]] = []
    for tid in sorted(all_tids, key=lambda x: int(x) if x.isdigit() else x):
        name_info = id_to_name.get(tid, {"app_key": "", "trackName": "", "sellerName": ""})
        row = {
            "trackId": tid,
            "app_key": name_info.get("app_key", ""),
            "trackName": name_info.get("trackName", ""),
            "sellerName": name_info.get("sellerName", ""),
        }
        has_any = []
        for c in args.countries:
            c_low = c.lower()
            counts = per_country_track_ids.get(c_low, {}).get(tid, {"iphone":0,"ipad":0,"appletv":0,"total":0})
            row[f"{c_low}_total"] = counts["total"]
            row[f"{c_low}_iphone"] = counts["iphone"]
            row[f"{c_low}_ipad"] = counts["ipad"]
            row[f"{c_low}_appletv"] = counts["appletv"]
            row[f"has_{c_low}"] = bool(counts["total"] > 0)
            row[f"path_{c_low}"] = str((ss_root / c_low / tid).resolve()) if counts["total"] > 0 else ""
            if counts["total"] > 0:
                has_any.append(c_low)
        row["countries_with_screenshots"] = ";".join(has_any)
        row["has_both"] = all(row.get(f"has_{c.lower()}") for c in args.countries)
        rows.append(row)

    out_df = pd.DataFrame(rows)

    # 見やすい列順
    ordered_cols = ["trackId","app_key","trackName","sellerName"]
    for c in args.countries:
        cl = c.lower()
        ordered_cols += [f"has_{cl}", f"{cl}_total", f"{cl}_iphone", f"{cl}_ipad", f"{cl}_appletv", f"path_{cl}"]
    ordered_cols += ["has_both", "countries_with_screenshots"]

    # 存在する列のみ出力
    ordered_cols = [c for c in ordered_cols if c in out_df.columns] + [c for c in out_df.columns if c not in ordered_cols]
    out_df = out_df.reindex(columns=ordered_cols)

    out_path = Path(args.out)
    out_df.to_csv(out_path, index=False, quoting=csv.QUOTE_MINIMAL)
    print(f"Wrote {out_path}  (rows={len(out_df)})")

if __name__ == "__main__":
    main()
