#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
from pathlib import Path
from typing import List, Tuple, Dict
import pandas as pd
from PIL import Image
import streamlit as st

IMAGE_EXTS = {".jpg", ".jpeg", ".png", ".webp"}
DEVICE_DIRS = ["iphone", "ipad", "appletv"]

# ---------- helpers ----------

def coalesce_path(s: str | None) -> Path | None:
    if not s or str(s).strip() == "":
        return None
    p = Path(str(s)).expanduser()
    return p if p.exists() else None

@st.cache_data(show_spinner=False)
def load_csv(csv_path: str) -> pd.DataFrame:
    df = pd.read_csv(csv_path)
    # 最低限の列チェック
    need = {"trackId", "appName", "path_gb", "path_jp"}
    missing = need - set(df.columns)
    if missing:
        raise ValueError(f"CSVに必要な列が足りません: {missing}")
    # 表示用にソート
    return df.sort_values("appName").reset_index(drop=True)

def list_images_under(root: Path) -> List[Path]:
    """root配下から画像を再帰的に収集。存在しない場合は空"""
    if not root or not root.exists():
        return []
    out: List[Path] = []
    # まずはデバイス別サブフォルダを優先的に走査
    for sub in DEVICE_DIRS:
        d = root / sub
        if d.exists():
            for p in d.rglob("*"):
                if p.is_file() and p.suffix.lower() in IMAGE_EXTS:
                    out.append(p)
    # サブフォルダ外に直置きがあればそれも拾う
    for p in root.iterdir() if root.exists() else []:
        if p.is_file() and p.suffix.lower() in IMAGE_EXTS:
            out.append(p)
    # 重複排除＆安定ソート
    out = sorted(list(dict.fromkeys(out)), key=lambda p: p.as_posix())
    return out

def split_by_device(paths: List[Path]) -> Dict[str, List[Path]]:
    buckets = {d: [] for d in DEVICE_DIRS}
    buckets["other"] = []
    for p in paths:
        tagged = False
        for d in DEVICE_DIRS:
            if f"/{d}/" in p.as_posix() or p.parent.name.lower() == d:
                buckets[d].append(p)
                tagged = True
                break
        if not tagged:
            buckets["other"].append(p)
    for k in buckets:
        buckets[k] = sorted(buckets[k], key=lambda x: x.name)
    return buckets

def make_columns(n: int):
    n = max(1, min(n, 6))
    return st.columns(n, gap="small")

def show_images_grid(paths: List[Path], max_per_block: int, ncols: int, caption_prefix: str):
    paths = paths[:max_per_block]
    if not paths:
        st.info("No images.")
        return
    cols = make_columns(ncols)
    for i, p in enumerate(paths):
        with cols[i % ncols]:
            try:
                img = Image.open(p)
                st.image(img, caption=f"{caption_prefix}: {p.name}", use_container_width=True)
            except Exception as e:
                st.warning(f"Failed to open: {p.name} ({e})")

# ---------- main app ----------

def main():
    st.set_page_config(page_title="App Screenshots Viewer (GB/JP)", layout="wide")

    # CLI引数（streamlit起動時は使わなくてもOK）
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("--csv", default="screenshots_summary.csv")
    args, _ = parser.parse_known_args()

    st.title("App Screenshots Viewer")
    st.caption("Compare App Store screenshots by country (GB vs JP)")

    # CSVロード
    try:
        df = load_csv(args.csv)
    except Exception as e:
        st.error(f"CSV読み込みに失敗しました: {e}")
        st.stop()

    # サイドバー: 検索 & 絞り込み
    with st.sidebar:
        st.header("Filters")
        q = st.text_input("Search app name", value="").strip().lower()
        only_both = st.checkbox("GB と JP の両方あるアプリだけ表示", value=False)
        max_per_device = st.slider("各デバイス/国の最大表示枚数", 1, 30, 10)
        ncols = st.slider("グリッド列数（1国あたり）", 1, 6, 3)
        device_filter = st.multiselect(
            "表示するデバイス",
            options=["iphone", "ipad", "appletv", "other"],
            default=["iphone", "ipad", "appletv", "other"]
        )

    # 絞り込み
    view = df.copy()
    if q:
        view = view[view["appName"].str.lower().str.contains(q)]
    # パスの有無判定
    def has_any(path_col):
        return view[path_col].fillna("").str.strip().ne("")
    if only_both:
        view = view[has_any("path_gb") & has_any("path_jp")]

    if view.empty:
        st.warning("条件に合うアプリがありません。検索条件を緩めてください。")
        st.stop()

    # アプリ選択
    app_names = view["appName"].tolist()
    default_idx = 0
    sel_name = st.selectbox("Choose an app", app_names, index=default_idx, key="app_select")
    sel_row = view[view["appName"] == sel_name].iloc[0]

    track_id = str(sel_row["trackId"])
    path_gb = coalesce_path(sel_row.get("path_gb"))
    path_jp = coalesce_path(sel_row.get("path_jp"))

    st.subheader(f"{sel_name}  (trackId: {track_id})")
    cols_top = st.columns(2, gap="large")

    # 左: GB
    with cols_top[0]:
        st.markdown("### 🇬🇧 GB")
        if not path_gb:
            st.info("No GB screenshots.")
        else:
            gb_all = list_images_under(path_gb)
            gb_split = split_by_device(gb_all)
            total = sum(len(v) for v in gb_split.values())
            st.caption(f"Path: `{path_gb}` • {total} images")
            for dev in ["iphone","ipad","appletv","other"]:
                if dev not in device_filter:
                    continue
                imgs = gb_split.get(dev, [])
                if imgs:
                    with st.expander(f"{dev} ({len(imgs)})", expanded=(dev=="iphone")):
                        show_images_grid(imgs, max_per_device, ncols, caption_prefix=dev)

    # 右: JP
    with cols_top[1]:
        st.markdown("### 🇯🇵 JP")
        if not path_jp:
            st.info("No JP screenshots.")
        else:
            jp_all = list_images_under(path_jp)
            jp_split = split_by_device(jp_all)
            total = sum(len(v) for v in jp_split.values())
            st.caption(f"Path: `{path_jp}` • {total} images")
            for dev in ["iphone","ipad","appletv","other"]:
                if dev not in device_filter:
                    continue
                imgs = jp_split.get(dev, [])
                if imgs:
                    with st.expander(f"{dev} ({len(imgs)})", expanded=(dev=="iphone")):
                        show_images_grid(imgs, max_per_device, ncols, caption_prefix=dev)

    # 下部：切り替えしやすい簡易テーブル
    with st.expander("Show table"):
        st.dataframe(
            view[["trackId","appName","path_gb","path_jp"]].reset_index(drop=True),
            use_container_width=True,
            hide_index=True
        )

if __name__ == "__main__":
    main()
