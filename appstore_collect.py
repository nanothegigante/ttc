#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
App Store metadata/screenshot/review collector (ID-first workflow)

- Accepts resolver output (apps_master.csv) via --input-ids
- Looks up per-country (GB/JP etc.) with language override
- Saves metadata.csv, optional reviews.csv, and screenshots/

Columns expected in --input-ids CSV:
  - trackId (preferred) or bundleId (fallback)
  - app_key (optional, carried through for your joins)
  - query_name (optional)

Usage:
  python appstore_collect.py \
    --input-ids out_resolve/apps_master.csv \
    --outdir out_collect \
    --countries gb jp \
    --lang-map gb=en_us,jp=ja_jp \
    --save-reviews --reviews-per-country 50 \
    --max-screenshots 8 --sleep 0.7
"""

import argparse
import csv
import json
import os
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd
import requests
from tqdm import tqdm

ITUNES_LOOKUP = "https://itunes.apple.com/lookup"
REVIEWS_RSS = "https://itunes.apple.com/{country}/rss/customerreviews/id={track_id}/sortby=mostrecent/json"

DEFAULT_HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; AppCollector/1.1)"
}

def lookup_by_track_id(track_id: int, country: str, lang: str) -> Optional[Dict[str, Any]]:
    params = {"id": track_id, "entity": "software", "country": country, "lang": lang}
    r = requests.get(ITUNES_LOOKUP, params=params, headers=DEFAULT_HEADERS, timeout=30)
    r.raise_for_status()
    data = r.json()
    if data.get("resultCount", 0) == 0:
        return None
    return data["results"][0]

def lookup_by_bundle_id(bundle_id: str, country: str, lang: str) -> Optional[Dict[str, Any]]:
    # Apple Lookup API supports bundleId parameter as identifier
    params = {"bundleId": bundle_id, "entity": "software", "country": country, "lang": lang}
    r = requests.get(ITUNES_LOOKUP, params=params, headers=DEFAULT_HEADERS, timeout=30)
    r.raise_for_status()
    data = r.json()
    if data.get("resultCount", 0) == 0:
        return None
    return data["results"][0]

def infer_has_iap_guess(rec: Dict[str, Any]) -> Optional[bool]:
    desc = (rec.get("description") or "").lower()
    hints = ["subscription", "in-app", "premium", "upgrade", "課金", "サブスクリプション"]
    if any(h in desc for h in hints):
        return True
    return None  # unknown (APIではIAPフラグが常に取れないため)

def norm_meta(rec: Dict[str, Any], country: str, lang: str, app_key: Optional[str], query_name: Optional[str]) -> Dict[str, Any]:
    def j(val):
        try:
            return json.dumps(val or [], ensure_ascii=False)
        except Exception:
            return "[]"

    out = {
        "app_key": app_key,
        "query_name": query_name,
        "country": country.upper(),
        "lang": lang,
        "trackId": rec.get("trackId"),
        "bundleId": rec.get("bundleId"),
        "trackName": rec.get("trackName"),
        "sellerName": rec.get("sellerName") or rec.get("artistName"),
        "developerName": rec.get("artistName"),
        "description": rec.get("description"),
        "releaseDate": rec.get("releaseDate"),
        "currentVersionReleaseDate": rec.get("currentVersionReleaseDate"),
        "version": rec.get("version"),
        "primaryGenreName": rec.get("primaryGenreName"),
        "genres": ";".join(rec.get("genres", [])),
        "contentAdvisoryRating": rec.get("contentAdvisoryRating"),  # Age rating
        "languageCodesISO2A": ";".join(rec.get("languageCodesISO2A", [])),
        "averageUserRating": rec.get("averageUserRating"),
        "userRatingCount": rec.get("userRatingCount"),
        "averageUserRatingForCurrentVersion": rec.get("averageUserRatingForCurrentVersion"),
        "userRatingCountForCurrentVersion": rec.get("userRatingCountForCurrentVersion"),
        "price": rec.get("price"),
        "formattedPrice": rec.get("formattedPrice"),
        "currency": rec.get("currency"),
        "minimumOsVersion": rec.get("minimumOsVersion"),
        "supportedDevices_count": len(rec.get("supportedDevices", []) or []),
        "trackViewUrl": rec.get("trackViewUrl"),
        "sellerUrl": rec.get("sellerUrl"),
        "has_in_app_purchases_guess": infer_has_iap_guess(rec),
        # Raw arrays (JSON)
        "screenshotUrls_json": j(rec.get("screenshotUrls")),
        "ipadScreenshotUrls_json": j(rec.get("ipadScreenshotUrls")),
        "appletvScreenshotUrls_json": j(rec.get("appletvScreenshotUrls")),
    }
    return out

def download_screenshots(urls: List[str], outdir: Path, max_count: int, sleep_sec: float):
    outdir.mkdir(parents=True, exist_ok=True)
    for i, url in enumerate(urls[:max_count], 1):
        try:
            ext = ".jpg"
            lower = url.lower()
            if ".png" in lower: ext = ".png"
            elif ".jpeg" in lower: ext = ".jpeg"
            fp = outdir / f"{i:02d}{ext}"
            if fp.exists():
                continue
            resp = requests.get(url, headers=DEFAULT_HEADERS, timeout=40)
            resp.raise_for_status()
            fp.write_bytes(resp.content)
            time.sleep(sleep_sec)
        except Exception as e:
            print(f"[WARN] screenshot DL failed: {url} -> {e}")

def fetch_reviews(track_id: int, country: str, limit: int) -> List[Dict[str, Any]]:
    url = REVIEWS_RSS.format(country=country.lower(), track_id=track_id)
    try:
        r = requests.get(url, headers=DEFAULT_HEADERS, timeout=30)
        r.raise_for_status()
        data = r.json()
    except Exception:
        return []
    feed = data.get("feed", {})
    entries = feed.get("entry", [])
    reviews: List[Dict[str, Any]] = []
    for e in entries:
        if "im:rating" in e and "im:version" in e:
            reviews.append({
                "country": country.upper(),
                "trackId": track_id,
                "author": e.get("author", {}).get("name", {}).get("label"),
                "title": e.get("title", {}).get("label"),
                "content": e.get("content", {}).get("label"),
                "rating": e.get("im:rating", {}).get("label"),
                "version": e.get("im:version", {}).get("label"),
                "updated": e.get("updated", {}).get("label"),
                "id": e.get("id", {}).get("label"),
            })
            if len(reviews) >= limit:
                break
    return reviews

def read_ids_csv(path: Path) -> List[Dict[str, Any]]:
    df = pd.read_csv(path)
    rows: List[Dict[str, Any]] = []
    for _, r in df.iterrows():
        rows.append({
            "app_key": None if pd.isna(r.get("app_key", None)) else str(r.get("app_key")),
            "query_name": None if pd.isna(r.get("query_name", None)) else str(r.get("query_name")),
            "trackId": None if pd.isna(r.get("trackId", None)) else int(r.get("trackId")),
            "bundleId": None if pd.isna(r.get("bundleId", None)) else str(r.get("bundleId")),
        })
    return rows

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input-ids", required=True, help="resolver出力CSV（apps_master.csv）。列: trackId または bundleId（両方可）")
    ap.add_argument("--outdir", default="out_collect")
    ap.add_argument("--countries", nargs="+", default=["gb","jp"])
    ap.add_argument("--lang-map", default="gb=en_us,jp=ja_jp")
    ap.add_argument("--save-reviews", action="store_true")
    ap.add_argument("--reviews-per-country", type=int, default=50)
    ap.add_argument("--max-screenshots", type=int, default=12, help="各デバイス種別(Phone/iPad/TV)ごとの最大DL枚数")
    ap.add_argument("--sleep", type=float, default=0.5, help="API呼び出し間のsleep秒")
    args = ap.parse_args()

    outdir = Path(args.outdir); outdir.mkdir(parents=True, exist_ok=True)
    ss_dir = outdir / "screenshots"; ss_dir.mkdir(exist_ok=True)

    # country -> lang
    lang_map: Dict[str, str] = {}
    if args.lang_map:
        for pair in args.lang_map.split(","):
            c, l = pair.split("=")
            lang_map[c.strip().lower()] = l.strip()
    for c in args.countries:
        lang_map.setdefault(c.lower(), "en_us")

    id_rows = read_ids_csv(Path(args.input_ids))

    meta_rows: List[Dict[str, Any]] = []
    review_rows: List[Dict[str, Any]] = []

    for base in tqdm(id_rows, desc="Apps"):
        track_id = base.get("trackId")
        bundle_id = base.get("bundleId")
        app_key = base.get("app_key")
        query_name = base.get("query_name")

        for country in args.countries:
            lang = lang_map[country.lower()]
            rec: Optional[Dict[str, Any]] = None

            try:
                if track_id:
                    rec = lookup_by_track_id(track_id, country, lang)
                elif bundle_id:
                    rec = lookup_by_bundle_id(bundle_id, country, lang)
                else:
                    print(f"[WARN] no ID for row app_key={app_key}, query_name={query_name}")
                    continue
            except requests.HTTPError as e:
                print(f"[HTTP {country} id={track_id or bundle_id}] {e}")
                time.sleep(args.sleep)
                continue
            except Exception as e:
                print(f"[ERROR {country} id={track_id or bundle_id}] {e}")
                time.sleep(args.sleep)
                continue

            if not rec:
                # その国のストアでは見つからない可能性もある
                time.sleep(args.sleep)
                continue

            # メタデータ
            meta = norm_meta(rec, country, lang, app_key, query_name)
            meta_rows.append(meta)

            # スクリーンショット（iPhone）
            ph_urls = rec.get("screenshotUrls", []) or []
            if ph_urls:
                dest = ss_dir / country.lower() / str(rec.get("trackId")) / "iphone"
                download_screenshots(ph_urls, dest, args.max_screenshots, sleep_sec=0.2)

            # iPad
            ipad_urls = rec.get("ipadScreenshotUrls", []) or []
            if ipad_urls:
                dest = ss_dir / country.lower() / str(rec.get("trackId")) / "ipad"
                download_screenshots(ipad_urls, dest, args.max_screenshots, sleep_sec=0.2)

            # Apple TV
            tv_urls = rec.get("appletvScreenshotUrls", []) or []
            if tv_urls:
                dest = ss_dir / country.lower() / str(rec.get("trackId")) / "appletv"
                download_screenshots(tv_urls, dest, args.max_screenshots, sleep_sec=0.2)

            # レビュー（任意）
            if args.save_reviews and rec.get("trackId"):
                try:
                    reviews = fetch_reviews(int(rec["trackId"]), country, args.reviews_per_country)
                    review_rows.extend(reviews)
                except Exception:
                    pass

            time.sleep(args.sleep)

    # 保存
    meta_df = pd.DataFrame(meta_rows)
    # 列順の見やすさを少し整える
    preferred_cols = [
        "app_key","query_name","country","lang","trackId","bundleId","trackName",
        "sellerName","developerName","primaryGenreName","genres",
        "contentAdvisoryRating","languageCodesISO2A",
        "averageUserRating","userRatingCount",
        "averageUserRatingForCurrentVersion","userRatingCountForCurrentVersion",
        "price","formattedPrice","currency",
        "minimumOsVersion","supportedDevices_count",
        "releaseDate","currentVersionReleaseDate","version",
        "has_in_app_purchases_guess",
        "trackViewUrl","sellerUrl",
        "screenshotUrls_json","ipadScreenshotUrls_json","appletvScreenshotUrls_json",
        "description"
    ]
    # 存在する列だけ並べ替え
    cols = [c for c in preferred_cols if c in meta_df.columns] + [c for c in meta_df.columns if c not in preferred_cols]
    meta_df = meta_df.reindex(columns=cols)
    meta_df.to_csv(outdir / "metadata.csv", index=False, quoting=csv.QUOTE_MINIMAL)

    if review_rows:
        rv_df = pd.DataFrame(review_rows)
        rv_df.to_csv(outdir / "reviews.csv", index=False, quoting=csv.QUOTE_MINIMAL)

    print(f"Saved: {outdir/'metadata.csv'}")
    if review_rows:
        print(f"Saved: {outdir/'reviews.csv'}")
    print(f"Screenshots: {ss_dir.resolve()}")

if __name__ == "__main__":
    main()




"""各国横断検索のステップがなくて、ID運用じゃない版"""
"""appstore_resolver.pyを食わせない版"""

# import argparse, csv, json, os, re, time
# from pathlib import Path
# from typing import Dict, Any, List, Optional
# import requests
# from rapidfuzz import process, fuzz
# from tqdm import tqdm
# import pandas as pd

# ITUNES_SEARCH = "https://itunes.apple.com/search"
# ITUNES_LOOKUP = "https://itunes.apple.com/lookup"
# # レビューRSS（JSON）
# REVIEWS_RSS = "https://itunes.apple.com/{country}/rss/customerreviews/id={track_id}/sortby=mostrecent/json"

# DEFAULT_HEADERS = {
#     "User-Agent": "Mozilla/5.0 (compatible; ResearchBot/1.0; +https://example.org)"
# }

# def search_app_by_name(name: str, country: str, lang: str, limit: int = 10) -> Optional[Dict[str, Any]]:
#     """名前で検索 → 最も近い結果を1件返す（ファジーマッチ）"""
#     params = {
#         "term": name,
#         "entity": "software",
#         "country": country,
#         "lang": lang,
#         "limit": limit,
#         "media": "software",
#     }
#     r = requests.get(ITUNES_SEARCH, params=params, headers=DEFAULT_HEADERS, timeout=20)
#     r.raise_for_status()
#     data = r.json()
#     if data.get("resultCount", 0) == 0:
#         return None

#     # ファジーマッチ：trackNameを基準に最良を選択
#     choices = {i: d.get("trackName","") for i, d in enumerate(data["results"])}
#     best = process.extractOne(name, choices, scorer=fuzz.WRatio)
#     if not best:
#         return data["results"][0]
#     best_idx = best[2]
#     return data["results"][best_idx]

# def lookup_by_id(track_id: int, country: str, lang: str) -> Optional[Dict[str, Any]]:
#     params = {"id": track_id, "entity": "software", "country": country, "lang": lang}
#     r = requests.get(ITUNES_LOOKUP, params=params, headers=DEFAULT_HEADERS, timeout=20)
#     r.raise_for_status()
#     data = r.json()
#     if data.get("resultCount", 0) == 0:
#         return None
#     return data["results"][0]

# def infer_has_iap(rec: Dict[str, Any]) -> Optional[bool]:
#     """IAP有無の推定（完全ではない）"""
#     # 1) price が 0 かつ 説明文に subscription/premium/課金 など
#     desc = (rec.get("description") or "").lower()
#     hints = ["subscription", "in-app", "in-app", "premium", "upgrade", "課金", "サブスクリプション"]
#     if any(h in desc for h in hints):
#         return True
#     # 2) formattedPrice が "Free" でも IAP がある場合は store で表示される（API未提供こと多し）
#     # 不明な場合は None
#     return None

# def normalize_record(raw: Dict[str, Any], country: str, lang: str) -> Dict[str, Any]:
#     def arr(key): return raw.get(key) if isinstance(raw.get(key), list) else []
#     rec = {
#         "country": country.upper(),
#         "lang": lang,
#         "trackId": raw.get("trackId"),
#         "bundleId": raw.get("bundleId"),
#         "trackName": raw.get("trackName"),
#         "sellerName": raw.get("sellerName"),
#         "developerName": raw.get("artistName"),
#         "description": raw.get("description"),
#         "releaseDate": raw.get("releaseDate"),
#         "currentVersionReleaseDate": raw.get("currentVersionReleaseDate"),
#         "primaryGenreName": raw.get("primaryGenreName"),
#         "genres": ";".join(raw.get("genres", [])),
#         "contentAdvisoryRating": raw.get("contentAdvisoryRating"),  # Age rating
#         "languageCodesISO2A": ";".join(raw.get("languageCodesISO2A", [])),
#         "averageUserRating": raw.get("averageUserRating"),
#         "userRatingCount": raw.get("userRatingCount"),
#         "averageUserRatingForCurrentVersion": raw.get("averageUserRatingForCurrentVersion"),
#         "userRatingCountForCurrentVersion": raw.get("userRatingCountForCurrentVersion"),
#         "price": raw.get("price"),
#         "formattedPrice": raw.get("formattedPrice"),
#         "currency": raw.get("currency"),
#         "minimumOsVersion": raw.get("minimumOsVersion"),
#         "supportedDevices_count": len(arr("supportedDevices")),
#         "screenshotUrls": json.dumps(raw.get("screenshotUrls", []), ensure_ascii=False),
#         "ipadScreenshotUrls": json.dumps(raw.get("ipadScreenshotUrls", []), ensure_ascii=False),
#         "appletvScreenshotUrls": json.dumps(raw.get("appletvScreenshotUrls", []), ensure_ascii=False),
#         "sellerUrl": raw.get("sellerUrl"),
#         "trackViewUrl": raw.get("trackViewUrl"),
#         "has_in_app_purchases_guess": infer_has_iap(raw),
#     }
#     return rec

# def download_screenshots(urls: List[str], outdir: Path, sleep_sec: float = 0.2):
#     outdir.mkdir(parents=True, exist_ok=True)
#     for i, url in enumerate(urls, 1):
#         try:
#             ext = ".jpg"
#             if ".png" in url.lower(): ext = ".png"
#             fp = outdir / f"{i:02d}{ext}"
#             if fp.exists(): continue
#             resp = requests.get(url, headers=DEFAULT_HEADERS, timeout=30)
#             resp.raise_for_status()
#             fp.write_bytes(resp.content)
#             time.sleep(sleep_sec)
#         except Exception as e:
#             print(f"[WARN] screenshot DL failed: {url} -> {e}")

# def fetch_reviews(track_id: int, country: str, limit: int = 50) -> List[Dict[str, Any]]:
#     """レビューRSS(JSON)から最近のレビューを取得（最大limit、国別）。"""
#     url = REVIEWS_RSS.format(country=country.lower(), track_id=track_id)
#     try:
#         r = requests.get(url, headers=DEFAULT_HEADERS, timeout=20)
#         r.raise_for_status()
#         data = r.json()
#     except Exception:
#         return []

#     feed = data.get("feed", {})
#     entries = feed.get("entry", [])
#     # 先頭要素がアプリ本体情報の場合があるので、レビュー構造に合わせて弾く
#     reviews = []
#     for e in entries:
#         if "im:rating" in e and "im:version" in e:
#             reviews.append({
#                 "country": country.upper(),
#                 "trackId": track_id,
#                 "author": e.get("author", {}).get("name", {}).get("label"),
#                 "title": e.get("title", {}).get("label"),
#                 "content": e.get("content", {}).get("label"),
#                 "rating": e.get("im:rating", {}).get("label"),
#                 "version": e.get("im:version", {}).get("label"),
#                 "updated": e.get("updated", {}).get("label"),
#                 "id": e.get("id", {}).get("label"),
#             })
#         if len(reviews) >= limit:
#             break
#     return reviews

# def main():
#     ap = argparse.ArgumentParser()
#     ap.add_argument("--input", required=True, help="1行1アプリ名のtxt")
#     ap.add_argument("--outdir", default="out", help="出力ディレクトリ")
#     ap.add_argument("--countries", nargs="+", default=["gb","jp"], help="例: gb jp us")
#     ap.add_argument("--lang-map", default="", help="country→langの手動対応。例: gb=en_us,jp=ja_jp")
#     ap.add_argument("--save-reviews", action="store_true")
#     ap.add_argument("--reviews-per-country", type=int, default=50)
#     ap.add_argument("--sleep", type=float, default=0.4, help="API間のスリープ秒")
#     args = ap.parse_args()

#     outdir = Path(args.outdir)
#     outdir.mkdir(parents=True, exist_ok=True)
#     ss_dir = outdir / "screenshots"
#     ss_dir.mkdir(exist_ok=True)

#     # country → lang のデフォルト
#     lang_default = {"gb":"en_us", "jp":"ja_jp"}
#     if args.lang_map:
#         for pair in args.lang_map.split(","):
#             c, l = pair.split("=")
#             lang_default[c.lower()] = l

#     with open(args.input, "r", encoding="utf-8") as f:
#         app_names = [line.strip() for line in f if line.strip()]

#     meta_rows: List[Dict[str, Any]] = []
#     reviews_rows: List[Dict[str, Any]] = []

#     for name in tqdm(app_names, desc="Apps"):
#         for country in args.countries:
#             lang = lang_default.get(country.lower(), "en_us")
#             try:
#                 search_hit = search_app_by_name(name, country, lang)
#                 if not search_hit:
#                     print(f"[INFO] Not found in {country.upper()}: {name}")
#                     continue

#                 # より完全なフィールドが欲しいので lookup で再取得
#                 track_id = search_hit.get("trackId")
#                 rec = lookup_by_id(track_id, country, lang) or search_hit
#                 norm = normalize_record(rec, country, lang)
#                 meta_rows.append(norm)

#                 # スクショDL
#                 shot_urls = rec.get("screenshotUrls", [])
#                 if shot_urls:
#                     target_dir = ss_dir / country.lower() / str(rec.get("trackId"))
#                     download_screenshots(shot_urls, target_dir)

#                 # レビュー
#                 if args.save_reviews and track_id:
#                     rv = fetch_reviews(track_id, country, args.reviews_per_country)
#                     reviews_rows.extend(rv)

#                 time.sleep(args.sleep)
#             except requests.HTTPError as e:
#                 print(f"[HTTP {country} {name}] {e}")
#             except Exception as e:
#                 print(f"[ERROR {country} {name}] {e}")

#     # CSV保存
#     meta_df = pd.DataFrame(meta_rows)
#     meta_df.to_csv(outdir / "metadata.csv", index=False, quoting=csv.QUOTE_MINIMAL)

#     if args.save_reviews and reviews_rows:
#         rv_df = pd.DataFrame(reviews_rows)
#         rv_df.to_csv(outdir / "reviews.csv", index=False, quoting=csv.QUOTE_MINIMAL)

#     print(f"Saved: {outdir/'metadata.csv'}")
#     if args.save_reviews:
#         print(f"Saved: {outdir/'reviews.csv'}")

# if __name__ == "__main__":
#     main()
