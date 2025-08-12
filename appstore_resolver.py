#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse, csv, json, time
from pathlib import Path
from typing import Dict, Any, List, Optional, Tuple
import requests
import pandas as pd
from rapidfuzz import fuzz, process
from tqdm import tqdm

ITUNES_SEARCH = "https://itunes.apple.com/search"
ITUNES_LOOKUP = "https://itunes.apple.com/lookup"

DEFAULT_HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; AppResolver/1.0)"
}

HEALTHY_GENRES = {"Health & Fitness", "Medical"}  # 妊活系の重み付け用

def search_apps(term: str, country: str, lang: str, limit: int) -> List[Dict[str, Any]]:
    params = {
        "term": term,
        "entity": "software",
        "country": country,
        "lang": lang,
        "limit": limit,
        "media": "software",
    }
    r = requests.get(ITUNES_SEARCH, params=params, headers=DEFAULT_HEADERS, timeout=25)
    r.raise_for_status()
    data = r.json()
    return data.get("results", [])

def lookup(track_id: int, country: str, lang: str) -> Optional[Dict[str, Any]]:
    params = {"id": track_id, "entity": "software", "country": country, "lang": lang}
    r = requests.get(ITUNES_LOOKUP, params=params, headers=DEFAULT_HEADERS, timeout=25)
    r.raise_for_status()
    data = r.json()
    if data.get("resultCount", 0) == 0:
        return None
    return data["results"][0]

def to_set(vals) -> set:
    if not vals: return set()
    if isinstance(vals, str):
        return {vals}
    return set([str(v) for v in vals if v])

def norm(s: Optional[str]) -> str:
    return (s or "").strip().lower()

def score_candidate(
    qname: str,
    cand: Dict[str, Any],
    match_modes: List[str],
    aliases: List[str],
    developer_hint: Optional[str],
    bundle_hint: Optional[str],
) -> Tuple[float, Dict[str, float]]:
    """候補スコア計算（詳細内訳も返す）"""
    tname = cand.get("trackName") or ""
    seller = cand.get("sellerName") or cand.get("artistName") or ""
    bundle = cand.get("bundleId") or ""
    pgenre = cand.get("primaryGenreName") or ""
    genres = set(cand.get("genres", []))

    # 1) 名前類似度
    name_scores = []
    details = {}

    def add_score(label, val):
        name_scores.append(val); details[label] = val

    qn = norm(qname)
    tn = norm(tname)
    alns = [norm(a) for a in aliases if a]

    if "exact" in match_modes:
        exact = 100.0 if tn == qn or any(tn == a for a in alns) else 0.0
        add_score("exact", exact)

    if "startswith" in match_modes:
        ssw = 92.0 if (tn.startswith(qn) or any(tn.startswith(a) for a in alns)) else 0.0
        add_score("startswith", ssw)

    if "contains" in match_modes:
        cont = 88.0 if (qn in tn or any(a in tn for a in alns)) else 0.0
        add_score("contains", cont)

    if "fuzzy" in match_modes:
        base = max(fuzz.WRatio(qname, tname), *(fuzz.WRatio(a, tname) for a in alns)) if alns else fuzz.WRatio(qname, tname)
        add_score("fuzzy", float(base))

    name_score = max(name_scores) if name_scores else 0.0

    # 2) 開発元ヒント
    dev_bonus = 0.0
    if developer_hint:
        if norm(developer_hint) in norm(seller):
            dev_bonus = 8.0
        else:
            # 緩くファジー
            dev_bonus = 4.0 if fuzz.partial_ratio(developer_hint, seller) >= 80 else 0.0
    details["dev_bonus"] = dev_bonus

    # 3) バンドルIDヒント
    bundle_bonus = 0.0
    if bundle_hint:
        if norm(bundle_hint) == norm(bundle):
            bundle_bonus = 25.0
        elif norm(bundle_hint) in norm(bundle):
            bundle_bonus = 12.0
    details["bundle_bonus"] = bundle_bonus

    # 4) ジャンル重み（妊活系を仮に重み付け）
    genre_bonus = 0.0
    if pgenre in HEALTHY_GENRES or (HEALTHY_GENRES & genres):
        genre_bonus = 3.0
    details["genre_bonus"] = genre_bonus

    total = name_score + dev_bonus + bundle_bonus + genre_bonus
    details["total"] = total
    return total, details

def pick_winner(scored: List[Tuple[Dict[str, Any], float, Dict[str, float]]], min_score: float, min_gap: float):
    """スコア上位から自動確定するか判定"""
    if not scored:
        return None, []
    ranked = sorted(scored, key=lambda x: x[1], reverse=True)
    top = ranked[0]
    if top[1] < min_score:
        return None, ranked
    if len(ranked) >= 2 and (top[1] - ranked[1][1]) < min_gap:
        return None, ranked
    return top, ranked

def load_inputs(args) -> List[Dict[str, Any]]:
    rows = []
    if args.input_names:
        with open(args.input_names, "r", encoding="utf-8") as f:
            for line in f:
                q = line.strip()
                if q:
                    rows.append({"app_key": q, "query_name": q, "developer_hint": "", "bundle_hint": "", "aliases": []})
    elif args.input_csv:
        df = pd.read_csv(args.input_csv)
        for _, r in df.iterrows():
            aliases = []
            for c in df.columns:
                if str(c).lower().startswith("alias"):
                    v = r[c]
                    if isinstance(v, str) and v.strip():
                        aliases.append(v.strip())
            rows.append({
                "app_key": str(r.get("app_key", r.get("query_name", ""))) or str(r.get("query_name", "")),
                "query_name": str(r.get("query_name", "")),
                "developer_hint": str(r.get("developer_hint", "")) if not pd.isna(r.get("developer_hint", "")) else "",
                "bundle_hint": str(r.get("bundle_hint", "")) if not pd.isna(r.get("bundle_hint", "")) else "",
                "aliases": aliases
            })
    else:
        raise ValueError("Provide --input-names or --input-csv")
    return rows

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input-names", help="1行1アプリ名のtxt（言語混在OK）")
    ap.add_argument("--input-csv", help="CSV: query_name,developer_hint,bundle_hint,alias_1,alias_2,...")
    ap.add_argument("--outdir", default="out_resolve")
    ap.add_argument("--countries", nargs="+", default=["gb","jp"])
    ap.add_argument("--lang-map", default="gb=en_us,jp=ja_jp")
    ap.add_argument("--limit-per-country", type=int, default=25)
    ap.add_argument("--sleep", type=float, default=0.4)
    ap.add_argument("--match-mode", nargs="+", default=["startswith","contains","fuzzy"])  # exact/startswith/contains/fuzzy
    ap.add_argument("--min-score", type=float, default=80.0)
    ap.add_argument("--min-gap", type=float, default=8.0)
    args = ap.parse_args()

    outdir = Path(args.outdir); outdir.mkdir(parents=True, exist_ok=True)

    # country→lang
    lang_map = {}
    if args.lang_map:
        for pair in args.lang_map.split(","):
            c, l = pair.split("=")
            lang_map[c.strip().lower()] = l.strip()
    for c in args.countries:
        lang_map.setdefault(c.lower(), "en_us")

    rows = load_inputs(args)

    master_rows = []       # 確定ID台帳
    needs_review_rows = [] # あいまい案件
    cand_rows = []         # 監査用全候補

    # trackIdで重複を統一
    existing_track_ids = set()

    for row in tqdm(rows, desc="Resolve"):
        app_key = row["app_key"]
        qname = row["query_name"]
        dev_hint = row["developer_hint"]
        bundle_hint = row["bundle_hint"]
        aliases = row["aliases"]

        # 国横断で候補収集（trackId重複はまとめる）
        candidates_by_id: Dict[int, Dict[str, Any]] = {}

        for country in args.countries:
            lang = lang_map[country.lower()]
            try:
                results = search_apps(qname, country, lang, args.limit_per_country)
            except Exception as e:
                print(f"[WARN] search failed {country} {qname}: {e}")
                results = []

            for rec in results:
                tid = rec.get("trackId")
                if not tid:
                    continue
                # 代表（最初の国の結果）を基本に、国を記録
                if tid not in candidates_by_id:
                    candidates_by_id[tid] = rec.copy()
                    candidates_by_id[tid]["_countries"] = {country.upper()}
                else:
                    candidates_by_id[tid]["_countries"].add(country.upper())

            time.sleep(args.sleep)

        # スコアリング
        scored = []
        for tid, rec in candidates_by_id.items():
            total, details = score_candidate(
                qname, rec, args.match_mode, aliases, dev_hint, bundle_hint
            )
            scored.append((rec, total, details))
            cand_rows.append({
                "app_key": app_key,
                "query_name": qname,
                "trackId": tid,
                "bundleId": rec.get("bundleId"),
                "trackName": rec.get("trackName"),
                "sellerName": rec.get("sellerName") or rec.get("artistName"),
                "primaryGenreName": rec.get("primaryGenreName"),
                "countries_found": ";".join(sorted(list(rec.get("_countries", set())))),
                **details
            })

        winner, ranked = pick_winner(scored, args.min_score, args.min_gap)

        if winner is None:
            # 自動確定できない → needs_review
            # 上位最大5件だけ書き出し
            for rec, score, details in ranked[:5]:
                needs_review_rows.append({
                    "app_key": app_key,
                    "query_name": qname,
                    "trackId": rec.get("trackId"),
                    "bundleId": rec.get("bundleId"),
                    "trackName": rec.get("trackName"),
                    "sellerName": rec.get("sellerName") or rec.get("artistName"),
                    "primaryGenreName": rec.get("primaryGenreName"),
                    "countries_found": ";".join(sorted(list(rec.get("_countries", set())))),
                    "score_total": score,
                    "score_breakdown": json.dumps(details, ensure_ascii=False)
                })
            continue

        # 勝者（自動確定）
        rec, total, details = winner
        track_id = rec.get("trackId")
        if track_id in existing_track_ids:
            # すでに登録済みならスキップ（別名で同じアプリを指していたケース）
            continue
        existing_track_ids.add(track_id)

        # 代表国で lookup して bundleIdや名前を最終確定（失敗しても検索結果を使う）
        final = rec
        for country in args.countries:
            try:
                looked = lookup(track_id, country, lang_map[country.lower()])
                if looked and looked.get("bundleId"):
                    final = looked
                    break
            except Exception:
                pass
            time.sleep(args.sleep)

        master_rows.append({
            "app_key": app_key,
            "query_name": qname,
            "trackId": final.get("trackId"),
            "bundleId": final.get("bundleId"),
            "trackName": final.get("trackName"),
            "sellerName": final.get("sellerName") or final.get("artistName"),
            "primaryGenreName": final.get("primaryGenreName"),
            "languageCodesISO2A": ";".join(final.get("languageCodesISO2A", [])),
            "releaseDate": final.get("releaseDate"),
            "countries_found": ";".join(sorted(list(rec.get("_countries", set())))),
            "score_total": total,
            "score_breakdown": json.dumps(details, ensure_ascii=False)
        })

    # 保存
    pd.DataFrame(master_rows).to_csv(outdir / "apps_master.csv", index=False, quoting=csv.QUOTE_MINIMAL)
    pd.DataFrame(needs_review_rows).to_csv(outdir / "needs_review.csv", index=False, quoting=csv.QUOTE_MINIMAL)
    pd.DataFrame(cand_rows).to_csv(outdir / "candidates_raw.csv", index=False, quoting=csv.QUOTE_MINIMAL)

    print(f"Saved: {outdir/'apps_master.csv'}")
    print(f"Saved: {outdir/'needs_review.csv'}  (manual check)")
    print(f"Saved: {outdir/'candidates_raw.csv'}  (audit)")

if __name__ == "__main__":
    main()