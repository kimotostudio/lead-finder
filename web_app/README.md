# Lead Finder Web Application

個人経営のカウンセリング・セラピー・ウェルネスビジネス向けリード生成Webアプリケーション。

47都道府県・778都市対応 / 2パス検索 / JP偏重クエリ / AIフィルタ & AI弱さ検証

---

## セットアップ

```bash
cd web_app
pip install -r requirements.txt

# AI機能を使う場合
pip install openai
export OPENAI_API_KEY=sk-...   # Windows: $env:OPENAI_API_KEY = "sk-..."

python app.py
# http://localhost:5000
```

---

## 使い方

1. **エリア選択**: 地方 → 都道府県 → 都市（複数選択可、全選択ボタンあり）
2. **業種選択**: チップUIで複数業種を選択
3. **ターゲットプリセット**: 個人事業主優先 / 小規模事業者 / 全規模対象
4. **詳細設定**（任意）:
   - 取得件数（5/10/20/50/100件）
   - スコア範囲、弱さ閾値、個人度フィルタ
   - AIフィルタ（ポータル・SNS・海外等を自動除外）
   - AI弱さ検証（上位リードのWEAK/NOT_WEAK判定）
5. **検索開始** → プログレスバー → 結果カード → CSVダウンロード

---

## 検索パイプライン

```
クエリ生成 (2-Pass: Pass1=5本/ペア, Pass2=結果不足時に拡張)
  → URL収集 (DuckDuckGo)
  → プレフィルタ (ドメイン上限3件, ブロックURL除外, 海外TLD除外)
  → ハードフィルタ (ポータル/SNS/ディレクトリ等の既知パターン)
  → [任意] AI関連性ゲート (pre-crawl, URL単位)
  → URL優先度ソート (.jp優先, ルートパス優先, 独自ドメイン優先)
  → プレチェック (HEAD要求で生存確認)
  → 並列クロール & 処理 (5ワーカー)
  → 弱さスコアリング (0-100) & 個人事業主分類 (solo/small/corporate/unknown)
  → スコアフィルタ & ソロフィルタ
  → [任意] AIフィルタ (post-crawl, フラグ付き除外)
  → [任意] AI弱さ検証 (GPT-4o-mini, WEAK/NOT_WEAK)
  → CSV保存 & JSON結果表示
```

### 2パス検索戦略

| パス | クエリ数/ペア | 内容 |
|------|-------------|------|
| Pass 1 | 5本 | `{city} {btype}` + `料金` + `予約` + `site:.jp` + `公式` |
| Pass 2 | 7本+ | `個人`/`自宅` + `site:peraichi.com`/`site:jimdofree.com` + スピリチュアル拡張 |

Pass 2 は Pass 1 の収集URLが `MIN_URLS_PER_PAIR`（8件）未満の場合のみ実行。

### JP偏重検索

- `site:.jp` クエリで日本ドメイン優先
- `公式` `営業時間` 等の日本語シグナルを付加
- 海外TLD（`.de`, `.fr`, `.co.uk` 等50+）を自動除外
- `.jp`ドメインをURL優先度ソートで上位に配置

---

## AI機能

### AIフィルタ（post-crawl）

クロール済みリードの実コンテンツをGPT-4o-miniで分析し、不要なリードをフラグ付きで除外。

**判定フラグ:**
| フラグ | 意味 |
|--------|------|
| `OVERSEAS` | 海外サイト |
| `NON_JP_LANG` | 非日本語コンテンツ |
| `PORTAL` | ポータル/一覧サイト |
| `SNS` | SNSプロフィール/リンクまとめ |
| `DIRECTORY` | 口コミ/ランキングサイト |
| `JOB_LISTING` | 求人サイト |
| `PDF_OR_FILE` | PDF/非Webページ |
| `IRRELEVANT_INDUSTRY` | 無関係業種 |
| `NO_LOCAL_SIGNAL` | 地域シグナルなし |

**出力フィールド:**
- `ai_action`: KEEP / DROP
- `ai_flags`: 該当フラグ配列
- `ai_filter_reason`: 除外理由
- `ai_filter_confidence`: 確信度 1-10（6未満はKEEP優先）

### AI弱さ検証

上位N件のリードを「本当に弱いサイトか」AIが再判定。

**出力フィールド:**
- `ai_verified`: true (WEAK) / false (NOT_WEAK)
- `ai_reason`: 判定理由
- `ai_confidence`: 確信度 1-10

### UIでの表示

- **結果カード**: AI弱さ判定（WEAK=黄色バッジ / NOT_WEAK=緑バッジ）、AIフィルタ結果（KEEP=青 / DROP=赤+フラグ表示）
- **サマリー行**: 結果上部にAIフィルタ統計（検査件数/保持/除外/フラグ内訳）とAI検証統計（弱/強件数）

### コスト目安

| 件数 | 推定コスト |
|------|-----------|
| 10件 | 約0.2円 |
| 30件 | 約0.6円 |
| 50件 | 約1円 |

---

## 設定パラメータ

`app.py` 内の定数で調整:

| パラメータ | デフォルト | 説明 |
|-----------|----------|------|
| `MIN_URLS_PER_PAIR` | 8 | Pass 2 発動の閾値 |
| `MAX_URLS_TO_PROCESS` | 500 | クロール上限URL数 |
| `MAX_QUERIES_TOTAL` | 300 | 総クエリ数上限 |
| `LEAD_PROCESS_WORKERS` | 5 | 並列クロールワーカー数 |
| `AI_RELEVANCE_TOP_N` | 30 | AIフィルタ対象件数 |
| `AI_RELEVANCE_MIN_CONFIDENCE` | 6 | AIフィルタ最低確信度 |
| `MAX_URLS_PER_DOMAIN` | 5 | 1ドメインあたりの保持URL上限 |
| `PRECHECK_UNKNOWN_KEEP_RATIO` | 0.6 | 事前チェックunknownの保持率（弱い個人サイト救済） |
| `PRECHECK_THIN_PENALTY` | 1 | 薄いページへの減点幅（0で無効） |
| `PRECHECK_NEGATIVE_KEEP_RATIO` | 0.2 | 事前チェックでマイナス評価URLを保持する比率 |
| `PRECHECK_FALLBACK_MIN_RATIO` | 0.4 | 事前チェック後の保持率がこれ未満なら全URLにフォールバック |
| `PAIR_MIN_FILTERED_URLS` | 2 | 都市×業種ペアごとに確保したい最低URL数 |
| `PAIR_RESCUE_MAX_PER_PAIR` | 4 | 都市×業種ペア救済時に追加する最大URL数 |
| `FOREIGN_FILTER_MODE` | balanced | `strict`=海外TLDを全除外 / `balanced`=JPシグナルURLは救済 |
| `ALLOW_SOLO_PLATFORM_SITES` | true | Ameblo/Note/Peraichi等の個人向け基盤を除外しない |

---

## CSV出力

UTF-8 with BOM（Excel/Sheets互換）。日本語ヘッダー。弱さスコア降順ソート。

| 列グループ | フィールド |
|-----------|-----------|
| 基本情報 | store_name, url, comment, score |
| フィルタ | filter_reason, score_boost, boost_reasons |
| 弱さ分析 | weakness_score, weakness_grade, weakness_reasons |
| 個人事業主分析 | solo_score, solo_classification, solo_reasons, solo_evidence_snippets, solo_detected_corp_terms, solo_boost, solo_boost_reasons |
| URL状態 | url_status, error_code |
| 地域・業種 | region, city, business_type, site_type |
| 連絡先 | phone, email |
| メタ | source_query, fetched_at_iso |
| Liveness | http_status, final_url, is_alive, checked_at_iso |
| AI弱さ検証 | ai_verified, ai_reason, ai_confidence |
| AIフィルタ | ai_action, ai_flags, ai_filter_reason, ai_filter_confidence |

---

## API仕様

### POST /api/search

検索開始。

```json
{
  "prefecture": "東京都",
  "cities": ["新宿区", "渋谷区"],
  "business_types": ["カウンセリング", "コーチング"],
  "limit": 10,
  "min_score": 20,
  "max_score": null,
  "solo_priority": true,
  "solo_classifications": ["solo", "small", "unknown"],
  "solo_score_min": null,
  "solo_score_max": null,
  "min_weakness": 0,
  "use_ai_verify": false,
  "ai_top_n": 30,
  "use_ai_relevance": false,
  "ai_relevance_top_n": 30
}
```

### GET /api/progress

検索進捗取得。ステータス: `idle` / `starting` / `running` / `completed` / `error` / `cancelled`

完了時は `results`（リード配列）、`csv_path`、`stats`（パイプライン統計）を含む。

### POST /api/cancel

実行中の検索をキャンセル。

### GET /api/download/\<filename\>

CSV ダウンロード。

### GET /api/regions

全地方・都道府県データ取得。

### GET /api/prefectures/\<region\>

地方内の都道府県リスト取得。

### GET /api/cities/\<prefecture\>

都道府県内の都市リスト取得。

### GET /api/business-types

カテゴリ別業種リスト取得（`categories` とフラットな `all` を返す）。

---

## ファイル構成

```
web_app/
├── app.py                 # Flaskアプリ + 検索パイプライン
├── templates/
│   └── index.html         # SPAテンプレート (Bootstrap 5)
├── static/
│   ├── js/app.js          # フロントエンドJS (jQuery)
│   └── css/style.css      # カスタムCSS (プロSaaSデザイン)
├── output/                # CSV出力先 (自動生成)
└── logs/                  # ログファイル (自動生成)
```

依存する親ディレクトリのモジュール:

| モジュール | 役割 |
|-----------|------|
| `src/ai_verifier.py` | AIフィルタ & AI弱さ検証 (GPT-4o-mini) |
| `src/processor.py` | リード処理パイプライン |
| `src/scorer.py` | スコアリング |
| `src/normalize.py` | CSV正規化 & スキーマ定義 |
| `src/output_writer.py` | CSV書き出し |
| `src/weakness.py` | 弱さスコア計算 |
| `src/solo_classifier.py` | 個人事業主判定 |
| `src/solo_boost.py` | ソロブースト計算 |
| `config/cities_data.py` | 47都道府県・778都市データ |

---

## テスト

```bash
# プロジェクトルートから
python -m pytest tests/ -v
```

112テスト: URLフィルタ、2パスクエリ生成、JP偏重クエリ、海外TLD除外、弱さスコア、個人事業主分類、CSV出力。

---

## トラブルシューティング

| 症状 | 対処 |
|------|------|
| ポート5000が使用中 | `$env:PORT=8000; python app.py` |
| モジュールが見つからない | `web_app/` からではなくプロジェクトルートで確認 |
| 検索が開始されない | 都市と業種が選択されているか確認、`logs/app.log` を確認 |
| AI機能が動かない | `pip install openai` と `OPENAI_API_KEY` 環境変数を確認 |
| CSVが空 | 検索が正常完了しているか確認、`output/` の権限確認 |

---

## License

MIT License
