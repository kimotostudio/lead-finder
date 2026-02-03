# Lead Finder System

小規模ビジネス（カウンセリング、コーチング、ヨガ、整体など）のリード発掘・スコアリングシステム。
47都道府県・778都市対応のWebアプリケーション + CLI パイプライン。

---

## 主な機能

- **2パス検索戦略**: Pass 1（粗いクエリ5本）→ 結果不足時に Pass 2（拡張クエリ）を自動実行
- **JP偏重検索**: `site:.jp`・`公式`・`営業時間` 等の日本語シグナルでノイズ低減、海外TLD自動除外
- **弱さスコアリング (0-100)**: デザインの古さ、予約システム欠如、料金不明等を自動検出
- **個人事業主分類**: solo / small / corporate / unknown の4段階判定
- **AIフィルタ (GPT-4o-mini)**: ポータル・SNS・海外サイト等をフラグ付きで自動除外
- **AI弱さ検証 (GPT-4o-mini)**: 上位リードの弱さをAIが再チェック
- **CSV出力**: 全フィールド（弱さ・個人度・AI判定含む）をCSVエクスポート
- **キャンセル機能**: 実行中の検索をいつでも中止可能

---

## クイックスタート

### 1. 環境準備

```powershell
# 仮想環境作成
python -m venv .venv
.\.venv\Scripts\activate

# 依存関係インストール
pip install -r requirements.txt
```

### 2. Webアプリ起動（推奨）

```powershell
cd web_app
python app.py
```

ブラウザで `http://localhost:5000` にアクセス。

### 3. AI機能を使う場合

```powershell
# OpenAI APIキーを設定
$env:OPENAI_API_KEY = "sk-..."
```

詳細設定パネルで「AIで関係ないサイトを除外」「AI検証」をONにする。

---

## Webアプリ使用方法

1. **エリア選択**: 地方 → 都道府県 → 都市（複数選択可）
2. **業種選択**: チップUIで複数業種を選択
3. **ターゲット設定**: プリセット（個人事業主優先 / 小規模 / 全規模）を選択
4. **詳細設定**（任意）:
   - 取得件数（5〜100件/クエリ）
   - スコア範囲・弱さ閾値・個人度フィルタ
   - AIフィルタ / AI検証の有効化
5. **検索開始** → プログレスバーで進捗確認 → 結果カードで確認 → CSV ダウンロード

---

## 検索パイプライン

```
検索クエリ生成 (2-Pass)
  → URL収集 (DuckDuckGo)
  → プレフィルタ (ドメイン重複制限, ブロックURL除外, 海外TLD除外)
  → ハードフィルタ (is_blocked_url)
  → [任意] AI関連性ゲート (pre-crawl, URL単位)
  → URL優先度ソート (.jp優先, ルートパス優先)
  → プレチェック (HEAD要求で生存確認)
  → クロール & 処理 (並列5ワーカー)
  → 弱さスコアリング & 個人事業主分類
  → スコアフィルタ & ソロフィルタ
  → [任意] AIフィルタ (post-crawl, フラグ付き)
  → [任意] AI弱さ検証 (GPT-4o-mini)
  → CSV保存 & 結果表示
```

### 2パス検索

| パス | クエリ数/ペア | 内容 |
|------|-------------|------|
| Pass 1 | 5本 | 基本クエリ + `料金`/`予約` + `site:.jp` + `公式` |
| Pass 2 | 7本+ | `個人`/`自宅` + プラットフォーム指定 + スピリチュアル系拡張 |

Pass 2 は Pass 1 の収集URLが `MIN_URLS_PER_PAIR`（デフォルト8）未満の場合のみ実行。

---

## AIフィルタ

### post-crawlフィルタ（`batch_filter_relevance`）

クロール済みリードの実コンテンツ（タイトル、テキスト）をGPT-4o-miniで分析。

**出力フィールド:**
| フィールド | 値 | 説明 |
|-----------|-----|------|
| `ai_action` | KEEP / DROP | 保持 or 除外 |
| `ai_flags` | 配列 | 該当フラグ（複数可） |
| `ai_filter_reason` | 文字列 | 除外理由（日本語） |
| `ai_filter_confidence` | 1-10 | 確信度（6未満はKEEP優先） |

**判定フラグ:**
- `OVERSEAS` — 海外サイト
- `NON_JP_LANG` — 非日本語
- `PORTAL` — ポータル/一覧サイト
- `SNS` — SNSプロフィール/リンクまとめ
- `DIRECTORY` — 口コミ/ランキングサイト
- `JOB_LISTING` — 求人サイト
- `PDF_OR_FILE` — PDF/非Webページ
- `IRRELEVANT_INDUSTRY` — 無関係業種
- `NO_LOCAL_SIGNAL` — 地域シグナルなし

### AI弱さ検証（`batch_verify`）

上位N件のリードを「本当に弱いサイトか」AIが再判定。WEAK/NOT_WEAK + 確信度を付与。

---

## スコアリング

### 弱さスコア (0-100)

| 要素 | 点数 |
|------|------|
| viewport未設定 | +15 |
| OGP未設定 | +10 |
| 画像5枚未満 | +10 |
| テキスト300文字未満 | +15 |
| 予約システムなし | +10 |
| 料金情報なし | +10 |
| 問い合わせフォームなし | +10 |
| SSL未対応 | +10 |
| フリープラットフォーム | +10 |

**グレード**: A (60+), B (40-59), C (<40)

### 個人事業主スコア

HTML/テキスト分析で solo / small / corporate / unknown を判定。法人語検出、プラットフォーム判定、テキストシグナル分析を組み合わせ。

---

## CSV出力フォーマット

| 列 | フィールド | 説明 |
|----|-----------|------|
| A | store_name | 店舗名 |
| B | url | URL |
| C | comment | コメント |
| D | score | スコア (0-100) |
| E | filter_reason | フィルタ理由 |
| F-G | score_boost, boost_reasons | スコア増分 |
| H-J | weakness_score, weakness_grade, weakness_reasons | 弱さ分析 |
| K-Q | solo_* | 個人事業主分析 |
| R-S | url_status, error_code | URL状態 |
| T-V | region, city, business_type | 地域・業種 |
| W | site_type | サイト種別 |
| X-Y | phone, email | 連絡先 |
| Z | source_query | 検索クエリ |
| AA | fetched_at_iso | 取得日時 |
| AB-AE | http_status, final_url, is_alive, checked_at_iso | Liveness |
| AF-AH | ai_verified, ai_reason, ai_confidence | AI弱さ検証 |
| AI-AL | ai_action, ai_flags, ai_filter_reason, ai_filter_confidence | AIフィルタ |

---

## ファイル構成

```
lead-finder/
├── web_app/
│   ├── app.py                 # Flask Webアプリ (メイン)
│   ├── templates/
│   │   └── index.html         # SPA テンプレート
│   ├── static/
│   │   ├── js/app.js          # フロントエンドJS
│   │   └── css/style.css      # UIスタイル
│   └── output/                # CSV出力先
├── src/
│   ├── ai_verifier.py         # AIフィルタ & AI弱さ検証 (GPT-4o-mini)
│   ├── processor.py           # リード処理
│   ├── scorer.py              # スコアリング
│   ├── normalize.py           # CSV正規化 & スキーマ定義
│   ├── output_writer.py       # CSV書き出し
│   ├── weakness.py            # 弱さスコア計算
│   ├── solo_classifier.py     # 個人事業主判定
│   ├── solo_boost.py          # ソロブースト計算
│   ├── liveness.py            # URL生存確認
│   └── engines/               # 検索エンジン
├── tests/
│   ├── test_url_filtering.py  # URLフィルタ・クエリ生成テスト
│   ├── test_weakness.py       # 弱さスコアテスト
│   ├── test_solo_classifier.py# 個人事業主判定テスト
│   ├── test_solo_boost.py     # ソロブーストテスト
│   └── test_csv_output.py     # CSV出力テスト
├── tools/                     # CLIパイプラインツール
├── config/                    # クエリ・キーワード設定
└── output/                    # CSV / HTML出力
```

---

## 設定パラメータ

| パラメータ | デフォルト | 説明 |
|-----------|----------|------|
| `MIN_URLS_PER_PAIR` | 8 | Pass 2 発動の閾値 |
| `MAX_URLS_TO_PROCESS` | 400 | クロール上限 |
| `MAX_QUERIES_TOTAL` | 250 | 総クエリ数上限 |
| `LEAD_PROCESS_WORKERS` | 5 | 並列クロールワーカー数 |
| `AI_RELEVANCE_TOP_N` | 30 | AIフィルタ対象件数 |
| `AI_RELEVANCE_MIN_CONFIDENCE` | 6 | AIフィルタ最低確信度 |

---

## 環境変数

```
OPENAI_API_KEY=sk-...           # AI機能用 (任意)
GOOGLE_APPLICATION_CREDENTIALS  # Sheets API用 (CLIパイプライン)
SHEETS_SPREADSHEET_ID           # Sheets ID (CLIパイプライン)
BING_API_KEY                    # Bing検索API (任意)
BRAVE_API_KEY                   # Brave検索API (任意)
```

---

## テスト

```powershell
python -m pytest tests/ -v
```

112テスト（URLフィルタ、クエリ生成、弱さスコア、個人事業主分類、CSV出力）。

---

## CLIパイプライン（旧方式）

```powershell
# テスト実行
.\run_pipeline.bat tokyo

# フル実行
.\run_full_pipeline.bat tokyo kanagawa saitama
```

Google Sheets連携 → Main集計 → HTML生成 の完全自動パイプラインも利用可能。

---

## License

MIT License
