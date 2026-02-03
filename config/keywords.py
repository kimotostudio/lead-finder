"""
Japanese keyword lists for content detection and scoring.
"""

# Pricing/menu keywords
PRICING_KEYWORDS = [
    '料金', '価格', 'メニュー', 'コース', 'プラン', '費用', '金額',
    'price', 'menu', '値段', 'お支払い', '代金', 'pricing'
]

# Booking/reservation keywords
BOOKING_KEYWORDS = [
    '予約', 'カレンダー', '空き状況', 'ご予約', '申し込み', '予約する',
    'booking', 'reservation', 'お申込み', '予約フォーム', 'book now'
]

# Access/location keywords
ACCESS_KEYWORDS = [
    'アクセス', '住所', '所在地', '地図', '交通', '最寄り駅', 'access',
    'map', 'address', '行き方', '場所', 'location', 'directions'
]

# Profile/about keywords
PROFILE_KEYWORDS = [
    'プロフィール', '自己紹介', '代表', 'オーナー', '施術者', 'about',
    'profile', '紹介', 'スタッフ', 'セラピスト', '講師'
]

# Japanese cities and wards (Tokyo 23 wards + major cities)
CITY_KEYWORDS = [
    # Tokyo 23 wards
    '千代田区', '中央区', '港区', '新宿区', '文京区', '台東区', '墨田区',
    '江東区', '品川区', '目黒区', '大田区', '世田谷区', '渋谷区', '中野区',
    '杉並区', '豊島区', '北区', '荒川区', '板橋区', '練馬区', '足立区',
    '葛飾区', '江戸川区',
    # Major cities
    '東京都', '大阪府', '京都府', '北海道', '神奈川県', '愛知県', '福岡県',
    '横浜市', '名古屋市', '札幌市', '神戸市', '福岡市', '川崎市', '埼玉市',
    '千葉市', '仙台市', '広島市', '北九州市', 'さいたま市', '大阪市',
]

# Free site builder platforms
FREE_PLATFORMS = {
    'peraichi': ['peraichi.com'],
    'crayon': ['crayonsite.info', 'crayonsite.net'],
    'jimdo': ['jimdo', 'jimdofree'],
    'wix': ['wixsite.com', 'wix.com'],
    'ameblo': ['ameblo.jp', 'ameba.jp'],
    'fc2': ['fc2.com'],
    'note': ['note.com'],
    'studio.site': ['studio.site'],
    'lit.link': ['lit.link'],
    'linktree': ['linktr.ee'],
    'thebase': ['thebase.in'],
    'wordpress': ['wp-content', 'wordpress'],
}

# Sites to exclude (large portals, aggregators, social media)
EXCLUDED_DOMAINS = [
    # E-commerce
    'amazon', 'rakuten', 'yahoo', 'mercari', 'ebay', 'shopify',
    # Aggregators & portals - EXPANDED
    'hotpepper.jp', 'tabelog', 'retty', 'gurunavi', 'epark.jp',
    'ekiten.jp', 'google.com/maps', 'yelp', 'tripadvisor',
    'mitsuraku.jp', 'ozmall.co.jp', 'beauty-park.jp',
    'navitime.co.jp', 'judo-ch.jp', 'karadarefre.jp',
    'raku-navi.jp', 'health-more.jp', 'rairai.net',
    'privatesalon-navi.com', 'fitlu.jp', 'cani.jp', 'karada-campus.com',
    'seitainavi.jp', 'otokoro.com', 'lesson.market', 'qool.jp',
    'coralful.jp', 'yogajournal.jp', 'coubic.com', 'jmty.jp',
    'rsvia.co.jp', 'aumo.jp', 'andco.group', 'minoriba.jp',
    'dr-recella.com', 'reserva.be', 'tol-app.jp', 'cityliving-web.jp',
    # Social media only (no website)
    'facebook.com/pages', 'instagram.com', 'twitter.com', 'x.com',
    'linkedin.com', 'youtube.com', 'tiktok.com',
    # Blogs/media (unless it's their business site)
    'medium.com', 'hatena', 'livedoor', 'note.com/n/', 'gurum.biz',
    'ameba.jp/profile', 'mallorypork.com', 'arcangel.jp',
    'blogtag.ameba.jp', 'ethnic-magazine.com', 'ameblo.jp/zzzzzz',
    # Job boards
    'indeed.com', 'mynavi', 'rikunabi', 'doda',
    # Chinese sites
    'zhihu.com', 'baidu.com', 'weibo.com', 'qq.com',
    '.cn/', 'shuhaixsw.com',
    # Ads
    'bing.com/aclick', 'google.com/aclk',
    # Other
    'wikipedia', 'zehitomo.com', 'street-academy.com',
    'shuminavi.net', '.mom/', '51hlw5.com', 'disney',
    'j-acc.org', 'yoga-gene.com', 'surugabank.co.jp', 'dormy-hotels.com',
    'regasu-shinjuku.or.jp', 'porcelarts-navi.com', 'share-park.com',
    'sontokugama.com', 'tailwalk.jp', 'w-2-b.com',
    'anaintercontinental-manza.jp', 'dm2.co.jp', 'stylesearch.jp',
    'gaillard.jp', 'photorait.net', 'visitingcafe.com',
    'gamewith.jp', 'nailbook.jp', 'nitticompany.com',
    'gfoodd.com', 'dailyshincho.jp', 'spirituabreath.com',
    'shiritaiko10.com', 'next-business.co.jp', 'shairesalon-go.today',
    'salon-knowledge.com', 'biz.moneyforward.com', 'contents.uranai.cloud',
    'twinpentagon.com', 'school-afloat.com', 'dreamnews.jp',
    'referralmap.net', 'therapylife.jp', 'icosaka.com',
    'ysroad.co.jp', 'shisha-chillin.com', 'itainews.com',
    'beautyshare.jp', 'deriheruhotel.com', 'jalan.net',
]

# Aggregator detection keywords (content-based filtering)
AGGREGATOR_KEYWORDS = [
    # Listing/recommendation indicators
    'おすすめ', 'ランキング', '厳選', '比較', '一覧', 'まとめ',
    '選', 'best', 'top', '口コミ', 'レビュー', '評判',
    # Numbers suggesting lists
    '10選', '20選', '30選', '5選', '15選', '25選',
    # Portal/directory language
    '検索', '予約サイト', 'ポータル', 'ナビ', '情報サイト',
    '掲載店舗', '登録', '一括', '比較サイト',
]

# Owner/Proprietor extraction keywords
OWNER_KEYWORDS = [
    '代表', 'オーナー', '店主', '経営者', '主宰', 'セラピスト',
    '施術者', '講師', 'インストラクター', '院長', 'サロンオーナー',
    '運営者', 'プロフィール', '自己紹介',
]

# Business name extraction patterns (shop/studio name)
BUSINESS_NAME_PATTERNS = [
    'サロン', 'スタジオ', '整体院', '整骨院', 'クリニック',
    'エステ', 'マッサージ', 'ヨガ', 'ピラティス', 'ジム',
    '治療院', '鍼灸', 'リラクゼーション', 'ネイル', 'ヘア',
]

# Prioritized domains (small business builders)
PRIORITIZED_PATTERNS = [
    'peraichi.com', 'studio.site', 'wixsite.com', 'jimdofree.com',
    'ameblo.jp', 'lit.link', 'linktr.ee', 'thebase.in',
]

# Query variations to generate
QUERY_SUFFIXES = [
    '',  # Original query
    '個人',
    '個人経営',
    'プライベート',
    'スタジオ',
    'サロン',
]

# Site type suffixes for targeted search
SITE_TYPE_SUFFIXES = [
    'peraichi',
    'ameblo',
    'jimdo',
    'studio.site',
    'lit.link',
]
