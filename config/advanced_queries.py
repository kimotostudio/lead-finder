"""
Advanced query generation for deep business discovery.
Based on multi-layer search strategy to find individual counseling/therapy businesses.
"""

# Target regions - can be modified
TARGET_REGIONS = {
    '神奈川': ['横浜市', '川崎市', '相模原市', '藤沢市', '横須賀市', '平塚市', '茅ヶ崎市', '厚木市', '大和市', '鎌倉市'],
    '埼玉': ['さいたま市', '川口市', '所沢市', '越谷市', '草加市', '春日部市', '熊谷市', '川越市', '久喜市', '入間市'],
    '東京': ['新宿区', '渋谷区', '世田谷区', '練馬区', '大田区', '足立区', '江戸川区', '杉並区', '板橋区', '江東区', '品川区', '目黒区', '中野区', '北区', '豊島区'],
}

# Layer A: Abstract terms × business name patterns (MAIN STRATEGY)
LAYER_A_PATTERNS = [
    '{city} 相談室',
    '{city} カウンセリング ルーム',
    '{city} セッション ルーム',
    '{city} 個人セッション',
    '{city} 傾聴 ルーム',
    '{city} 対話 セッション',
    '{city} メンタルサポート 個人',
    '{city} コーチング 個人',
    '{city} 自分軸 コーチング',
    '{city} 内省 サポート',
    '{city} 心の整理 セッション',
    '{city} 安心して話せる 場所',
    '{city} 完全予約制 セッション',
    '{city} 個人 カウンセラー',
    '{city} プライベート セッション',
    '{city} 心理カウンセリング 個人',
    '{city} ライフコーチング 個人',
    '{city} 傾聴カウンセリング',
    '{city} 心のケア 個人',
    '{city} メンタルケア 個人',
]

# Layer B: Platform-specific searches (HIGH VOLUME)
LAYER_B_PLATFORMS = {
    'peraichi.com': [
        '{city} カウンセリング site:peraichi.com',
        '{city} セッション site:peraichi.com',
        '{city} コーチング site:peraichi.com',
        '{city} 相談室 site:peraichi.com',
        '{city} セラピー site:peraichi.com',
    ],
    'crayonsite.info': [
        '{city} 相談 site:crayonsite.info',
        '{city} カウンセリング site:crayonsite.info',
        '{city} セッション site:crayonsite.info',
    ],
    'jimdofree.com': [
        '{city} セラピー site:jimdofree.com',
        '{city} カウンセリング site:jimdofree.com',
        '{city} コーチング site:jimdofree.com',
        '{city} 相談室 site:jimdofree.com',
    ],
    'wixsite.com': [
        '{city} コーチング site:wixsite.com',
        '{city} セッション site:wixsite.com',
        '{city} カウンセリング site:wixsite.com',
        '{city} セラピー site:wixsite.com',
    ],
    'ameblo.jp': [
        '{city} カウンセリング site:ameblo.jp',
        '{city} セッション site:ameblo.jp',
        '{city} コーチング site:ameblo.jp',
        '{city} 相談室 site:ameblo.jp',
    ],
    'note.com': [
        '{city} 対話 site:note.com',
        '{city} コーチング site:note.com',
        '{city} カウンセリング site:note.com',
        '{city} セッション site:note.com',
    ],
    'fc2.com': [
        '{city} セラピー site:fc2.com',
        '{city} カウンセリング site:fc2.com',
        '{city} 相談 site:fc2.com',
    ],
}

# Layer C: Business name suffix patterns (CATCH MISSED ONES)
LAYER_C_PATTERNS = [
    '{city} "ルーム" 相談',
    '{city} "サロン" カウンセリング',
    '{city} "の部屋" 相談',
    '{city} "庵" 相談',
    '{city} "アトリエ" セッション',
    '{city} "小さなサロン" 相談',
    '{city} "自宅サロン" セッション',
    '{city} "スペース" カウンセリング',
    '{city} "ハウス" セッション',
    '{city} "オフィス" カウンセリング',
]

# Layer D: General wellness salons (ADDITIONAL VOLUME - high payment capacity)
LAYER_D_WELLNESS = [
    '{city} ピラティス 個人 サロン',
    '{city} ヨガ 個人 サロン',
    '{city} 呼吸 トレーニング 個人',
    '{city} パーソナル ヨガ 予約',
    '{city} パーソナル ピラティス 予約',
    '{city} リラクゼーション サロン 個人',
    '{city} アロマ サロン 個人',
    '{city} 整体 予約制 個人',
    '{city} プライベート ヨガ',
    '{city} プライベート ピラティス',
    '{city} 個人 整体院',
    '{city} 完全予約制 整体',
]


def generate_queries(region: str, limit_per_layer: int = None) -> list:
    """
    Generate multi-layer search queries for a region.

    Args:
        region: Target region (神奈川, 埼玉, 東京)
        limit_per_layer: Optional limit on queries per layer

    Returns:
        List of search query strings
    """
    if region not in TARGET_REGIONS:
        raise ValueError(f"Region '{region}' not found. Available: {list(TARGET_REGIONS.keys())}")

    cities = TARGET_REGIONS[region]
    all_queries = []

    # Layer A: Abstract terms (PRIORITY)
    layer_a = []
    for city in cities:
        for pattern in LAYER_A_PATTERNS:
            layer_a.append(pattern.format(city=city))

    if limit_per_layer and len(layer_a) > limit_per_layer:
        layer_a = layer_a[:limit_per_layer]
    all_queries.extend(layer_a)

    # Layer B: Platform-specific (HIGH VOLUME)
    layer_b = []
    for city in cities:
        for platform, patterns in LAYER_B_PLATFORMS.items():
            for pattern in patterns:
                layer_b.append(pattern.format(city=city))

    if limit_per_layer and len(layer_b) > limit_per_layer:
        layer_b = layer_b[:limit_per_layer]
    all_queries.extend(layer_b)

    # Layer C: Suffix patterns (CATCH MISSED)
    layer_c = []
    for city in cities:
        for pattern in LAYER_C_PATTERNS:
            layer_c.append(pattern.format(city=city))

    if limit_per_layer and len(layer_c) > limit_per_layer:
        layer_c = layer_c[:limit_per_layer]
    all_queries.extend(layer_c)

    # Layer D: Wellness (ADDITIONAL)
    layer_d = []
    for city in cities:
        for pattern in LAYER_D_WELLNESS:
            layer_d.append(pattern.format(city=city))

    if limit_per_layer and len(layer_d) > limit_per_layer:
        layer_d = layer_d[:limit_per_layer]
    all_queries.extend(layer_d)

    return all_queries


def generate_test_queries(region: str, cities_limit: int = 3, queries_per_city: int = 5) -> list:
    """
    Generate a smaller test set of queries.

    Args:
        region: Target region
        cities_limit: How many cities to include
        queries_per_city: How many queries per city

    Returns:
        List of test queries
    """
    if region not in TARGET_REGIONS:
        raise ValueError(f"Region '{region}' not found")

    cities = TARGET_REGIONS[region][:cities_limit]
    queries = []

    # Mix of all layers
    patterns = (
        LAYER_A_PATTERNS[:2] +  # 2 from Layer A
        list(LAYER_B_PLATFORMS['peraichi.com'])[:1] +  # 1 from peraichi
        list(LAYER_B_PLATFORMS['ameblo.jp'])[:1] +  # 1 from ameblo
        LAYER_C_PATTERNS[:1]  # 1 from Layer C
    )

    for city in cities:
        for pattern in patterns[:queries_per_city]:
            queries.append(pattern.format(city=city))

    return queries


def generate_queries_for_cities(cities: list, business_types: list, limit: int = 10) -> list:
    """
    Generate solo/small-business optimized queries for arbitrary cities/business types.

    Priority:
    1) Solo-business intent queries
    2) Platform-specific queries
    3) Standard discovery queries
    """
    queries = []
    for city in cities or []:
        for btype in business_types or []:
            base = f'{city} {btype}'
            solo_queries = [
                f'{city} 個人 {btype}',
                f'{city} {btype} 個人サロン',
                f'{city} 自宅 {btype}',
                f'{city} {btype} 自宅サロン',
                f'{city} プライベート {btype}',
                f'{city} {btype} プライベートサロン',
                f'{city} {btype} 完全予約制',
                f'{city} {btype} 女性専用',
                f'{city} {btype} マンツーマン',
                f'{city} {btype} 少人数',
                f'{city} {btype} アットホーム',
                f'{city} {btype} 隠れ家',
            ]
            platform_queries = [
                f'{base} site:ameblo.jp',
                f'{base} site:amebaownd.com',
                f'{base} site:note.com',
                f'{base} site:lit.link',
                f'{base} site:peraichi.com',
                f'{base} site:studio.site',
                f'{base} site:strikingly.com',
                f'{base} site:jimdofree.com',
                f'{base} site:wix.com',
                f'{base} site:wordpress.com',
                f'{base} site:fc2.com',
                f'{base} site:jugem.jp',
                f'{base} site:cocolog-nifty.com',
                f'{base} site:livedoor.blog',
            ]
            standard_queries = [
                base,
                f'{base} 予約',
                f'{base} 料金',
                f'{base} site:.jp',
            ]
            all_queries = solo_queries + platform_queries + standard_queries
            queries.extend(all_queries[:max(1, int(limit))])
    return queries
