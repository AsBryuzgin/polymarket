from __future__ import annotations


DOMAIN_KEYWORDS = {
    "sports": [
        "nba", "nfl", "mlb", "nhl", "epl", "lal", "serie-a", "liga", "uefa",
        "champions-league", "atp", "wta", "ufc", "mma", "f1", "nascar",
        "spread", "moneyline", "totals", "vs", "fc", "win on"
    ],
    "politics": [
        "election", "president", "prime minister", "senate", "house", "governor",
        "democrat", "republican", "vote", "voter", "candidate", "campaign",
        "parliament", "coalition", "mayor", "politic"
    ],
    "geopolitics": [
        "russia", "ukraine", "ceasefire", "israel", "gaza", "iran", "china",
        "taiwan", "war", "nato", "missile", "invasion", "peace deal",
        "sanction", "troops"
    ],
    "macro": [
        "cpi", "ppi", "inflation", "fed", "fomc", "ecb", "boj", "rate cut",
        "rate hike", "gdp", "unemployment", "payrolls", "recession", "yield",
        "treasury", "macro"
    ],
    "weather": [
        "hurricane", "storm", "rainfall", "snow", "temperature", "earthquake",
        "wildfire", "weather", "climate", "flood", "tornado"
    ],
    "entertainment": [
        "album", "movie", "box office", "oscar", "grammy", "emmy",
        "rihanna", "playboi", "jesus christ", "gta", "netflix", "trailer",
        "release date", "celebrity", "music"
    ],
    "finance": [
        "bitcoin", "btc", "eth", "ethereum", "sol", "spy", "qqq", "nasdaq",
        "s&p", "dow", "gold", "oil", "stock", "earnings", "crypto", "bond",
        "treasury yield"
    ],
}


def classify_domain(title: str | None = None, slug: str | None = None, event_slug: str | None = None) -> str:
    text = " ".join(
        [
            (title or ""),
            (slug or ""),
            (event_slug or ""),
        ]
    ).lower()

    scores: dict[str, int] = {domain: 0 for domain in DOMAIN_KEYWORDS}

    for domain, keywords in DOMAIN_KEYWORDS.items():
        for keyword in keywords:
            if keyword in text:
                scores[domain] += 1

    best_domain = max(scores, key=scores.get)
    if scores[best_domain] == 0:
        return "other"

    return best_domain
