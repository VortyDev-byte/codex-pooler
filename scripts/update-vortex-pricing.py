"""Import the three rollout models from a saved official pricing page and component."""
import hashlib
import json
import re
from pathlib import Path
from bs4 import BeautifulSoup

root = Path(__file__).resolve().parents[1]
page = BeautifulSoup((root / "pricing-current.html").read_text(encoding="utf-8"), "html.parser")
component = (root / "pricing-component.js").read_text(encoding="utf-8")
target = root / "priv/pricing/openai/pricing.json"
data = json.loads(target.read_text())
timestamp = "2026-09-29T21:00:00.000000Z"
names = ("gpt-6.1-sol", "gpt-6-sol", "gpt-6-luna")
for name in names:
    data["models"][name] = dict(categories=["language_model"], category="language_model",
        model=name, prices={}, pricing_type="per_1m_tokens", pricing_types=["per_1m_tokens"], timestamp=timestamp)
for element in page.select("[props]"):
    if element.get("component-export") != "TextTokenPricingTables":
        continue
    props = json.loads(element["props"])
    tier = props["tier"][1]
    if tier not in ("standard", "batch", "flex", "fast"):
        continue
    section = component.split(tier + ":{", 1)[1]
    for encoded in props["rows"][1]:
        row = [value[1] for value in encoded[1]]
        if row[0] not in names:
            continue
        name = row[0]
        short = dict(zip(("input", "cached_input", "cache_write", "output"), row[1:]))
        raw = re.search('"' + re.escape(name) + r'":\{([^}]+)\}', section).group(1)
        aliases = {"input": "input", "cachedInput": "cached_input", "cacheWrite": "cache_write", "output": "output"}
        long = {aliases[k]: float(v) for k, v in re.findall(r'(\w+):([\d.]+)', raw)}
        assert len(short) == len(long) == 4
        data["models"][name]["prices"][tier] = dict(default=short, short_context=short, long_context=long)
for name in names:
    assert len(data["models"][name]["prices"]) == 4
data["models_count"] = len(data["models"])
data["generated_at"] = timestamp
for model in data["models"].values():
    model["timestamp"] = timestamp
target.write_text(json.dumps(data, indent=2, sort_keys=True) + "\n", encoding="utf-8")
test = root / "test/codex_pooler/catalog/openai_pricing_importer_test.exs"
text = test.read_text(encoding="utf-8")
text = re.sub(r'@target_sha256 "[^"]+"', '@target_sha256 "' + hashlib.sha256(target.read_bytes()).hexdigest() + '"', text)
text = re.sub(r'@target_generated_at "[^"]+"', '@target_generated_at "' + timestamp + '"', text)
test.write_text(text, encoding="utf-8")
print("Imported official short/long context rates for", ", ".join(names))
