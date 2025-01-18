from typing import Optional

from tldextract import tldextract


def parse_domain(url: str) -> str:
    extracted = tldextract.extract(url)
    return extracted.registered_domain