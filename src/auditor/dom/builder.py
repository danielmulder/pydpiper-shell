# src/auditor/dom/builder.py
import logging
import re
import json
from collections import Counter
from typing import Dict, List, Set, Tuple, Optional
from urllib.parse import urljoin, urlparse
from bs4 import BeautifulSoup, Tag, Doctype

from .models import HTMLDocument
from .core import ElementBase
from .registry import DOMRegistry
from ..utils.stopwords import combine_stopwords

logger = logging.getLogger(__name__)


class DOMBuilder:
    """
    Builder responsible for parsing raw HTML into a structured HTMLDocument model.
    It handles DOM tree construction, n-gram analysis, and structured data extraction.
    """

    def __init__(self):
        """Initializes the builder and ensures the DOMRegistry is populated."""
        DOMRegistry.discover()

    def parse_doc(
            self,
            url: str,
            html: str,
            status_map: Optional[Dict[str, int]] = None
    ) -> HTMLDocument:
        """
        Parses raw HTML content into a rich HTMLDocument object.

        Args:
            url (str): The URL of the page being parsed.
            html (str): The raw HTML string.
            status_map (Optional[Dict[str, int]]): A map of URLs to HTTP status codes
                                                   for checking link health.

        Returns:
            HTMLDocument: A structured representation of the parsed page.
        """
        if not html:
            return HTMLDocument(raw_url=url)

        # Basic cleanup of potentially dirty HTML (e.g., BOM)
        clean_html = html.replace('\ufeff', '').strip()
        soup = BeautifulSoup(clean_html, 'html.parser')

        # --- Basic Validity Checks ---
        found_doctype = bool(re.search(r'<!doctype', clean_html[:1000], re.IGNORECASE))
        if not found_doctype:
            # Fallback check using BS4 structure
            for item in soup.contents:
                if isinstance(item, Doctype):
                    found_doctype = True
                    break

        # Check for presence of <html> tag
        found_root = (
                bool(soup.find('html')) or
                bool(re.search(r'<html', clean_html[:2000], re.IGNORECASE))
        )

        # --- Build DOM Trees ---
        head_obj = self._build_tree(soup.head, base_url=url, status_map=status_map) if soup.head else None
        body_obj = self._build_tree(soup.body, base_url=url, status_map=status_map) if soup.body else None

        # --- Extract Structured Data (JSON-LD) ---
        structured_data = []
        for script in soup.find_all('script', type='application/ld+json'):
            if script.string:
                try:
                    data = json.loads(script.string)
                    if isinstance(data, list):
                        structured_data.extend(data)
                    else:
                        structured_data.append(data)
                except Exception:
                    pass  # Ignore malformed JSON-LD

        # --- Content Analysis (N-Grams) ---
        body_text = ""
        unigrams, bigrams, trigrams = {}, {}, {}
        if soup.body:
            body_text = soup.body.get_text(" ", strip=True)
            unigrams = self._calculate_ngrams(body_text, n=1, top_n=20)
            bigrams = self._calculate_ngrams(body_text, n=2, top_n=20)
            trigrams = self._calculate_ngrams(body_text, n=3, top_n=20)

        return HTMLDocument(
            raw_url=url,
            has_doctype=found_doctype,
            root_tag_valid=found_root,
            head=head_obj,
            body=body_obj,
            body_text=body_text[:5000],  # Truncate for storage efficiency
            body_unigrams=unigrams,
            body_bigrams=bigrams,
            body_trigrams=trigrams,
            structured_data=structured_data
        )

    @combine_stopwords
    def _calculate_ngrams(
            self, text: str, stopwords: Set[str], n: int = 1, top_n: int = 20
    ) -> Dict[str, int]:
        """
        Calculates the most frequent n-grams from the given text.
        Filters out pure number sequences (phone numbers, times).

        Args:
            text (str): The raw text content.
            stopwords (Set[str]): A set of stopwords to filter out (injected by decorator).
            n (int): The size of the n-gram (1 for unigram, 2 for bigram, etc.).
            top_n (int): The number of top results to return.

        Returns:
            Dict[str, int]: A dictionary mapping n-gram strings to their counts.
        """
        if not text:
            return {}

        # Extract words using regex to handle basic tokenization
        words = re.findall(r'\b[a-zA-ZÀ-ÖØ-öø-ÿ0-9]+\b', text.lower())

        # Filter stopwords and short tokens
        filtered_words = [w for w in words if w not in stopwords and len(w) > 1]

        if len(filtered_words) < n:
            return {}

        grams = Counter()
        if n == 1:
            grams.update(filtered_words)
        else:
            # Create sliding window for n-grams
            grams.update(zip(*(filtered_words[i:] for i in range(n))))

        result = {}
        # We fetch more candidates than top_n because we might filter some out (numbers)
        candidates = grams.most_common(top_n * 3)

        for item, count in candidates:
            key = " ".join(item) if n > 1 else item

            # --- FILTER: NUMERIC NOISE ---
            # Reject n-grams that contain NO letters (e.g. "0342 44 28" or "17 00")
            # This preserves "Windows 11" but removes pure phone numbers/times.
            if not re.search(r'[a-zA-ZÀ-ÖØ-öø-ÿ]', key):
                continue

            result[key] = count

            # Stop once we have filled the quota
            if len(result) >= top_n:
                break

        return result

    def _build_tree(
            self, tag: Tag, base_url: str, status_map: Optional[Dict[str, int]]
    ) -> ElementBase:
        """
        Recursively builds a simplified element tree from a BeautifulSoup Tag.

        Applies enrichment logic for specific elements (e.g., status checks for links).
        """
        children = []
        if hasattr(tag, 'children'):
            for child in tag.children:
                if isinstance(child, Tag):
                    children.append(self._build_tree(child, base_url, status_map))

        # Retrieve specific parser from registry if available
        parser = DOMRegistry.get_parser(tag.name)
        element = None

        if parser:
            element = parser(tag, children)
        else:
            # Fallback for generic elements
            text = tag.get_text(" ", strip=True)[:50] if hasattr(tag, 'get_text') else ""
            element = ElementBase(tag=tag.name, attrs=tag.attrs, text=text, children=children)

        # --- Enrichment: Status & Internal/External Checks for Links ---
        if element.tag == 'a' and hasattr(element, 'href') and element.href:
            try:
                # 1. Resolve Absolute URL
                abs_url = urljoin(base_url, element.href)
                # Remove fragment for consistent status checking
                clean_url = urlparse(abs_url)._replace(fragment="").geturl()

                # 2. Status Lookup
                if status_map and clean_url in status_map:
                    element.status_code = status_map[clean_url]

                # 3. Internal vs External Check
                base_parsed = urlparse(base_url)
                target_parsed = urlparse(abs_url)

                # Normalize domains by removing 'www.' prefix
                source_domain = base_parsed.netloc.removeprefix("www.")
                target_domain = target_parsed.netloc.removeprefix("www.")

                # Logic: If target domain exists and differs from source -> External
                if (target_domain and
                        target_domain != source_domain and
                        not target_domain.endswith(f".{source_domain}")):
                    element.is_external = True

            except Exception:
                pass  # Fail silently on malformed URLs to prevent crash

        return element