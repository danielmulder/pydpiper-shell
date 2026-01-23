# scripts/performance_test.py
import json
import time
from pathlib import Path
from typing import List, Dict, Any
from collections import defaultdict

from bs4 import BeautifulSoup
from pydantic import ValidationError, TypeAdapter

# We moeten de project-paden, datamodellen en de cache manager importeren
from pydpiper_shell.core.utils.path_utils import PathUtils
from crawler.model import Page
from pydpiper_shell.core.managers.database_manager import DatabaseManager

# --- CONFIGURATIE ---
# Pas dit aan naar het project ID dat je wilt testen.
PROJECT_ID = 4


# --------------------


class PageParser:
    """
    De `PageParser` klasse verwerkt HTML-inhoud van een pagina en haalt diverse elementen op.
    """

    def __init__(self, page_content, base_url):
        self.soup = BeautifulSoup(page_content, 'html.parser')
        self.base_url = base_url

    def extract_elements(self):
        """Haalt een selectie van belangrijke elementen uit de HTML-inhoud op."""
        return {
            'page_title': self.extract_page_title(),
            'meta_description': self.extract_meta_description(),
            'canonical_tag': self.extract_canonical_tag(),
            'headings': self.extract_headings(),
        }

    def extract_page_title(self):
        """Haalt de paginatitel op."""
        element = self.soup.find('title')
        return element.get_text(strip=True) if element else ''

    def extract_meta_description(self):
        """Haalt de meta description van de pagina op."""
        meta_tag = self.soup.find('meta', attrs={'name': 'description'})
        return meta_tag.get('content', '').strip() if meta_tag else ''

    def extract_canonical_tag(self):
        """Haalt de canonical tag op."""
        link_tag = self.soup.find('link', attrs={'rel': 'canonical'})
        return link_tag.get('href', '').strip() if link_tag else ''

    def extract_headings(self):
        """Haalt alle headings (H1 tot H6) op."""
        headings = {}
        for level in range(1, 7):
            tags = [tag.get_text(strip=True) for tag in self.soup.find_all(f'h{level}')]
            if tags:
                headings[f'h{level}'] = tags
        return headings


def save_extracted_data(project_id: int, all_data: List[Dict[str, Any]]):
    """
    Groepeert de geëxtraheerde data per element type en slaat elk type op
    in een apart JSON-bestand in de cache.
    """
    print("\n   Start met wegschrijven van geëxtraheerde data naar de cache...")

    # Gebruik een defaultdict om data per sleutel te groeperen
    grouped_data = defaultdict(list)
    for page_data in all_data:
        for key, value in page_data.items():
            # Voeg een referentie naar de pagina URL toe voor context
            if value:  # Sla alleen niet-lege data op
                grouped_data[key].append({
                    "url": all_data.index(page_data),  # Simpele index als referentie
                    "data": value
                })

    # Initialiseer de cache manager
    cache_mgr = CacheManager()

    # Sla elke groep op in een eigen bestand
    for key, data_list in grouped_data.items():
        # Belangrijk: De CachePersistenceManager verwacht Pydantic modellen.
        # Voor dit standalone script slaan we het direct als JSON op.
        # We moeten de save_cache methode omzeilen en direct schrijven.
        project_dir = cache_mgr._project_dir(project_id)
        file_path = project_dir / f"{key}.json"

        try:
            file_path.write_text(json.dumps(data_list, indent=2, ensure_ascii=False), encoding='utf-8')
            print(f"   ✅ Data voor '{key}' opgeslagen in cache ({len(data_list)} items).")
        except Exception as e:
            print(f"   ❌ Fout bij opslaan van '{key}': {e}")


def run_performance_test():
    """Voert de volledige extractie- en manipulatietest uit."""
    print(f"🚀 Start performance test met volledige PageParser voor project ID: {PROJECT_ID}")

    # 1. Laad de data
    try:
        cache_file = PathUtils.get_cache_root() / str(PROJECT_ID) / "pages.json"
        if not cache_file.exists():
            print(f"❌ Fout: Cachebestand 'pages.json' niet gevonden voor project {PROJECT_ID}.")
            return

        print(f"   Cachebestand gevonden. Bezig met laden van '{cache_file.name}'...")
        raw_data = json.loads(cache_file.read_text(encoding='utf-8'))
        PageListAdapter = TypeAdapter(List[Page])
        pages = PageListAdapter.validate_python(raw_data)
        page_count = len(pages)
        print(f"   Succesvol {page_count} pagina's geladen en gevalideerd.")

    except Exception as e:
        print(f"❌ Fout bij het laden van de cache: {e}")
        return

    # 2. Voer de parsing uit en meet de tijd
    print("   Start met parsen van HTML content via PageParser.extract_elements...")
    all_extracted_data = []

    start_time = time.perf_counter()

    for page in pages:
        if page.content:
            parser = PageParser(page.content, str(page.url))
            extracted = parser.extract_elements()
            all_extracted_data.append(extracted)

    end_time = time.perf_counter()
    duration = end_time - start_time

    # 3. Toon de performance resultaten
    print("\n" + "—" * 40)
    print("📊 Performance Resultaten (met PageParser)")
    print("—" * 40)
    print(f"   Pagina's verwerkt:      {page_count}")
    print(f"   Totale verwerkingstijd: {duration:.4f} seconden")
    if duration > 0:
        pages_per_second = page_count / duration
        print(f"   Snelheid:               {pages_per_second:.2f} pagina's per seconde")
    print("—" * 40)

    # 4. Sla de geëxtraheerde data op
    if all_extracted_data:
        save_extracted_data(PROJECT_ID, all_extracted_data)

    print("\n✅ Test voltooid.")


if __name__ == "__main__":
    run_performance_test()