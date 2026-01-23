# src/pydpiper_shell/core/plugins/modules/bedrijfsadres_scraper_plugin.py
import logging
import pandas as pd
import requests
import argparse
from typing import Optional, List, Dict
from urllib.parse import quote_plus  # Voor het correct encoden van bedrijfsnamen in de URL

from bs4 import BeautifulSoup

from pydpiper_shell.core.plugins.base import PluginBase
from pydpiper_shell.core.plugins.facade import PluginFacade
from pydpiper_shell.core.utils.path_utils import PathUtils

logger = logging.getLogger(__name__)


class BedrijfsadresScraperPlugin(PluginBase):
    """
    Plugin om bedrijfsadressen op te zoeken in het bedrijvenregister
    op basis van een lijst bedrijfsnamen en het resultaat naar Excel te exporteren.
    """

    BASE_URL = "https://www.bedrijvenregister.nl/zoekresultaten?q_source=header&q="

    # Gebruik een requests.Session om de verbinding te hergebruiken
    _session: Optional[requests.Session] = None

    def _get_session(self) -> requests.Session:
        """Initialiseer of retourneer de requests sessie."""
        if self._session is None:
            self._session = requests.Session()
            # Voeg een User-Agent toe om een standaard browser na te bootsen
            self._session.headers.update({
                'User-Agent': 'PydPiper-BedrijfsZoeker/1.0'
            })
        return self._session

    def _scrape_adres(self, bedrijfsnaam: str) -> str:
        """Zoekt het adres voor één bedrijfsnaam."""
        session = self._get_session()

        # De bedrijfsnaam correct encoden voor gebruik in de URL
        encoded_name = quote_plus(bedrijfsnaam)
        search_url = f"{self.BASE_URL}{encoded_name}"

        logger.info(f"Zoeken naar '{bedrijfsnaam}' op {search_url}")

        try:
            # Synchrone aanroep met timeout
            response = session.get(search_url, timeout=15)
            response.raise_for_status()  # Gooi uitzondering op bij 4xx/5xx status

            soup = BeautifulSoup(response.content, 'html.parser')

            # --- Parsen: Zoek de Vestigingsadres-rij ---

            # 1. Zoek het label-element (<div class="namecolumn">) met de tekst "Vestigingsadres:"
            # De lambda zorgt ervoor dat de tekst exact (met witruimte) of gedeeltelijk matcht.
            adres_label = soup.find('div', class_='namecolumn', string=lambda t: t and 'Vestigingsadres:' in t)

            if adres_label:
                # 2. Ga naar de ouder-div, dit zou de <div class="resultrow"> moeten zijn
                resultrow_div = adres_label.parent

                if resultrow_div and 'resultrow' in resultrow_div.get('class', []):
                    # 3. Zoek de adres-waarde in de sibling <div class="valuecolumn">
                    adres_value = resultrow_div.find('div', class_='valuecolumn')

                    if adres_value:
                        return adres_value.get_text(strip=True)
                    else:
                        # Moet de waarde wel bestaan, maar kon niet geparst worden.
                        return "Adres niet gevonden (Parsing Fout)"

            # Als het label niet is gevonden, retourneer dan een duidelijke melding.
            return "Adres niet gevonden"

        except requests.exceptions.HTTPError as e:
            return f"FOUT: HTTP-status {e.response.status_code}"
        except requests.exceptions.RequestException as e:
            return f"FOUT: Verbindingsfout ({e})"
        except Exception as e:
            logger.error(f"Onverwachte fout tijdens scraping voor '{bedrijfsnaam}': {e}", exc_info=True)
            return f"FOUT: Onverwachte fout"

    def run(self, app: PluginFacade, args: list[str]) -> int:
        """
        De hoofdfunctie van de plugin: ontvangt namen, zoekt adressen en exporteert
        naar een Excel-bestand in de Documenten-map.
        """
        parser = argparse.ArgumentParser(prog="bedrijfsadres_scraper", description="Zoekt bedrijfsadressen.")
        # Gebruik nargs='+' om minstens één of meer namen op te vangen
        parser.add_argument("bedrijfsnamen", nargs='+', help="Een of meer bedrijfsnamen om op te zoeken.")
        parser.add_argument("--output-file", type=str, default="bedrijfsadressen_output.xlsx",
                            help="De naam van het Excel-outputbestand.")

        try:
            parsed_args = parser.parse_args(args)
        except SystemExit:
            return 1

        bedrijfsnamen = [name.strip() for name in parsed_args.bedrijfsnamen if name.strip()]

        if not bedrijfsnamen:
            print("❌ Fout: Geen geldige bedrijfsnamen opgegeven.")
            return 1

        app.logger.info(f"Starten BedrijfsadresScraperPlugin voor {len(bedrijfsnamen)} namen...")
        print(f"Starten met zoeken naar adressen voor {len(bedrijfsnamen)} bedrijfsnamen...")

        results: List[Dict[str, str]] = []

        # Itereren over bedrijfsnamen
        for naam in parsed_args.bedrijfsnamen:
            adres = self._scrape_adres(naam)
            results.append({
                "Bedrijfsnaam": naam,
                "Vestigingsadres": adres,
            })
            print(f"[{naam}] -> {adres}")  # Directe feedback geven

        # Sluit de requests sessie na gebruik
        if self._session:
            self._session.close()

        if not results:
            print("✅ Zoeken voltooid. Geen resultaten gevonden.")
            return 0

        # DataFrame maken en exporteren
        final_df = pd.DataFrame(results)

        try:
            # Pad naar de Documenten-map
            output_dir = PathUtils.get_user_documents_dir()
            output_file = output_dir / parsed_args.output_file

            # Schrijf naar Excel
            final_df.to_excel(output_file, index=False, engine='openpyxl')

            print(f"\n✅ Zoeken voltooid! {len(final_df)} adressen opgeslagen in:")
            print(f"   {output_file}")
            return 0

        except Exception as e:
            app.logger.error(f"Fout bij opslaan van Excel-bestand: {e}", exc_info=True)
            print(f"❌ Fout: Kon het Excel-bestand niet opslaan: {e}")
            return 1