# src/crawler/services/redirect_resolver_service.py
import asyncio
import logging
from typing import Optional, Tuple

import aiohttp
from yarl import URL  # Yarl wordt gebruikt door aiohttp voor URL objecten

from pydpiper_shell.core.utils.config_loader import get_nested_config
from crawler.services.generate_default_user_agent_service import generate_default_user_agent

logger = logging.getLogger(__name__)


class RedirectResolverService:
    """
    Een asynchrone service om de uiteindelijke endpoint-URL te bepalen
    na het volgen van HTTP-redirects.
    """

    def __init__(self):
        """Haalt de benodigde configuratie op uit settings.json."""
        # Haal timeout-waarden op uit de sectie 'session'
        self.timeout = get_nested_config("session.time_out", 10)
        self.max_redirects = get_nested_config("session.max_redirects", 10)
        self.user_agent = generate_default_user_agent()

    async def resolve_final_url(
            self, initial_url: str, session: Optional[aiohttp.ClientSession] = None
    ) -> Tuple[Optional[str], Optional[str]]:
        """
        Volgt redirects voor een gegeven URL en retourneert de uiteindelijke URL.

        Args:
            initial_url: De URL om mee te beginnen.
            session: Een optionele, bestaande aiohttp.ClientSession.

        Returns:
            Tuple[Optional[str], Optional[str]]: De uiteindelijke URL en een foutmelding (als die er is).
        """
        # Bepaal of we een tijdelijke sessie moeten aanmaken of een bestaande moeten gebruiken
        should_close_session = False
        if session is None:
            timeout_obj = aiohttp.ClientTimeout(total=self.timeout)
            default_headers = {'User-Agent': self.user_agent}
            session = aiohttp.ClientSession(
                timeout=timeout_obj, headers=default_headers
            )
            should_close_session = True

        final_url: Optional[str] = None
        error_message: Optional[str] = None

        try:
            # Gebruik aiohttp's ingebouwde redirect-volging
            async with session.get(
                    initial_url,
                    allow_redirects=True,
                    max_redirects=self.max_redirects
            ) as response:
                # De finale URL is beschikbaar via response.url (van het yarl.URL type)
                final_url = str(response.url)

                # Optioneel: Check of er daadwerkelijk redirects zijn geweest
                if response.history:
                    logger.debug(
                        "URL resolved na %d redirects: %s -> %s",
                        len(response.history), initial_url, final_url
                    )
                else:
                    logger.debug("URL resolved zonder redirects: %s", final_url)

        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            error_message = f"Client/Timeout Error: {type(e).__name__} - {e}"
            logger.warning("Error resolving URL %s: %s", initial_url, error_message)
        except Exception as e:
            error_message = f"Unexpected Error: {type(e).__name__} - {e}"
            logger.error("Unexpected error resolving URL %s: %s", initial_url, error_message, exc_info=True)
        finally:
            if should_close_session and session:
                await session.close()

        return final_url, error_message