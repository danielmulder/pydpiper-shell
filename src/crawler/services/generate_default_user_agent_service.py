# Import the config loader
from pydpiper_shell.core.utils.config_loader import get_nested_config
import platform


def generate_default_user_agent() -> str:
    """
    Generates a generic Chrome user agent string based on the operating system
    and the version retrieved from settings.json.

    Returns:
        str: The constructed User-Agent string.
    """
    os_name = platform.system()

    # Determine the OS part of the User Agent string
    if os_name == "Windows":
        os_part = "Windows NT 10.0; Win64; x64"
    elif os_name == "Darwin":  # macOS
        os_part = "Macintosh; Intel Mac OS X 10_15_7"
    elif os_name == "Linux":
        os_part = "X11; Linux x86_64"
    else:
        os_part = "Unknown OS"

    # Retrieve the Chrome version from the config, with a safe fallback
    chrome_version = get_nested_config("user_agent.chrome_version", "120.0.0.0")

    # Construct the full User Agent string
    user_agent = (
        f"Mozilla/5.0 ({os_part}) AppleWebKit/537.36 (KHTML, like Gecko) "
        f"Chrome/{chrome_version} Safari/537.36"
    )
    return user_agent