# src/pydpiper_shell/controllers/services/sys_info_service.py
import psutil
import platform

def ram_info() -> dict:
    """
    Retrieves and formats virtual memory (RAM) information.

    Returns:
        dict: A dictionary containing total, used, and available RAM in MB,
              percentage used, and the operating system platform.
    """
    vm = psutil.virtual_memory()
    return {
        "total_mb": round(vm.total / (1024 * 1024), 2),
        "used_mb": round(vm.used / (1024 * 1024), 2),
        "available_mb": round(vm.available / (1024 * 1024), 2),
        "percent_used": vm.percent,
        "platform": platform.system(),
    }

def hd_info() -> list[dict]:
    """
    Retrieves and formats disk usage information for all mounted partitions.

    Returns:
        list[dict]: A list of dictionaries, where each dictionary represents
                    a disk partition and its usage details in GB.
    """
    drives = []
    # Iterate over all physical disk partitions
    for part in psutil.disk_partitions(all=False):
        try:
            # Get disk usage for the mount point
            usage = psutil.disk_usage(part.mountpoint)
        except PermissionError:
            # Skip partitions for which usage information cannot be read
            continue

        drives.append({
            "device": part.device,
            "mountpoint": part.mountpoint,
            "fstype": part.fstype,
            # Convert bytes to Gigabytes (1024^3) and round
            "total_gb": round(usage.total / (1024 ** 3), 2),
            "used_gb": round(usage.used / (1024 ** 3), 2),
            "free_gb": round(usage.free / (1024 ** 3), 2),
            "percent_used": usage.percent,
        })
    return drives