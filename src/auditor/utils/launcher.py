import sys
import subprocess
import os
import platform
import socket


def find_free_port(start_port: int = 5000, max_tries: int = 10) -> int | None:
    """
    Scans for an available network port starting from 'start_port'.

    Returns:
        int: The first available port found.
        None: If all ports in the range are occupied.
    """
    for port in range(start_port, start_port + max_tries):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            try:
                # Attempt to bind to localhost to verify the port is free
                s.bind(('0.0.0.0', port))
                return port
            except OSError:
                # Port is currently in use, proceed to the next candidate
                continue
    return None


def launch_report_server_detached(project_id: int, db_path: str) -> bool:
    """
    Launches the PydPiper Studio Flask server in a NEW console window.
    This prevents the server logs from cluttering the active shell session.
    """

    # 1. Automatically find a free port in the 5000-5010 range
    port = find_free_port(5000, 10)

    if port is None:
        print("❌ Error: No free ports found between 5000 and 5010.")
        return False

    # 2. Identify the active Python interpreter
    python_exe = sys.executable

    # 3. Resolve the server entry point (app.py) path
    current_dir = os.path.dirname(os.path.abspath(__file__))
    server_script = os.path.abspath(os.path.join(current_dir, '..', 'server', 'app.py'))

    if not os.path.exists(server_script):
        print(f"❌ Error: Cannot find server script at {server_script}")
        return False

    # 4. Construct the command arguments
    cmd_args = [
        python_exe,
        server_script,
        "--project-id", str(project_id),
        "--db-path", str(db_path),
        "--port", str(port)
    ]

    print(f"🚀 Launching report server for '{project_id}' on port {port}...")

    try:
        system = platform.system()

        if system == "Windows":
            # Windows: CREATE_NEW_CONSOLE (flag 16) opens a dedicated CMD window
            subprocess.Popen(cmd_args, creationflags=subprocess.CREATE_NEW_CONSOLE)

        elif system == "Darwin":  # macOS
            # macOS: Use osascript to instruct Terminal.app to execute the command
            cmd_str = " ".join(f"'{arg}'" for arg in cmd_args)
            script = f'tell application "Terminal" to do script "{cmd_str}"'
            subprocess.Popen(['osascript', '-e', script])

        elif system == "Linux":
            # Linux: Attempt to find a suitable terminal emulator
            cmd_str = " ".join(f"'{arg}'" for arg in cmd_args)
            terminals = [
                ['gnome-terminal', '--', 'bash', '-c', f'{cmd_str}; exec bash'],
                ['xterm', '-e', f'{cmd_str}; read'],
                ['konsole', '-e', f'{cmd_str}']
            ]
            launched = False
            for term_cmd in terminals:
                try:
                    subprocess.Popen(term_cmd, start_new_session=True)
                    launched = True
                    break
                except FileNotFoundError:
                    continue

            if not launched:
                # Fallback: execute in background if no GUI terminal is found
                print("⚠️  No GUI terminal found, starting in background.")
                subprocess.Popen(cmd_args)

        print(f"✅ Server detached. Check the new window!")
        return True

    except Exception as e:
        print(f"❌ Failed to launch server: {e}")
        return False