import os
import subprocess
import time
import tempfile
import shutil
import sys
import threading
from pathlib import Path
from typing import Optional
import psutil
import random

from collections import deque


class OpenVPNManagerError(Exception):
    """Custom exception for OpenVPNManager errors."""

    pass


class OpenVPNManager:
    """
    Manages multiple OpenVPN connections on Windows using a specific list of .ovpn files.

    Accepts default credentials or a mapping of credentials per configuration during
    initialization. Handles connecting, disconnecting, status checks, and streams
    OpenVPN process output to the terminal for managed connections.

    Includes functionality to cycle through connections based on least recent usage.
    """

    def __init__(
        self,
        config_files: list[str | Path],
        default_credentials: Optional[tuple[str, str]] = None,
        credentials_map: Optional[dict[str, tuple[str, str]]] = None,
        initial_queue_order: str = "random",
    ):
        """
        Initializes the OpenVPN Manager with a specific list of config files and credentials.

        Args:
            config_files: A list of paths (str or Path objects) to the .ovpn files to manage.
            default_credentials: Optional tuple (username, password) to use for connections
                                 if specific credentials aren't provided in credentials_map.
            credentials_map: Optional dictionary mapping config_name (file stem) to a
                             tuple (username, password) for specific configurations.
            initial_queue_order: How to initially order the connection cycle queue.
                                 'sorted' (default) or 'random'.

        Raises:
            OpenVPNManagerError: If file paths are invalid, files don't exist, aren't files,
                                 or if the OpenVPN executable cannot be found.
            TypeError: If config_files is not list/tuple or credential formats are incorrect.
            ValueError: If initial_queue_order is invalid.
        """
        if not isinstance(config_files, (list, tuple)):
            raise TypeError("config_files must be a list or tuple of paths.")
        if default_credentials and not (
            isinstance(default_credentials, tuple) and len(default_credentials) == 2
        ):
            raise TypeError(
                "default_credentials must be a tuple of (username, password)"
            )
        if credentials_map:
            if not isinstance(credentials_map, dict):
                raise TypeError("credentials_map must be a dictionary.")
            for name, creds in credentials_map.items():
                if not (isinstance(creds, tuple) and len(creds) == 2):
                    raise TypeError(
                        f"Credentials for '{name}' in credentials_map must be a tuple of (username, password)"
                    )
        if initial_queue_order not in ["sorted", "random"]:
            raise ValueError("initial_queue_order must be 'sorted' or 'random'")

        self.openvpn_executable = self._find_openvpn_executable()
        if not self.openvpn_executable:
            raise OpenVPNManagerError(
                "OpenVPN executable not found in PATH or standard locations."
            )

        self.available_configs: dict[str, Path] = {}  # {config_name: config_path}
        # {config_name: {'process': psutil.Process, 'stdout_thread': T, 'stderr_thread': T, 'log_capture': bool}}
        self.active_connections: dict[str, dict] = {}
        self._log_lock = threading.Lock()
        self._queue_lock = threading.Lock()  # Lock specific to queue operations

        # Store credentials internally
        self._default_credentials = default_credentials
        self._credentials_map = credentials_map if credentials_map else {}

        # --- Validate and Populate managed files ---
        if not config_files:
            print("Warning: No configuration files provided to manage.")

        temp_config_list = []
        for file_path_input in config_files:
            try:
                file_path = Path(file_path_input).resolve()
                if not file_path.exists():
                    raise OpenVPNManagerError(f"Config file not found: {file_path}")
                if not file_path.is_file():
                    raise OpenVPNManagerError(f"Config path is not a file: {file_path}")

                config_name = file_path.stem
                if config_name in self.available_configs:
                    print(
                        f"Warning: Duplicate config name '{config_name}' detected. File {file_path} overwrites previous entry {self.available_configs[config_name]}."
                    )
                self.available_configs[config_name] = file_path
                if (
                    config_name not in temp_config_list
                ):  # Avoid duplicates in list for queue
                    temp_config_list.append(config_name)

            except TypeError as e:
                raise OpenVPNManagerError(
                    f"Invalid item in config_files list: {file_path_input}. Error: {e}"
                )

        # --- Initialize Connection Queue ---
        if initial_queue_order == "sorted":
            initial_queue_items = sorted(temp_config_list)
        else:  # random
            initial_queue_items = random.sample(temp_config_list, len(temp_config_list))

        # The deque stores the order: front=least_recently_used, back=most_recently_used
        self._connection_queue: deque[str] = deque(initial_queue_items)

        self._update_connection_state()  # Populate initial state

    # --- Logging Helper ---
    def _log(self, message: str):
        """Prints messages safely from multiple threads."""
        with self._log_lock:
            print(message, flush=True)

    # --- Find Executable ---
    def _find_openvpn_executable(self) -> Optional[str]:
        """Finds the OpenVPN executable in the system PATH or common locations."""
        executable = shutil.which("openvpn.exe") or shutil.which("openvpn")
        if executable:
            return executable
        common_paths = [
            Path("C:/Program Files/OpenVPN/bin/openvpn.exe"),
            Path("C:/Program Files (x86)/OpenVPN/bin/openvpn.exe"),
        ]
        for p in common_paths:
            if p.exists():
                return str(p)
        return None

    # --- Process Helpers ---
    def _get_process_for_config(self, config_path: Path) -> Optional[psutil.Process]:
        config_filename_str = str(config_path)
        config_filename_alt_slash = config_filename_str.replace("\\", "/")
        for proc in psutil.process_iter(["pid", "name", "cmdline"]):
            try:
                proc_name = proc.info["name"]
                if proc_name and proc_name.lower() == "openvpn.exe":
                    cmdline = proc.info["cmdline"]
                    if cmdline and isinstance(cmdline, list) and len(cmdline) > 1:
                        full_cmd = " ".join(cmdline)
                        if "--config" in full_cmd and (
                            config_filename_str in full_cmd
                            or config_filename_alt_slash in full_cmd
                        ):
                            # Basic check to avoid matching the script itself if it spawns openvpn
                            if (
                                sys.executable not in full_cmd
                                and sys.argv[0] not in full_cmd
                            ):
                                return proc
            except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
                continue
            except Exception:  # Catch potential other errors during iteration
                pass
        return None

    def _is_process_running(self, process_obj: Optional[psutil.Process]) -> bool:
        if process_obj is None:
            return False
        try:
            # Check if running and not a zombie
            return (
                process_obj.is_running()
                and process_obj.status() != psutil.STATUS_ZOMBIE
            )
        except psutil.Error:  # Catches NoSuchProcess, AccessDenied etc.
            return False

    # --- State Management ---
    def _update_connection_state(self):
        """Checks processes matching managed configs and updates internal state."""
        active_found = {}
        found_count = 0
        # Check only configs we manage
        for config_name, config_path in self.available_configs.items():
            try:
                process_obj = self._get_process_for_config(config_path)
                if process_obj and self._is_process_running(process_obj):
                    existing_entry = self.active_connections.get(config_name)
                    # If we are already tracking this exact PID with log capture, keep it
                    if (
                        existing_entry
                        and existing_entry.get("process")
                        and existing_entry["process"].pid == process_obj.pid
                        and existing_entry.get("log_capture")
                    ):
                        active_found[config_name] = existing_entry
                    else:  # Otherwise, track it without logs (or update if PID changed)
                        active_found[config_name] = {
                            "process": process_obj,
                            "stdout_thread": None,
                            "stderr_thread": None,
                            "log_capture": False,  # Mark as false since we didn't start it here
                        }
                    found_count += 1
            except Exception as e:
                # Be more specific about the error if possible
                self._log(f"  Warning: Error checking state for '{config_name}': {e}")

        # Remove tracked connections whose process is no longer running
        current_active_names = list(self.active_connections.keys())
        for name in current_active_names:
            if name not in active_found:
                # Check if the process we *were* tracking is actually gone
                old_data = self.active_connections.get(name)
                if old_data and not self._is_process_running(old_data.get("process")):
                    # Clean up threads if they exist and are somehow alive (unlikely)
                    # In practice, daemon threads should exit when the process object is GC'd
                    # or when the main thread exits. Explicit join isn't usually needed here.
                    # self._log(f"Cleaning up stale entry for '{name}'") # Debug log
                    pass  # Just remove from tracking below
                elif name in active_found:
                    # This case means a new process was found for the same config name
                    # The old entry will be replaced below.
                    pass
                # else: process might still be running but didn't match _get_process_for_config? Unlikely.

        self.active_connections = active_found
        # Optional: Log summary
        # self._log(f"Connection state updated. Tracking {len(self.active_connections)} active connection(s).")

    # --- Stream Reading ---
    def _stream_reader(self, stream, stream_name: str, config_name: str):
        """Reads lines from a stream and prints them with context."""
        try:
            # Use iter(stream.readline, '') which is robust for PIPE streams
            for line in iter(stream.readline, ""):
                if line:  # Avoid printing empty lines often caused by termination
                    self._log(f"[{config_name}:{stream_name}] {line.strip()}")
                else:
                    # Empty string usually means EOF
                    break
        except ValueError:
            # Can happen if the stream is closed abruptly between iter and readline
            pass  # self._log(f"[{config_name}:{stream_name}] Stream closed (ValueError).")
        except Exception as e:
            self._log(f"[{config_name}:{stream_name}] Error reading stream: {e}")
        # finally: self._log(f"[{config_name}:{stream_name}] Reader thread finished.") # Can be verbose

    # --- Queue Management ---
    def _update_connection_queue(self, config_name: str):
        """Moves the specified config_name to the end (most recent) of the queue."""
        with self._queue_lock:
            try:
                # Remove the item if it exists
                self._connection_queue.remove(config_name)
            except ValueError:
                # Item wasn't in the queue (maybe added externally? or not managed?)
                # Or this is the very first connection for this item.
                # We should still add it as the most recent.
                if config_name not in self.available_configs:
                    self._log(
                        f"Warning: Tried to update queue for unmanaged config '{config_name}'."
                    )
                    return  # Don't add unmanaged things to the queue
            # Append the item to the end (most recent)
            self._connection_queue.append(config_name)
            # self._log(f"Updated connection queue: {list(self._connection_queue)}") # Debug log

    # --- Public Methods ---
    def refresh(self):
        """Updates the status of active connections associated with the managed files."""
        # self._log("Refreshing connection state...")
        self._update_connection_state()
        # self._log("Refresh complete.")

    def list_managed(self) -> list[str]:
        """Returns a sorted list of managed configuration names."""
        return sorted(list(self.available_configs.keys()))

    def get_status(self) -> dict:
        """Checks current connection status for managed files."""
        self.refresh()  # Ensure state is up-to-date
        status = {
            "managed_files": {
                name: str(path) for name, path in self.available_configs.items()
            },
            "active_connections": {},
            "connection_queue": list(
                self._connection_queue
            ),  # Show current queue order
        }
        for name, conn_data in self.active_connections.items():
            proc_obj = conn_data.get("process")
            # Double check if process is still valid before reporting
            if proc_obj and self._is_process_running(proc_obj):
                status["active_connections"][name] = {
                    "pid": proc_obj.pid,
                    "log_capture": conn_data.get("log_capture", False),
                    # Optionally add more info like start time if needed
                    # "start_time": datetime.datetime.fromtimestamp(proc_obj.create_time()).isoformat()
                }
            # else: # If process died since refresh, don't report it as active
            #    pass # Or explicitly remove from active_connections here? Refresh should handle it.

        return status

    def connect(
        self,
        config_name: str,
        timeout_sec: int = 15,
    ) -> bool:
        """
        Connects to the specified *managed* VPN configuration using stored credentials.
        Updates the connection usage queue on successful connection.

        Args:
            config_name: The name (file stem) of the managed configuration to connect to.
            timeout_sec: How long to wait for process stability confirmation (default 15s).

        Returns:
            True if connection process started successfully and is running after timeout, False otherwise.
        """
        if config_name not in self.available_configs:
            self._log(
                f"Error: Configuration '{config_name}' is not managed by this instance."
            )
            self._log(f"Managed configs are: {', '.join(self.list_managed())}")
            return False

        config_path = self.available_configs[config_name]

        # Check if already running/tracked
        # Call refresh first to ensure active_connections is up to date
        self.refresh()
        if config_name in self.active_connections:
            conn_data = self.active_connections[config_name]
            proc_obj = conn_data.get("process")
            if self._is_process_running(proc_obj):
                self._log(
                    f"Info: '{config_name}' is already connected (PID: {proc_obj.pid}). Log capture: {conn_data.get('log_capture', False)}."
                )
                # Even if already connected, mark it as most recently used now
                self._update_connection_queue(config_name)
                return True
            else:
                self._log(
                    f"Info: Found stale track for '{config_name}'. Removing before reconnect."
                )
                # remove stale entry before attempting reconnect
                del self.active_connections[config_name]

        # Check untracked running process matching managed file (redundant if refresh() is called, but safe)
        existing_process = self._get_process_for_config(config_path)
        if existing_process and self._is_process_running(existing_process):
            self._log(
                f"Info: '{config_name}' is already running (PID: {existing_process.pid}) but logs cannot be captured by this manager instance. Tracking."
            )
            # Track it without logs
            self.active_connections[config_name] = {
                "process": existing_process,
                "stdout_thread": None,
                "stderr_thread": None,
                "log_capture": False,
            }
            # Mark as most recently used
            self._update_connection_queue(config_name)
            return True

        # --- Determine Credentials ---
        username: Optional[str] = None
        password: Optional[str] = None
        creds_found = False
        if config_name in self._credentials_map:
            username, password = self._credentials_map[config_name]
            creds_found = True
            # self._log(f"Using specific credentials for '{config_name}'.")
        elif self._default_credentials:
            username, password = self._default_credentials
            creds_found = True
            # self._log(f"Using default credentials for '{config_name}'.")

        # --- Prepare and Execute ---
        command = [self.openvpn_executable, "--config", str(config_path)]
        temp_cred_file = None  # Path object or None

        # Handle credentials file creation securely
        if creds_found and username is not None and password is not None:
            try:
                # Use mkstemp for secure temporary file creation
                fd, temp_cred_path_str = tempfile.mkstemp(
                    text=True, prefix=f"ovpn_cred_{config_name}_"
                )
                with os.fdopen(fd, "w") as f:
                    f.write(f"{username}\n{password}\n")
                temp_cred_file = Path(temp_cred_path_str)
                command.extend(["--auth-user-pass", str(temp_cred_file)])
                # self._log(f"Using temporary credential file: {temp_cred_file}") # Security: Avoid logging path?
            except Exception as e:
                self._log(f"Error: Failed to create temporary credential file: {e}")
                # Cleanup if file was partially created (though mkstemp is atomic)
                if temp_cred_file and temp_cred_file.exists():
                    try:
                        os.remove(temp_cred_file)
                    except OSError:
                        pass
                return False  # Cannot proceed without cred file

        process_obj = None
        stdout_thread = None
        stderr_thread = None
        popen_process = None

        try:
            self._log(f"Attempting to connect to '{config_name}'...")
            # Use Popen for non-blocking execution and stream capture
            popen_process = subprocess.Popen(
                command,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,  # Decode stdout/stderr as text
                encoding="utf-8",  # Specify encoding
                errors="replace",  # How to handle decoding errors
                bufsize=1,  # Line buffered
                # Consider adding startupinfo=subprocess.STARTUPINFO(dwFlags=subprocess.CREATE_NO_WINDOW) on Windows
                # if you don't want a console window flashing briefly.
            )
            initial_pid = popen_process.pid
            self._log(
                f"OpenVPN process initiated (PID: {initial_pid}). Starting log readers..."
            )

            # Start threads to read stdout and stderr concurrently
            stdout_thread = threading.Thread(
                target=self._stream_reader,
                args=(popen_process.stdout, "stdout", config_name),
                daemon=True,  # Daemon threads exit when main program exits
            )
            stderr_thread = threading.Thread(
                target=self._stream_reader,
                args=(popen_process.stderr, "stderr", config_name),
                daemon=True,
            )
            stdout_thread.start()
            stderr_thread.start()

            # Wait for process stability - check quickly first
            time.sleep(1.0)  # Give it a moment to potentially fail fast
            quick_exit_code = popen_process.poll()
            if quick_exit_code is not None:
                self._log(
                    f"Error: OpenVPN process for '{config_name}' terminated quickly (RC: {quick_exit_code}). Check logs above."
                )
                # Ensure threads are finished or stop them if possible (tricky with PIPE reads)
                # Usually letting daemon threads finish naturally is okay here.
                raise OpenVPNManagerError("Process terminated quickly")

            # Wait remaining time for stability, polling intermittently
            wait_interval = 0.5
            elapsed_wait = 1.0
            while elapsed_wait < timeout_sec:
                exit_code = popen_process.poll()
                if exit_code is not None:
                    self._log(
                        f"Error: OpenVPN process for '{config_name}' terminated during wait (RC: {exit_code}). Check logs above."
                    )
                    raise OpenVPNManagerError("Process terminated during wait")

                # Check if process is findable via psutil (more robust check)
                try:
                    proc_check = psutil.Process(initial_pid)
                    if self._is_process_running(proc_check):
                        # Optional: Add check for specific log message indicating successful connection? More complex.
                        pass  # Still running, continue waiting
                    else:
                        # Found by PID but not running according to psutil status
                        raise OpenVPNManagerError(
                            "Process found by PID but status is not running/zombie"
                        )
                except psutil.NoSuchProcess:
                    # Process disappeared entirely
                    raise OpenVPNManagerError(
                        "Process disappeared during wait (NoSuchProcess)"
                    )

                time.sleep(wait_interval)
                elapsed_wait += wait_interval

            # If loop finished without error, process should be stable
            # Final confirmation using psutil
            try:
                process_obj = psutil.Process(initial_pid)
                if self._is_process_running(process_obj):
                    self.active_connections[config_name] = {
                        "process": process_obj,
                        "stdout_thread": stdout_thread,
                        "stderr_thread": stderr_thread,
                        "log_capture": True,
                    }
                    self._log(
                        f"Success: OpenVPN process for '{config_name}' (PID: {initial_pid}) appears stable after {timeout_sec}s. Log capture active."
                    )
                    # --- SUCCESS: Update the queue ---
                    self._update_connection_queue(config_name)
                    # --- End Queue Update ---
                    return True
                else:
                    # Should have been caught earlier, but final check
                    raise OpenVPNManagerError(
                        "Process found but not running after timeout period"
                    )
            except psutil.NoSuchProcess:
                raise OpenVPNManagerError(
                    "Process disappeared after timeout period (NoSuchProcess)"
                )

        except OpenVPNManagerError as e:
            # Specific errors logged where they occur
            self._log(f"Connection attempt for '{config_name}' failed: {e}")
            # Ensure subprocess is cleaned up if it's somehow still running (should have exited)
            if popen_process and popen_process.poll() is None:
                try:
                    popen_process.terminate()
                except Exception:
                    pass
            return False
        except FileNotFoundError:
            self._log(
                f"Error: OpenVPN executable not found at '{self.openvpn_executable}'."
            )
            return False
        except PermissionError:
            self._log(
                f"Error: Permission denied trying to start OpenVPN from '{self.openvpn_executable}'."
            )
            return False
        except Exception as e:
            # Catch-all for unexpected errors during setup or Popen
            self._log(
                f"Error: An unexpected error occurred trying to connect '{config_name}': {e}"
            )
            if popen_process and popen_process.poll() is None:
                try:
                    popen_process.terminate()
                except Exception:
                    pass
            return False
        finally:
            # --- Securely remove the temporary credential file ---
            if temp_cred_file and temp_cred_file.exists():
                try:
                    os.remove(temp_cred_file)
                    # self._log(f"Removed temporary credential file: {temp_cred_file}")
                except OSError as e:
                    self._log(
                        f"Warning: Failed to remove temp cred file {temp_cred_file}: {e}"
                    )

    def is_connected(self) -> bool:
        """Checks if any managed connections are currently active."""
        self.refresh()
        return len(self.active_connections) > 0

    def disconnect(self, config_name: str, kill_timeout_sec: int = 5) -> bool:
        """
        Disconnects the specified *managed* VPN configuration.

        Args:
            config_name: The name (file stem) of the configuration to disconnect.
            kill_timeout_sec: Seconds to wait for graceful termination before killing.

        Returns:
            True if the process was stopped or not running, False on failure.
        """
        # Refresh state first to ensure we are targeting the correct process
        self.refresh()

        process_obj_to_stop: Optional[psutil.Process] = None
        pid_to_kill = -1
        source = "none"  # For logging

        # Check if it's actively tracked with log capture etc.
        if config_name in self.active_connections:
            conn_data = self.active_connections[config_name]
            process_obj_to_stop = conn_data.get("process")
            if process_obj_to_stop and self._is_process_running(process_obj_to_stop):
                pid_to_kill = process_obj_to_stop.pid
                source = "tracked"
                # self._log(f"Disconnecting tracked connection '{config_name}' (PID: {pid_to_kill})...")
            else:
                # Was tracked but process seems gone already
                # self._log(f"Info: Tracked process for '{config_name}' is not running.")
                if config_name in self.active_connections:
                    del self.active_connections[config_name]  # Clean up tracking entry
                return True  # Considered success as it's not running

        # If not tracked or tracking was stale, try finding by config path again
        if not process_obj_to_stop:
            config_path = self.available_configs.get(config_name)
            if config_path:
                # self._log(f"'{config_name}' not actively tracked, checking system processes...")
                process_obj_to_stop = self._get_process_for_config(config_path)
                if process_obj_to_stop and self._is_process_running(
                    process_obj_to_stop
                ):
                    pid_to_kill = process_obj_to_stop.pid
                    source = "found"
                    self._log(
                        f"Found untracked running process matching managed file '{config_name}' (PID: {pid_to_kill}). Disconnecting..."
                    )
                else:
                    process_obj_to_stop = None  # Ensure it's None if not found/running

        # If no process found by either method
        if not process_obj_to_stop:
            # self._log(f"Info: No running process found for '{config_name}'.")
            # Ensure any potentially stale tracking entry is removed
            if config_name in self.active_connections:
                del self.active_connections[config_name]
            return True  # Success as it's not running

        # --- Attempt Termination ---
        self._log(
            f"Attempting to terminate '{config_name}' (PID: {pid_to_kill}, Source: {source})..."
        )
        try:
            # proc_name = process_obj_to_stop.name() # Can fail if process disappears mid-check
            # Try graceful terminate first
            process_obj_to_stop.terminate()
            # Wait for process to exit using psutil.wait_procs for robustness
            # This waits up to kill_timeout_sec for the process to enter terminated/dead state
            gone, alive = psutil.wait_procs(
                [process_obj_to_stop], timeout=kill_timeout_sec
            )

            if process_obj_to_stop in alive:
                # Process didn't terminate gracefully within timeout
                self._log(
                    f"Warning: Process {pid_to_kill} did not terminate gracefully after {kill_timeout_sec}s. Sending KILL signal..."
                )
                process_obj_to_stop.kill()
                # Wait a short extra time after kill
                gone, alive = psutil.wait_procs([process_obj_to_stop], timeout=2)
                if process_obj_to_stop in alive:
                    # Still alive even after kill - this is problematic
                    self._log(
                        f"Error: Process {pid_to_kill} failed to terminate even after kill signal."
                    )
                    # Remove from tracking anyway, as we can't manage it
                    if config_name in self.active_connections:
                        del self.active_connections[config_name]
                    return False  # Indicate failure

            # If we reach here, process is gone (either terminated or killed)
            self._log(
                f"Successfully terminated process for '{config_name}' (PID: {pid_to_kill})."
            )
            # Remove from tracking
            if config_name in self.active_connections:
                del self.active_connections[config_name]
            return True

        except psutil.NoSuchProcess:
            # Process disappeared between finding it and trying to terminate/wait
            self._log(
                f"Info: Process for '{config_name}' (PID: {pid_to_kill}) disappeared before termination completed."
            )
            if config_name in self.active_connections:
                del self.active_connections[config_name]  # Clean up tracking
            return True  # Considered success as it's gone
        except psutil.AccessDenied:
            self._log(
                f"Error: Access denied trying to terminate PID {pid_to_kill} for '{config_name}'. Check permissions."
            )
            # Remove from tracking as we can't manage it
            if config_name in self.active_connections:
                del self.active_connections[config_name]
            return False
        except Exception as e:
            # Catch other potential errors during termination/waiting
            self._log(
                f"Error: Unexpected error disconnecting '{config_name}' (PID: {pid_to_kill}): {e}"
            )
            if config_name in self.active_connections:
                del self.active_connections[config_name]
            return False

    def disconnect_all(self, kill_timeout_sec: int = 5) -> dict[str, bool]:
        """
        Attempts to disconnect all currently active managed connections (tracked or found).

        Args:
            kill_timeout_sec: Timeout for graceful termination per connection.

        Returns:
            A dictionary mapping config_name to disconnection success (bool).
        """
        self._log("Attempting to disconnect all active managed connections...")
        self.refresh()  # Update state first

        results = {}
        # Get names from both tracked connections and available configs to ensure we try all
        all_managed_names = set(self.available_configs.keys())
        # Names currently thought to be active (includes tracked and potentially untracked found during refresh)
        currently_active_names = set(self.active_connections.keys())

        # We should attempt to disconnect any process associated with a managed file
        # regardless of whether it's currently in self.active_connections after the refresh.
        # The disconnect method itself handles finding the process if it's not tracked.
        names_to_try_disconnect = sorted(list(all_managed_names))

        if not names_to_try_disconnect:
            self._log("No managed configurations loaded.")
            return {}

        self._log(
            f"Checking managed configs for running processes: {', '.join(names_to_try_disconnect)}"
        )

        active_found_count = 0
        for config_name in names_to_try_disconnect:
            # The disconnect method internally checks if the process is running
            # No need to pre-check here, simplifies logic.
            # Let disconnect handle logging if it's already stopped.
            is_success = self.disconnect(config_name, kill_timeout_sec)
            results[config_name] = is_success
            if is_success and config_name in currently_active_names:
                # Count how many we successfully stopped that were known to be active
                active_found_count += 1
            elif not is_success:
                self._log(f"Disconnect All: Failed for '{config_name}'.")

        return results

    def change_vpn_connection(self, timeout_sec: int = 15) -> bool:
        """
        Disconnects all current managed connections and connects to the
        least recently used configuration based on the internal queue.

        Args:
            timeout_sec: How long to wait for the new connection process stability.

        Returns:
            True if the cycle was successful (disconnect worked or wasn't needed,
            and the new connection started successfully), False otherwise. Returns False
            if there are no managed configurations.
        """
        with self._queue_lock:  # Lock queue access while deciding
            if not self._connection_queue:
                self._log(
                    "Error: Cannot cycle connection, the connection queue is empty (no managed configs?)."
                )
                return False
            # Peek at the least recently used (front of deque)
            next_config_name = self._connection_queue[0]

        self._log(
            f"Cycling VPN connection: Disconnecting all, then connecting to least recently used: '{next_config_name}'..."
        )

        # Disconnect all existing connections
        disconnect_results = self.disconnect_all()
        failed_disconnects = [name for name, ok in disconnect_results.items() if not ok]
        if failed_disconnects:
            self._log(
                f"Warning: Failed to disconnect the following during cycle: {', '.join(failed_disconnects)}. Attempting connection to '{next_config_name}' anyway."
            )

        # Attempt to connect to the least recently used configuration.
        # The connect method will automatically update the queue on success.
        return self.connect(next_config_name, timeout_sec)

    def get_connection_queue(self) -> list[str]:
        """Returns a copy of the current connection queue order (least to most recent)."""
        with self._queue_lock:
            return list(self._connection_queue)
