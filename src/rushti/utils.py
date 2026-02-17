"""RushTI utility functions.

Stateless helpers for path resolution and data manipulation.
"""

import logging
import os
import stat
import sys

logger = logging.getLogger(__name__)


def set_current_directory():
    """Get the application's working directory for config resolution.

    For frozen executables (PyInstaller), returns the directory containing
    the executable. For normal Python scripts, returns the current working
    directory (where the user invoked the script from).

    Note: This function no longer changes the working directory (os.chdir)
    as that caused issues with relative paths in the new project structure.
    """
    if getattr(sys, "frozen", False):
        # Frozen exe: use directory of the executable
        application_path = os.path.abspath(sys.executable)
        return os.path.dirname(application_path)
    else:
        # Normal script: use current working directory
        return os.getcwd()


def get_application_directory():
    """Get the application's root directory.

    Resolution order:
    1. RUSHTI_DIR environment variable (if set)
    2. For frozen executables (PyInstaller): directory containing the executable
    3. For normal Python scripts: project root (two levels up from src/rushti/)

    This is used for resolving paths to application files (logs, database, checkpoints,
    visualizations) that should always be relative to the application location,
    regardless of where the application is invoked from.

    :return: Absolute path to application directory
    """
    # 1. RUSHTI_DIR environment variable takes precedence
    rushti_dir = os.environ.get("RUSHTI_DIR")
    if rushti_dir:
        return os.path.abspath(rushti_dir)

    if getattr(sys, "frozen", False):
        # Frozen exe: use directory of the executable
        application_path = os.path.abspath(sys.executable)
        return os.path.dirname(application_path)
    else:
        # Normal script: use directory containing this file's parent (src/rushti -> src)
        # Then go up one more level to get the project root
        script_dir = os.path.dirname(os.path.abspath(__file__))
        # Go up two levels: src/rushti -> src -> project_root
        return os.path.dirname(os.path.dirname(script_dir))


def resolve_app_path(relative_path):
    """Resolve a path relative to the application directory.

    For relative paths, prepends the application directory.
    For absolute paths, returns the path unchanged.

    :param relative_path: Path to resolve (relative or absolute)
    :return: Absolute path (normalized)
    """
    if os.path.isabs(relative_path):
        return relative_path
    # Join with app directory and normalize to remove any ./ or ../ components
    resolved = os.path.join(get_application_directory(), relative_path)
    return os.path.normpath(resolved)


def ensure_shared_file(path):
    """Set file permissions so any user can read and write.

    RushTI may be run by different OS users (developers via terminal,
    service accounts via TM1 or Control-M). Files created by one user
    must remain writable by others.

    Sets permissions to rw-rw-rw- (0o666) on POSIX systems.
    On Windows this is a no-op (ACLs govern access, not POSIX bits).

    :param path: Path to file
    """
    if os.name == "nt":
        return
    try:
        os.chmod(path, stat.S_IRUSR | stat.S_IWUSR | stat.S_IRGRP | stat.S_IWGRP | stat.S_IROTH | stat.S_IWOTH)
    except OSError as e:
        logger.debug(f"Could not set shared permissions on {path}: {e}")


def ensure_shared_dir(path):
    """Set directory permissions so any user can read, write, and traverse.

    Sets permissions to rwxrwxrwx (0o777) on POSIX systems.
    On Windows this is a no-op.

    :param path: Path to directory
    """
    if os.name == "nt":
        return
    try:
        os.chmod(path, stat.S_IRWXU | stat.S_IRWXG | stat.S_IRWXO)
    except OSError as e:
        logger.debug(f"Could not set shared permissions on {path}: {e}")


def makedirs_shared(path, exist_ok=True):
    """Create directories with shared (world-writable) permissions.

    Each newly created directory in the hierarchy is made accessible
    to all users so that different accounts running RushTI can write
    to the same data/log/checkpoint directories.

    :param path: Directory path to create
    :param exist_ok: If True, don't raise if directory already exists
    """
    path = os.path.normpath(path)
    if os.path.isdir(path):
        # Directory exists — still ensure permissions are open
        ensure_shared_dir(path)
        return

    # Walk up to find the first existing ancestor, then create downwards
    to_create = []
    current = path
    while not os.path.isdir(current):
        to_create.append(current)
        parent = os.path.dirname(current)
        if parent == current:
            break
        current = parent

    # Create from top-most missing directory downwards
    for d in reversed(to_create):
        os.makedirs(d, exist_ok=True)
        ensure_shared_dir(d)


def flatten_to_list(items) -> list:
    """Flatten nested iterables into a single flat list.

    :param items: Nested iterable (list, tuple, set) or single item
    :return: Flat list of all items
    """
    result = []
    if not isinstance(items, str):
        for item in items:
            if isinstance(item, (list, tuple, set)):
                result.extend(flatten_to_list(item))
            else:
                result.append(item)
    else:
        result.append(items)
    return result
