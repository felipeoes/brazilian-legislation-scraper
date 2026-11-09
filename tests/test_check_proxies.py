import importlib.util
from pathlib import Path

_spec = importlib.util.spec_from_file_location(
    "check_proxies", Path(__file__).parents[1] / "scripts" / "check_proxies.py"
)
check_proxies = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(check_proxies)


def test_candidates_keep_one_proxy_per_exit_ip_fastest_first():
    """Paraná limits each IP: two proxies behind one exit IP would share a budget."""
    alive = [
        ("http://a:1", "1.1.1.1", 5.0),
        ("http://b:2", "2.2.2.2", 1.0),
        ("http://c:3", "1.1.1.1", 2.0),
        ("http://d:4", "9.9.9.9", 0.5),  # exits through our own IP: no new budget
        ("http://e:5", None, 0.1),  # dead
    ]
    assert check_proxies.candidates(alive, own_ip="9.9.9.9") == [
        "http://b:2",
        "http://c:3",
    ]
