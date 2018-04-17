import pytest
from ..utils import remove_html, remove_punctuation, std_url, std

@pytest.mark.parametrize("input_string,expected",[
    ("<b>ABC</b>", "ABC"),
    ("", ""),
    (None, None),
    ("der <i> abc", "der abc")
])
def test_remove_html(input_string, expected):
    assert remove_html(input_string) == expected

@pytest.mark.parametrize("input_string,expected", [
    ("Yada.", "Yada"),
    ("yasd", "yasd"),
    (None, None),
    (".:!", ""),
    ("", "")
])
def test_remove_punctuation(input_string, expected):
    assert remove_punctuation(input_string) == expected

@pytest.mark.parametrize("url,expected", [
    ("http://yada.de", "yada.de"),
    ("www.fad.at", "www.fad.at"),
    ("https://diggr.link/blub/", "diggr.link/blub"),
    (None, None),
    ("", "")
])
def test_std_url(url, expected):
    assert std_url(url) == expected

@pytest.mark.parametrize("input_string, remove_strings, expected",[
    ("Evil Corp",None, "evil corp"),
    ("Evil Corp", ["corp"], "evil"),
    ("", ["abc"], ""),
    (None, None, None),
    ("Co. Ltd.", ["co ltd"], ""),
])
def test_std(input_string, remove_strings, expected):
    assert std(input_string, remove_strings=remove_strings) == expected