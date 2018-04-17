import pytest
from ..utils import remove_html

@pytest.mark.parametrize("input_string,expected",[
    ("<b>ABC</b>", "ABC"),
    ("", ""),
    (None, None),
    ("der <i> abc", "der abc")
])
def test_remove_html(input_string, expected):
    assert remove_html(input_string) == expected