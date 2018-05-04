import pytest
from ..config import *
from ..filters import *
import math


inp = [
    [
        {
            "platforms": [{ "name": "ps2", "date": "1999"}]
        },
        [
            [
                {
                    "platforms": [{ "name": "ps2", "date": "2001"}]
                },
                0.9
            ],
            [
                {
                    "platforms": [{ "name": "ps2", "date": "2000"}]
                },
                0.8
            ],
            [
                {
                    "platforms": [{ "name": "ps2", "date": "1999" }, {"name": "xbox", "date": "1999"}]
                },
                0.9
            ]
        ]
    ]
]

outp = [
    [
        {
            "platforms": [{ "name": "ps2", "date": "1999"}]
        },
        [
            [
                {
                    "platforms": [{ "name": "ps2", "date": "2001"}]
                },
                round(0.9-(math.log(3)/10),10)
            ],
            [
                {
                    "platforms": [{ "name": "ps2", "date": "2000"}]
                },
                round(0.8-(math.log(2)/10),10)
            ],
            [
                {
                    "platforms": [{ "name": "ps2", "date": "1999" }, {"name": "xbox", "date": "1999"}]
                },
                round(0.9,10)
            ]
        ]
    ]
]

@pytest.mark.parametrize(
    "matches_input, matches_output",
    [
        (inp, outp)
    ]
)
def test_closest_year_filter(matches_input, matches_output):
    assert closest_year_filter(matches_input) == matches_output



# PERFECT MATCH FILTER


inp = [
    [
        {
            "platforms": [{ "name": "ps2", "year": 1999}]
        },
        [
            [
                {
                    "platforms": [{ "name": "ps2", "year": 2001}]
                },
                1.0
            ],
            [
                {
                    "platforms": [{ "name": "ps2", "year": 2000}]
                },
                0.8
            ],
            [
                {
                    "platforms": [{ "name": "ps2", "year": 1999 }, {"name": "xbox", "year": 1999}]
                },
                0.9
            ]
        ]
    ]
]

outp = [
    [
        {
            "platforms": [{ "name": "ps2", "year": 1999}]
        },
        [
            [
                {
                    "platforms": [{ "name": "ps2", "year": 2001}]
                },
                1.0
            ]
        ]
    ]
]

@pytest.mark.parametrize(
    "matches_input, matches_output",
    [
        (inp, outp)
    ]
)
def test_perfect_match_filter(matches_input, matches_output):
    matches = closest_year_filter(matches_input)
    assert perfect_match_filter(matches) == matches_output
