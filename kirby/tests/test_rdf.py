import pytest

from ..rdf import build_match_uri

def test_build_match_uri():

    a = "http://kirby.diggr.link/dataset/mobygames/98889"
    b = "http://kirby.diggr.link/dataset/giantbomb/3030-37961"
    c = "http://kirby.diggr.link/dataset/mobygames/98888"


    uri1 = build_match_uri(a, b)
    uri2 = build_match_uri(b, a)

    uri3 = build_match_uri(a, c)
    uri4 = build_match_uri(c, a)

    assert uri1 == uri2
    assert uri1 != uri3
    assert uri2 != uri3
    assert uri3 == uri4