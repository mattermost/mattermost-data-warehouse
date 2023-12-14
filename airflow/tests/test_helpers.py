import pytest
from mattermost_dags._helpers import chunk


@pytest.mark.parametrize(
    "input,size,pad,output",
    [
        [['a', 'b', 'c', 'd'], 2, False, [('a', 'b'), ('c', 'd')]],
        [['a', 'b', 'c', 'd', 'e'], 2, False, [('a', 'b'), ('c', 'd')]],
        [['a', 'b', 'c', 'd', 'e'], 2, True, [('a', 'b'), ('c', 'd'), ('e', None)]],
        [[], 2, False, []],
        [[], 2, True, []],
    ],
)
def test_chunks(input, size, pad, output):
    assert list(chunk(input, size, pad=pad)) == output
