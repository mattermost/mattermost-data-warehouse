from itertools import zip_longest


def chunk(seq, n, pad=False, fillvalue=None):
    """
    Split sequence into chunks of length n.

    >>> list(partition(['a', 'b', 'c', 'd']))

    [('a', 'b'), ('c', 'd')]

    If the length of ``seq`` is not evenly divisible by ``n``, the final tuple
    is dropped if ``pad`` is false, or filled to length ``n`` by ``fillvalue``:

    >>> list(partition(['a', 'b', 'c', 'd', 'e']))
    [('a', 'b'), ('c', 'd')]

    >>> list(partition(2, [1, 2, 3, 4, 5], pad=True, fillvalue=None))
    [(1, 2), (3, 4), (5, None)]

    """
    args = [iter(seq)] * n
    if pad:
        return zip_longest(*args, fillvalue=fillvalue)
    else:
        return zip(*args)
