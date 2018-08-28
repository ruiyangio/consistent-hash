from scipy import stats

def find_first_ge(values, key):
    if key > values[-1]:
        return -1
    lo = 0
    high = len(values) - 1
    while lo < high:
        mid = (lo + high) // 2
        if values[mid] == key:
            return key
        elif values[mid] < key:
            lo = mid + 1
        elif values[mid] > key:
            high = mid

    return values[lo]

def test_uniformality(data, buckets, n_items):
    # Ktest with uniform cumulative distribution function
    d, p = stats.kstest(data, stats.uniform(loc=0.0, scale=(n_items / buckets)).cdf)
    if p < 0.01:
        return True
    return False
