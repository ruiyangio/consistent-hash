from scipy import stats

def find_first_ge(values, key):
    res = -1
    if key > values[-1]:
        return -1
    lo, high = 0, len(values) - 1
    res = -1
    while lo <= high:
        mid = lo + (high - lo) // 2
        if values[mid] == key:
            return key
        elif values[mid] < key:
            lo = mid + 1
        else:
            res, high = mid, mid - 1

    return values[res] if res != -1 else -1

def test_uniformality(data, buckets, n_items):
    # Ktest with uniform cumulative distribution function
    bucket_vol = n_items / buckets
    d, p = stats.kstest(data, stats.uniform(loc=0.0, scale=bucket_vol).cdf)
    if p < 0.01:
        return True
    return False
