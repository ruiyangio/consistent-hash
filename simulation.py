import uuid
from hashprovider import get_hash_value
from scipy import stats

def test_get_hash():
    t = {}
    for i in range(100000):
        mo = get_hash_value(str(i)) % 5
        t[mo] = t[mo] + 1 if mo in t else 1

    d, p = stats.kstest(list(t.values()), stats.uniform(loc=0.0, scale=(100000.0/5)).cdf)
    if p < 0.01:
        print("uniform")
    else:
        print("broke")
