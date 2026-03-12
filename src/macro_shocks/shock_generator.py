from __future__ import annotations

import numpy as np


def generate_random_shocks(n: int = 10, mean: float = 0.1, std: float = 0.05) -> list[float]:
    """Generate random macro shocks using a normal distribution."""
    shocks = np.random.normal(loc=mean, scale=std, size=n)
    return [float(max(0.0, s)) for s in shocks]
