description = "a module housing helper routines for simulateGraceDBEvent and simulateGraceDBStream"

import numpy as np

#-------------------------------------------------

def dt( rate, distrib="poisson" ):
    """
    draws the time between events based on distrib and rate
    distrib can be either "poisson" or "uniform"
    """
    if distrib=="poisson":
        return poisson_dt( rate )
    elif distrib=="uniform":
        return uniform_dt( rate )
    else:
        raise ValueError("distrib=%s not understood"%distrib)

def uniform_dt( rate ):
    return 1/rate

def poisson_dt( rate ):
    return - np.log(1 - np.random.rand()) / rate
