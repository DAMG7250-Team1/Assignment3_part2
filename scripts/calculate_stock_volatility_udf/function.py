import numpy as np

def main(prices):
    if not prices or len(prices) < 2:
        return 0.0
    
    # Convert to numpy array and calculate returns
    prices_array = np.array([float(p) for p in prices if p is not None])
    returns = np.log(prices_array[1:] / prices_array[:-1])
    
    # Calculate standard deviation (volatility)
    volatility = np.std(returns) * np.sqrt(252)  # Annualized
    
    return float(round(volatility, 4))
