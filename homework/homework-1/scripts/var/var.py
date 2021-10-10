import pandas as pd
import numpy as np

df = pd.read_csv('/data/ab_nyc.csv')
price = df["price"].to_list()
print("Numpy var: " + str(np.var(price)))
