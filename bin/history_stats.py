import pandas as pd
import numpy
import matplotlib.pyplot as plt

df = pd.read_csv('./bin/history.csv', names=['input', 'output', 'start', 'end'])  
df = df.dropna(axis=0, subset=['start', 'end'], ignore_index=True)
df['latency'] = df['end'] - df['start']

range = numpy.arange(0, df['end'].max() , 1, dtype=int)
latency = df.groupby(pd.cut(df['end'], range))['latency'].agg(['count', 'sum', 'mean', 'median', lambda x: numpy.quantile(x, 0.95), lambda x: numpy.quantile(x, 0.99), 'max'])
latency.columns = ['vazao', 'soma', 'media', 'mediana', '95_perc', '99_perc', 'max']
latency = latency.reset_index()

plot = latency.plot(kind='line', x='end', y='vazao')
plt.show()

temp_df = latency.groupby('vazao')['soma'].agg('sum')
temp_df = temp_df.reset_index()
print(temp_df)
temp_df['latencia'] = temp_df['soma']/temp_df['vazao']

#plot = temp_df.plot(kind='line', x='vazao', y='latencia')
#plt.show()


print(latency)
#print(df)