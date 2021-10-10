#!/usr/bin/python3
import sys

cum_sum = 0
count = 0
chunk_prices = []

for line in sys.stdin:
	fields = line.rsplit(",", 7)
	if len(fields) > 7:
		price = fields[1]
		if price == 'price':
			continue
		if price is not None:
			price = float(price)
			cum_sum += price
			count += 1
			chunk_prices.append(price)

chunk_mean = cum_sum / count
chunk_var = sum([(cp - chunk_mean) ** 2 for cp in chunk_prices]) / count

print(count, chunk_mean, chunk_var)
