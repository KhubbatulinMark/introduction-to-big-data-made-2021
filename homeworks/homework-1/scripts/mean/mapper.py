#!/usr/bin/env python

import sys

cum_sum, count = 0, 0

for line in sys.stdin:
	fields = line.rsplit(",", 7)
	if len(fields) > 7:
		price = fields[1]
		if price == 'price':
			continue
		if price is not None:
			cum_sum += float(price)
			count += 1

chunk_mean = cum_sum / count

print(count, chunk_mean)
