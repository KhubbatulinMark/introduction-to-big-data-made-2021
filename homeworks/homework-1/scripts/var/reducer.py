#!/usr/bin/python3
import sys

cj, mj, vj = 0, 0, 0

for line in sys.stdin:
        ck, mk, vk = [float(el) for el in line.split()]
        cjk = cj + ck
        vi = ((cj*vj + ck*vk) / cjk) + cj * ck * ((mj - mk)/cjk)**2
        cj, mj, vj = ck, mk, vi

print("Map-Reduce var: " + str(vj))
