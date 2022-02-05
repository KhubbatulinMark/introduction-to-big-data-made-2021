# Homework 6

## Random Hyperplanes LSH

Реализовать Random Hyperplanes LSH (для косинусного расстояния) в виде Spark Estimator
Основное задание [100 баллов]

1. Посмотреть как устроены MinHashLSH и BucketedRandomProjectionLSH в Spark
2. Унаследоваться от LSHModel и LSH
3. Определить недостающие методы для модели (hashFunction, hashDistance keyDistance, write) и для LSH (createRawLSHModel, copy, transformSchema)

Дополнительное задание [30 баллов]
* Сделать предсказания (на тех же данных и фичах: HashingTf-Idf)
* Подобрать количество гиперплоскостей и трешхолд по расстоянию