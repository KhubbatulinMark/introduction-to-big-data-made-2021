# Homework 4

## TF-IDF

По данным [TripAdvisor hotel reviews](https://www.kaggle.com/andrewmvd/trip-advisor-hotel-reviews) посчитать [Tf-Idf](https://ru.wikipedia.org/wiki/TF-IDF) с помощью
Spark DataFrame / Dataset API без использования Spark ML

Этапы:
* Привести все к одному регистру
* Удалить все спецсимволы
* Посчитать частоту слова в предложении
* Посчитать количество документов со словом
* Взять только 100 самых встречаемых
* Сджойнить две полученные таблички и посчитать Tf-Idf (только для слов
из предыдущего пункта)
* Запайвотить табличку

Запуск Jupyter-lab c Spark
```shell
make up
```
[Данные](./data/tripadvisor_hotel_reviews.csv)

[Notebook](https://nbviewer.org/github/KhubbatulinMark/introduction-to-big-data-made-2021/tree/private/homeworks/homework-4/)
