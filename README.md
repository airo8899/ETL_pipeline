# ETL_pipeline

ETL-пайплайн извлекает данные из БД сервисов ленты новостей и мессенджера за вчерашний день. Далее объединяет данные в одну таблицу. 

Рассчитывается общая сводная таблица по полу, возрастной категории и операционной системе и записывается в таблицу test.diykov_v1.

Также рассчитывается отдельно сводные таблицы по полу, возрастной категории и операционной системе, затем таблицы объединяются, и финальные данные записываются в таблицу test.diykov_v2.

