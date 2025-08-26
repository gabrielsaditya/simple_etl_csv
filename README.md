# simple_etl_csv
Project sederhana trial untuk mengenal airflow dan urutan pengerjaan di data engineer gambarannya seperti apa

1. Ambil data dari API spacex public
2. extract data dari api ke dataframe
3. transform data, seleksi data yang ingin dimasukkan ke csv lalu dikasih datetime
4. load data ke csv dengan folder output ./dags/output
