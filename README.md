# ETL_Pipeline

## Program Flow

![customers - public erd drawio (1)](https://user-images.githubusercontent.com/22236787/207084807-4df2db8e-6d69-4a20-801f-b2e958fc1613.png)

## Database Raw


## To Run Program
1. Create Schema 
  Create schema integration for table raw , data_mart for table data mart and schema public using for table clean
```sql
CREATE SCHEMA integration;

CREATE SCHEMA data_mart;
```

2. Create Table Raw 
```bash
python3 app/create_tb_raw.py
```

3. Create Index and Foreign key on table raw
```bash
python3 app/create_index_tb_raw.py
```

4.Insert data from csv to table raw
```bash
python3 app/insert_data_raw.py
```

5.Create Table Clean
```bash
python3 app/create_tb_clean.py
```

6.Create Index on table clean
```
python3 app/create_index_tb_clean.py
```

7.Insert data to table clean
```bash
python3 app/Insert_data_clean.py
```

8. Create table Mart
```bash
python3 app/create_tb_mart.py
```

9. Insert data to table mart
```bash
python3 app/Insert_data_mart.py
```



