# ETL_Pipeline


## Program Flow

![customers - public erd drawio (2)](https://user-images.githubusercontent.com/22236787/207203428-c4c33ca7-e286-4176-b244-bb49f8ca8abe.png)


extract data from csv and load to data_raw ,then data from data_raw clean data, remove duplicate data, set data type and load to data_clean. and then from data_clean select the required tables and columns and load them into data_mart. 

## Data Raw (Schema : integration)


![warehouse - integration](https://user-images.githubusercontent.com/22236787/207201985-2af3be71-ba4d-4754-b4d2-68f783a16a23.png)


## Data Clean (Schema : public)


![warehouse - public](https://user-images.githubusercontent.com/22236787/207202051-bfde3736-75e9-4cdc-b073-aaebf597e4c6.png)


## Data Mart Customers Orders (Schema : data_mart)


![warehouse - data_mart](https://user-images.githubusercontent.com/22236787/207202161-39aa339a-4023-4bf4-aacc-e7b3fc53fbd3.png)



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

3.Insert data from csv to table raw
```bash
python3 app/insert_data_raw.py
```

4. Alter Columns to add new column in table
```bash
python3 app/alter_table_raw.py
```

5. Create Index and Foreign key on table raw
```bash
python3 app/create_index_tb_raw.py
```

6.Create Table Clean
```bash
python3 app/create_tb_clean.py
```

7.Create Index on table clean
```
python3 app/create_index_tb_clean.py
```

8.Insert data to table clean
```bash
python3 app/Insert_data_clean.py
```

9. Create table Mart
```bash
python3 app/create_tb_mart.py
```

10. Insert data to table mart
```bash
python3 app/Insert_data_mart.py
```


LinkedIn : https://www.linkedin.com/in/tan-anugrah-22a225240/

