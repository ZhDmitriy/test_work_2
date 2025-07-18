""" ETL процесс выгрузки данных из Google таблиц """

import pandas as pd
from typing import NoReturn, Optional, List
import yaml
import dlt 
import psycopg2
from datetime import datetime


class YamlClient:
    """ Клиент для получениях статических данных из yaml файла"""
    link_yaml_file = "/home/dmitriy/Рабочий стол/Brusnika/etl_process/object_report.yaml"
    
    def get_report_id(self) -> str:
        """ Получаем report_id отчета в Гугл таблицах """
        with open(self.link_yaml_file, "r") as yaml_file:
            report_id = yaml.safe_load(yaml_file)
            return report_id['object_report'][0]['report_id'][0] 
            
    def get_object_gid_id(self) -> List[Optional[str]]:
        """ Получаем все листы, которые есть в отчете report_id в Гугл таблицах """
        with open(self.link_yaml_file, "r") as yaml_file:
            object_gid_id = yaml.safe_load(yaml_file)
            return object_gid_id['object_report'][1]['object_gids'] 


class GoogleSheetClientETL:
    """ Клиент для взаимодействия с Гугл таблицами """
    
    def __init__(self, report_id: str, object_gid: str):
        self.BASE_URL = f"https://docs.google.com/spreadsheets/d/{report_id}/export?format=csv&gid={object_gid}"
        self.object_gid = object_gid
        
    def extract_google_sheet_data(self) -> pd.DataFrame:
        """ Выгрузка данных с листа Гугл таблиц """
        data_report_object = pd.read_csv(self.BASE_URL)
        return data_report_object
    
    def transform_google_sheet_data(self, data_report_object: pd.DataFrame) -> pd.DataFrame:
        """ Структуризация и нормализация данных данных
            data_report_object: сырой отчет из Гугл таблиц
        """
        # удаляем ненужные строки
        data = data_report_object.drop([0, 1, 2, 4, 5, 6]).reset_index()

        # переименовываем колонки
        new_columns = data.iloc[0].values
        data.columns = new_columns
        data = data[1:].reset_index(drop=True)

        # добавляем для первых двух строк наименование колонок
        data.columns.values[1] = "Вид работ"
        data.columns.values[2] = "Подрядчик"

        # удаляем ненужные строки и колонки после всех преобразований 
        data = data.drop([0]).reset_index()
        data = data.drop(data.columns[[0, 1]], axis=1)

        # размечаем столбцы, которые содержат плановые показатели
        columns = data.columns.tolist()
        for item_column in range(len(columns)-1):
            if columns[item_column] == columns[item_column + 1]:
                columns[item_column] = columns[item_column] + "_plan"
        data.columns = columns

        # заполняем пропуски в видах работ и подрядчиках
        data['Вид работ'] = data['Вид работ'].ffill()
        data['Подрядчик'] = data['Подрядчик'].ffill()

        # удаляем строку с агрегатными значениями по колонке
        data_transform = data.drop([0]).reset_index(drop=True)

        # преобразовываем данные к формату удобную субд
        data_transposed = data_transform.set_index(['Вид работ', 'Подрядчик'])\
                                .stack()\
                                .reset_index()\
                                .rename(columns={'level_2': 'Дата', 0: 'Значение'})

        # определяем тип даты 
        def process_row(row):
            if '_plan' in str(row['Дата']):
                row['Статус'] = 'План'
                row['Дата'] = row['Дата'].replace('_plan', '')
            else:
                row['Статус'] = ''
            return row

        data_transposed = data_transposed.apply(process_row, axis=1)
        data_transposed['Id объекта'] = self.object_gid
        data_transposed['Дата'] = data_transposed['Дата'].apply(lambda data: datetime.strptime(data, "%d.%m.%Y").date().strftime("%Y-%m-%d"))
        
        data_transposed = data_transposed.rename(columns={
            "Вид работ": "TypeWork", 
            "Подрядчик": "Contractor", 
            "Дата": "Date", 
            "Статус": "Status", 
            "Значение": "Value",
            "Id объекта": "ObjectID"})
        
        return data_transposed
    
    def load_to_postgres_google_sheet_data(self, data_transform: pd.DataFrame) -> NoReturn:
        """ Загрузка данных в базу данных
            data_transform: преобразованный отчет из гугл таблиц
            object_number: номер объекта 
        """
        
        @dlt.resource(
            table_name=f"fact_plan_human_management_object", 
            write_disposition={"disposition": "append"},
            schema_contract={"tables": "evolve", "columns": "evolve", "data_type": "evolve"}
        )
        def get_report_google_sheet_object():
            for report_row in data_transform.itertuples(index=False):
                yield report_row._asdict()
                
        @dlt.source
        def object_source():
            return get_report_google_sheet_object
        
        pipeline = dlt.pipeline(
            pipeline_name="human_management_object",
            destination="postgres", 
            dataset_name="hm", 
            dev_mode=False
        )
        
        pipeline.run(
          object_source(),
          credentials="postgresql://loader:dlt@localhost:5432/dlt_data" # в идеале вынести в переменную среды
        )
        
        
def etl_process() -> NoReturn:
    
    yaml_client = YamlClient()
    
    # Индентификатор отчета в гугл таблицах
    report_id = yaml_client.get_report_id()
    
    # Объекты - листы гугл таблицы
    object_1_gid = yaml_client.get_object_gid_id()[0]
    object_2_gid = yaml_client.get_object_gid_id()[1]
    
    # Реализация ETL для объекта 1 из Гугл таблиц
    gs_client_object_1 = GoogleSheetClientETL(report_id=report_id, object_gid=object_1_gid)
    data_report_object = gs_client_object_1.extract_google_sheet_data() # Выгрузка данных
    data_transform = gs_client_object_1.transform_google_sheet_data(data_report_object=data_report_object) # Преобразование данных
    gs_client_object_1.load_to_postgres_google_sheet_data(data_transform=data_transform) # Загрузка данных в бд
    
    # Реализация ETL для объекта 2 из Гугл таблиц
    gs_client_object_2 = GoogleSheetClientETL(report_id=report_id, object_gid=object_2_gid)
    data_report_object = gs_client_object_2.extract_google_sheet_data() # Выгрузка данных
    data_transform = gs_client_object_2.transform_google_sheet_data(data_report_object=data_report_object) # Преобразование данных
    gs_client_object_2.load_to_postgres_google_sheet_data(data_transform=data_transform) # Загрузка данных в бд
        
    
if __name__ == '__main__':
    etl_process()


    
    
    