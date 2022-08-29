import pandas as pd
import numpy as np
import io

class XlsProcessor(object):
    def __init__(self) -> None:
        self.report_columns = [
            "Тип звонка",
            "Клиент",
            "Сотрудник",
            "Должность",
            "Через",
            "Переадресация",
            "Дата",
            "Время",
            "Ожидание",
            "Длительность",
        ]
        self.report_df = None
        self.report_statistic_table = None
        self.retir_lost_clients_table = None
        self.file_date = [0, 0]
    
    def get_file_date(self):
        return self.file_date

    # Установка отчётного файла
    def set_report_file(self, file):
        # Чтение файла .xlsx
        self.report_df = pd.read_excel(file, engine='openpyxl',)

        # Отрезаем ненужные строки, переименовываем столбцы
        columns_df = list(self.report_df.columns)
        cols = {}
        for i in range(len(self.report_columns)):
            cols[columns_df[i]] = self.report_columns[i]
        self.report_df.rename(columns=cols, inplace=True)

        self.file_date = [self.report_df["Клиент"][4], self.report_df["Клиент"][5]]
        self.file_date[0] = self.file_date[0].replace("/", ".", 2)
        self.file_date[1] = self.file_date[1].replace("/", ".", 2)
        self.report_df = self.report_df[10:].reset_index().drop(["index"], axis=1)
        return self.report_df
    
    # Формирование statistic
    def set_statistic_day_table(self, options, report_table=True):
        if report_table or options[0] in ('Общий', 'Магазины'):
            df = self.report_df

            if options[1] in ["МСК", "НН", "КР"]:
                df = df[df["Должность"].str.startswith(options[1])]
                #print(df)

            if options[2] == "Последний День":
                x_day = df["Дата"].iloc[0]
                df = df[df["Дата"] == x_day]

            
            df_uncalled = df[df["Тип звонка"] == "неотвеченный"]
            df_uncalled["uncalled"] = 1
            df_uncalled = df_uncalled[["Клиент", "uncalled"]]
            
            df = pd.merge(df, df_uncalled, on="Клиент", how="left").fillna(0).drop_duplicates()
            
            
            #df["uncalled"] = df[df["Клиент"] == df_uncalled["Клиент"]]
            #df = df_uncalled
            #print(len(df))
            #df = df.merge(df_uncalled[["Клиент", "uncalled"]])
            #df_new = df_new.groupby(["Клиент"]).count()
            #df_new = df_new[df_new["Клиент"] == "79189041166"]
            #print(len(df))
            #return df_new
            
            # Считаем уникальных звонивших... 
            # для этого сохраняем для каждого клиента(номера) на конкретный тип звонка только последний звонок
            
            #if options[3]:
            df["grouped_column"] = list(zip(df["Дата"].astype(str), df["Время"].astype(str), df["Клиент"]))
            #print(len(df))
            df_last_call = df.sort_values("grouped_column", ascending=False).drop_duplicates(subset=["Клиент", "Сотрудник", "Должность"])
            df = df.sort_values("grouped_column", ascending=False).drop_duplicates(subset=["Тип звонка", "Клиент", "Сотрудник", "Должность"])
            
            #print(len(df))
            

            # Сохранённые* (Перезвоненые)
            df_saved = df_last_call[df_last_call["Тип звонка"] == "исходящий"]
            #df_saved = df_saved.append(df[df["Тип звонка"] == "входящий"])
            df_saved = df_saved[df_saved["uncalled"] == 1]\
                .sort_values("Сотрудник")[["Сотрудник", "Должность", "Клиент"]].reset_index().drop(["index"], axis=1)
            df_saved.rename(columns={"Клиент":"Сохранённые*"}, inplace=True)
            df_saved = df_saved.groupby(["Сотрудник", "Должность"]).count().reset_index()

            # Потерянные* (Неперезвоненые)
            df_unsaved = df_last_call[df_last_call["Тип звонка"] == "неотвеченный"]
            #df_unsaved = df_unsaved.append(df[df["Тип звонка"] == "неуспешный исходящий"])
            df_lost_clients = df_unsaved[df_unsaved["uncalled"] == 1]\
                .sort_values("Сотрудник").reset_index().drop(["index"], axis=1)
            df_unsaved = df_lost_clients[["Сотрудник", "Должность", "Клиент"]]
            df_unsaved.rename(columns={"Клиент":"Потерянные*"}, inplace=True)
            
            df_unsaved = df_unsaved.groupby(["Сотрудник", "Должность"]).count().reset_index()

            # cформируем табличку потерянных клиентов    
            df_lost_clients = df_lost_clients.drop([
                "Тип звонка", "Переадресация", "Ожидание", "Длительность", "uncalled", "grouped_column"], axis=1)
            df_lost_clients["Дата"] = df_lost_clients["Дата"].astype(str).str[:10]
            df_lost_clients["Время"] = df_lost_clients["Время"].astype(str).str[11:]
            # Группируем данные и считаем количество уникальных типов звонков
            df = df.groupby(["Сотрудник", "Должность", "Тип звонка"]).count().reset_index()
            #print(df["Клиент"])

            # Получение числа входящих звонков
            df_inp = df[df["Тип звонка"] == "входящий"].sort_values("Сотрудник")[["Сотрудник", "Должность", "Клиент"]].reset_index().drop(["index"], axis=1)
            df_inp.rename(columns={"Клиент":"Входящие"}, inplace=True)

            # Добавление числа неотвеченных звонков
            df_ninp = df[df["Тип звонка"] == "неотвеченный"].sort_values("Сотрудник")[["Сотрудник", "Должность", "Клиент"]].reset_index().drop(["index"], axis=1)
            df_ninp.rename(columns={"Клиент":"Неотвеченные"}, inplace=True)

            df_rez = df_inp.merge(df_ninp, how='outer')

            # Добавляем числа сохранённых и потеряных звонящих (заполнены нулями - данные значения заполняются операторами)
            df_rez[["Сохранённые"]] = 0
            df_rez[["Потерянные"]] = 0
            df_rez[["Разница"]] = 0

            df_rez = df_rez.merge(df_saved, how='outer')
            df_rez = df_rez.merge(df_unsaved, how='outer')

            df_rez = df_rez.fillna(0)

            # Добавляем число исходящих звонков
            df_ninp = df[df["Тип звонка"] == "исходящий"].sort_values("Сотрудник")[["Сотрудник", "Должность", "Клиент"]].reset_index().drop(["index"], axis=1)
            df_ninp.rename(columns={"Клиент":"Исходящие"}, inplace=True)
            df_rez = df_rez.merge(df_ninp, how='outer')
            
            # Добавляем число неуспешных исходящих звонков
            df_ninp = df[df["Тип звонка"] == "неуспешный исходящий"].sort_values("Сотрудник")[["Сотрудник", "Должность", "Клиент"]].reset_index().drop(["index"], axis=1)
            df_ninp.rename(columns={"Клиент":"Неуспешный исходящий"}, inplace=True)
            df_rez = df_rez.merge(df_ninp, how='outer')
            
            # Заполняем пробелы в данных нулями и переводим столбцы в int
            df_rez = df_rez.fillna(0)
                        
            df_rez.loc[-1] = df_rez.sum(numeric_only=True)
            df_rez["Итого"] = df_rez.sum(axis=1)

            df_rez = df_rez.fillna(0)
            cols = [
                "Входящие", 'Неотвеченные', 'Сохранённые*', 'Потерянные*', 'Сохранённые', 'Потерянные', 
                'Разница', 'Исходящие', 'Неуспешный исходящий', 'Итого'
            ]
            for col in cols:
                df_rez[col] = df_rez[col].astype('int64')
            self.report_statistic_table = df_rez
            self.retir_lost_clients_table = df_lost_clients
            
            return self.report_statistic_table, self.retir_lost_clients_table

    
    # Формирование .xlsx файла статистики
    def get_statistic_file(self):
        output = io.BytesIO()
        writer = pd.ExcelWriter(output, engine='xlsxwriter')

        #workbook  = writer.book
        #worksheet = writer.sheets['Sheet1']

        #try:
        pd.io.formats.excel.header_style = {"font": {"bold": True},
                                        "borders": {"top": "thin", "right": "thin", "bottom": "thin", "left": "thin"},
                                        "pattern": {"pattern": "solid", "fore_colour": 26},
                                        "alignment": {"horizontal": "center", "vertical": "top"}}
        self.report_statistic_table.to_excel(writer, sheet_name='Sheet1')
        #finally:
        #    pd.formats.format.header_style = pd.core.format.header_style
        
        writer.save()
        

        return output, "stats.xlsx"
    

    # Формирование .xlsx файла потерянных
    def get_retir_file(self):
        output = io.BytesIO()
        writer = pd.ExcelWriter(output, engine='xlsxwriter')

        #workbook  = writer.book
        #worksheet = writer.sheets['Sheet1']

        #try:
        pd.io.formats.excel.header_style = {"font": {"bold": True},
                                        "borders": {"top": "thin", "right": "thin", "bottom": "thin", "left": "thin"},
                                        "pattern": {"pattern": "solid", "fore_colour": 26},
                                        "alignment": {"horizontal": "center", "vertical": "top"}}
        self.retir_lost_clients_table.to_excel(writer, sheet_name='Sheet1')
        #finally:
        #    pd.formats.format.header_style = pd.core.format.header_style
        
        writer.save()
        

        return output, "retir.xlsx"