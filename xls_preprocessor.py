import datetime
import json
import pandas as pd
import numpy as np
import io

# класс работающий с .xlsx отчётом
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

        self.table_formats = [
            'Общий',
            'Менеджеры',
            'Магазины'
        ]
        
        self.table_filials = [
            'Все',
            'НН',
            'КР',
            'МСК',
            'Оператор'
        ]
        
        self.table_times = [
            'Статистика',
            'Статистика за последний день',
            'Два дня',
            'Четыре дня'
        ]

        self.start_params = {
            "actors": 'Общий',
            "filial": 'Все',
            "report_mode": 'Статистика',
            "is_unique_users": 1,
            "date_limits": [0, 0],
        }

        self.table_params = {
            "actors": 'Общий',
            "filial": 'Все',
            "report_mode": 'Статистика',
            "is_unique_users": 1,
            "date_limits": [0, 0],
        }

        self.filename = ""
        self.file_or_api = None
        self.report_df = 0
        self.report_df_in_time = 0

        self.report_statistic_table = 0
        self.retir_lost_clients_table = 0
        self.add_df_more_clients = 0
        self.file_times = 0
        self.unique_clients = 0

        self.is_df_taken = False
    
    def get_file_date(self):
        #print(type(self.file_times[0]))
        #print(isinstance(min(self.file_times), (np.datetime64)))
        
        if isinstance(min(self.file_times), (np.datetime64)):
            file_maxmin_dates = [str(min(self.file_times))[:10], str(max(self.file_times))[:10]]
        else:
            file_maxmin_dates = [min(self.file_times).strftime('%Y-%m-%d'), max(self.file_times).strftime('%Y-%m-%d')]
        
        #print("times: ", file_maxmin_dates)
        return file_maxmin_dates

    def get_statistic_table(self):
        #print(self.report_statistic_table)
        return self.report_statistic_table[["Сотрудник", "Должность", "Входящие", "Неотвеченные", "Исходящие", "Неуспешный исходящие", "Загрузка менеджера по звонкам", "Уникальные", "Подробнее"]]

    def get_retir_table(self):
        #print(self.retir_lost_clients_table)
        with open('config-file.json', 'r') as json_data:
                config = json.load(json_data)
                json_data.close()

        try:
            ret_retir_lost_clients_table = self.retir_lost_clients_table.copy()
            #print(self.retir_lost_clients_table)
            ret_retir_lost_clients_table["Клиент"] = "<a href=\"" + \
                str(config["domain"]) + "#/history?type=external&direction=total&period=this_month&notes=true&searchQuery=" + ret_retir_lost_clients_table["Клиент"].astype("string") + \
                "\" target=\"_blank\">" + ret_retir_lost_clients_table["Клиент"].astype("string") + "</a>"
            #print(self.retir_lost_clients_table)
            return ret_retir_lost_clients_table
        except:
            return self.retir_lost_clients_table

    def get_add_table(self):
        with open('config-file.json', 'r') as json_data:
                config = json.load(json_data)
                json_data.close()

        try:
            ret_add_df_more_clients = self.add_df_more_clients.copy()
            ret_add_df_more_clients["Клиент"] = "<a href=\"" + \
                str(config["domain"]) + "#/history?type=external&direction=total&period=this_month&notes=true&searchQuery=" + ret_add_df_more_clients["Клиент"].astype("string") + \
                "\" target=\"_blank\">" + ret_add_df_more_clients["Клиент"].astype("string") + "</a>"
            return ret_add_df_more_clients
        except:
            return self.add_df_more_clients

    def get_table_filters(self, params):
        return self.table_formats, self.table_filials, self.table_times

    # Установка отчётного файла
    def set_report_file(self, is_file, file):
        ###
        # Определяем источника файла и получаем его
        ###

        if is_file == True:
            self.file_or_api = "file"
        else:
            self.file_or_api = "api"
        print("self.file_or_api: ", self.file_or_api)
        
        # Чтение файла .xlsx
        if self.file_or_api == "file":
            
            self.report_df = pd.read_excel(file, engine='openpyxl',)
            self.report_df = self.report_df[10:].reset_index().drop(["index"], axis=1)
            #print(self.report_df)
            
            # Отрезаем ненужные строки, переименовываем столбцы
            columns_df = list(self.report_df.columns)
            cols = {}
            for i in range(len(self.report_columns)):
                cols[columns_df[i]] = self.report_columns[i]
            self.report_df.rename(columns=cols, inplace=True)
        
        # Чтение данных из АТС
        else:
            import requests
            from io import BytesIO
            
            with open('config-file.json', 'r') as json_data:
                config = json.load(json_data)
                json_data.close()

            staff = requests.get(config["domain"] + '/crmapi/v1/users', headers=config["headers"])
            df_staff = pd.json_normalize(staff.json()["items"])
            #print(df_staff)

            today_str = "{:%Y%m%dT%H%M%SZ}".format(datetime.datetime.now())
            #print(today_str)
            params = {
                'start': '20220510T000000Z',
                'end': today_str #'20220910T000000Z'
            }

            history = requests.get(config["domain"] + '/crmapi/v1/history/csv', headers=config["headers"], params=params)

            df_history = pd.read_csv(BytesIO(history.content))
            df_history.columns = ['uid', 'type', 'client', 'account', 'via', 'start', 'wait', 'duration', 'record']
            df_history["account"] = df_history["account"].str[:-22]
            df_history = df_history.rename(columns={"account": "login"})

            df = df_history.merge(df_staff[["login", "position", "name"]])
            df.rename(columns={
                'type': 'Тип звонка',
                'login': 'Аккаунт',
                'via': 'Через',
                'client': 'Клиент',
                'start': 'Время звонка',
                'wait': 'Ожидание',
                'duration': 'Длительность',
                'record': 'Запись разговора',
                'position': 'Должность',
                'name': 'Сотрудник',
                }, inplace=True)

            df["Дата"] = df["Время звонка"].str[:10] + " 00:00:00"
            df["Дата"] = pd.to_datetime(df["Дата"], format="%Y-%m-%d %H:%M:%S")
            #print(df["Клиент"].value_counts())
            try:
                df["Клиент"] = df["Клиент"].astype('Int64')
            except:
                pass
            
            df["Через"] = df["Через"].astype('Int64')

            df["Время"] = pd.to_datetime(df["Время звонка"].str[11:], format='%H:%M:%SZ').dt.time


            df = df[["Аккаунт", "Тип звонка", "Клиент", "Сотрудник", "Должность", "Через", "Время звонка", "Дата", "Время", "Ожидание", "Длительность", "Запись разговора"]]
            df["Ожидание"] = pd.to_datetime(df["Ожидание"].astype(str), unit='s').dt.time
            df["Длительность"] = pd.to_datetime(df["Длительность"].astype(str), unit='s').dt.time

            df["Запись разговора"] = df["Запись разговора"].fillna("-")
            ##
            # Тут бы их отсортировать конечно
            ##

            df_missed = df[df["Тип звонка"] == 'missed']
            df_in = df[df["Тип звонка"] == 'in']
            df_out = df[df["Тип звонка"] == 'out']

            df_in_good = df_in[df_in["Запись разговора"] != "-"]
            df_out_good = df_out[df_out["Запись разговора"] != "-"]

            df_in_bad = df_missed.append(df_in[df_in["Запись разговора"] == "-"])
            df_out_bad = df_out[df_out["Запись разговора"] == "-"]

            df_in_good.loc[:, "Тип звонка"] = "входящий"
            df_out_good.loc[:, "Тип звонка"] = "исходящий"

            df_in_bad.loc[:, "Тип звонка"] = "неотвеченный"
            df_out_bad.loc[:, "Тип звонка"] = "неуспешный исходящий"

            df_rez_1 = df_in_good.append(df_in_bad)
            df_rez_2 = df_rez_1.append(df_out_good)
            df_rez = df_rez_2.append(df_out_bad)

            self.report_df = df_rez


        # Достаём временные промежутки
        self.file_times = self.report_df["Дата"].unique()
        self.is_df_taken = True
        return self.report_df
    
    

    # Формирование statistic
    def set_statistic_day_table(self, params, base_url):
        print(params)

        if params != self.start_params:
            self.table_params = params

        if self.is_df_taken:
            df_old = self.report_df

            df_old["Сотрудник"] = df_old["Сотрудник"].astype(str)
            df_old = df_old[df_old['Должность'].apply(lambda x: isinstance(x, (str)))]
            # Выделяем филиал
            if self.table_params["filial"] in ["МСК", "НН", "КР"]:
                df_old = df_old[df_old["Должность"].str.startswith(self.table_params["filial"], na= False)]
            elif self.table_params["filial"] in ["Оператор"]:
                df_old = df_old[df_old["Должность"].str.contains("Оператор")]

            # Промежуток времени
            forms_times = self.table_params["date_limits"]
            if self.table_params["date_limits"] != [0,0]:
                forms_times = [datetime.datetime.strptime(forms_times[0], "%Y-%m-%d"), datetime.datetime.strptime(forms_times[1], "%Y-%m-%d")]
                df_old = df_old[df_old["Дата"] <= forms_times[1]]
                df_old = df_old[df_old["Дата"] >= forms_times[0]]

            # Выбираем режим отчёта
            days=0
            df_super_old = df_old
            if self.table_params["report_mode"] == "Статистика за последний день":
                days = df_old["Дата"].iloc[0]
                df_old = df_old[df_old["Дата"] == days]

            elif self.table_params["report_mode"] == "Два дня":
                try:
                    days = sorted(df_old["Дата"].unique(), reverse=True)[:2]
                    df_0 = df_old[df_old["Дата"] == days[0]]
                    df_old = df_0.append(df_old[df_old["Дата"] == days[1]])
                except:
                    return 0

            elif self.table_params["report_mode"] == "Четыре дня":
                try:
                    days = sorted(df_old["Дата"].unique(), reverse=True)[:4]
                    df_0 = df_old[df_old["Дата"] == days[0]]
                    df_1 = df_0.append(df_old[df_old["Дата"] == days[1]])
                    df_2 = df_1.append(df_old[df_old["Дата"] == days[2]])
                    df_old = df_2.append(df_old[df_old["Дата"] == days[3]])
                except:
                    return 0
            
            self.report_df_in_time = df_old

            
            # Сортировка по времени + клиенту
            df_old.loc[:, "grouped_column"] = list(zip(df_old["Дата"].astype(str), df_old["Время"].astype(str), df_old["Клиент"]))

            ###
            # РУЧНОЕ ФОРМАТИРОВАНИЕ НАЗВАНИЙ ТАБЛИЦЫ
            ###
            # Магазины
            df_old.loc[df_old["Должность"] == "НН DAF new старый", "Должность"] = "НН Авито DAF"
            df_old.loc[df_old["Должность"] == "НН Авито DAF", "Должность"] = "НН Авито DAF"
            
            df_old.loc[df_old["Должность"] == "НН Kamaz Mercedes / DROM", "Должность"] = "НН Kamaz Mercedes DROM"
            df_old.loc[df_old["Должность"] == "НН Kamaz Mercedes DROM", "Должность"] = "НН Kamaz Mercedes DROM"
            
            ###
            # Менеджеры
            df_old.loc[df_old["Сотрудник"] == "Андрей GO DAF new", "Сотрудник"] = "Андрей GO"
            df_old.loc[df_old["Сотрудник"] == "Андрей KR АвитоЛюкс", "Сотрудник"] = "Андрей KR"
            df_old.loc[df_old["Сотрудник"] == "Андрей Новый Iveco", "Сотрудник"] = "Андрей Новый"
            df_old.loc[df_old["Сотрудник"] == "Андрей Новый Volvo", "Сотрудник"] = "Андрей Новый"
            df_old.loc[df_old["Сотрудник"] == "Андрей Новый Сайт", "Сотрудник"] = "Андрей Новый"
            df_old.loc[df_old["Сотрудник"] == "Виктор NN", "Сотрудник"] = "Виктор"
            df_old.loc[df_old["Сотрудник"] == "Виктор Sale KZN", "Сотрудник"] = "Виктор"
            df_old.loc[df_old["Сотрудник"] == "Виктор Екатеринбург", "Сотрудник"] = "Виктор"
            df_old.loc[df_old["Сотрудник"] == "Виктор Уфа", "Сотрудник"] = "Виктор"
            df_old.loc[df_old["Сотрудник"] == "Виктор Челябинск", "Сотрудник"] = "Виктор"
            df_old.loc[df_old["Сотрудник"] == "Давид KR Ростов", "Сотрудник"] = "Давид KR"
            df_old.loc[df_old["Сотрудник"] == "Евгений Scania MAN", "Сотрудник"] = "Евгений Сергеевич"
            df_old.loc[df_old["Сотрудник"] == "Евгений МСК", "Сотрудник"] = "Евгений Михайлович"

            df_old.loc[df_old["Сотрудник"] == "Евгений С", "Сотрудник"] = "Евгений Сергеевич"
            df_old.loc[df_old["Сотрудник"] == "Егор Kamaz", "Сотрудник"] = "Егор"
            df_old.loc[df_old["Сотрудник"] == "Михаил KR Ставрополь", "Сотрудник"] = "Михаил KR"

            ###
            # РУЧНОЕ УДАЛЕНИЕ СОТРУДНИКОВ ИЗ АНАЛИЗА ЗВОНКОВ
            ###
            staff = [
                # НН
                "79108872171", "79991417129", "79200692035", "79991200121", 
                "79991372707", "79101276869", "79092967666",
                # КР
                "79883388839", "79186620272", "79615085433", "79897626599", 
                "79898222355", "79604774244", "79951116699", "79528117194", 
                # МСК
                "79836900708", "79290508906", "79290424046", 
                # Беларусь
                "79966285887"
            ]

            df_old = df_old[~df_old["Клиент"].isin(staff)]
            df_old = df_old.sort_values("grouped_column", ascending=False)
            df_old["Клиент_len"] = df_old["Клиент"].astype("string").str.len()
            df_old = df_old[df_old["Клиент_len"] == 11].drop(["Клиент_len"], axis=1)
            
            
            ###
            # Начинается распределение в зависимости от режима отчёта
            ###
            df_super_old = df_old
            self.unique_clients = df_old[["Клиент"]].value_counts().reset_index() #df_old.drop_duplicates(subset=["Клиент"], keep='first')
            self.unique_clients.rename(columns={0: "Количество звонков"}, inplace=True)
            
            #df_unique = pd.pivot_table(df_old, values="Клиент", index=['Сотрудник', 'Должность'],
            #        columns=['Тип звонка'], aggfunc=np.sum).reset_index()
            #print(df_unique)

            if self.table_params["is_unique_users"]:
                df_old_unique = df_old.sort_values("grouped_column", ascending=False).drop_duplicates(subset=["Тип звонка", "Клиент", "Сотрудник", "Должность"], keep='first')
            else:
                df_old_unique = df_old
            

            # Группируем данные и считаем количество уникальных типов звонков
            df = df_old_unique.groupby(["Сотрудник", "Должность", "Тип звонка"]).count().reset_index()
            #print(df)
            
            # Уникальные клиенты в принципе
            
            df_rez = pd.pivot_table(df, values="Клиент", index=['Сотрудник', 'Должность'],
                    columns=['Тип звонка'], aggfunc=np.sum).reset_index()#.drop(["Тип звонка"], axis=1)
            df_rez.rename(columns={
                "входящий":"Входящие",
                "исходящий":"Исходящие",
                "неотвеченный":"Неотвеченные",
                "неуспешный исходящий":"Неуспешный исходящие",}, inplace=True)
            
            df_rez_keys = ["Входящие", "Неотвеченные", "Исходящие", "Неуспешный исходящие"]
            for key in df_rez_keys:
                if not key in df_rez.columns:
                    df_rez[key] = 0
            df_unique_all = df_old.drop_duplicates(subset=["Клиент", "Сотрудник", "Должность"], keep='first')

            df_u_all = df_unique_all.groupby(["Сотрудник", "Должность"]).count().reset_index()
            df_u_all.rename(columns={"Тип звонка":"Уникальные"}, inplace=True)
            
            # Вычисляем людей, у которых были неотвеченные звонки
            df_uncalled = df_old[df_old["Тип звонка"] == "неотвеченный"]
            df_uncalled.loc[:, "uncalled"] = 1
            df_uncalled = df_uncalled[["Клиент", "uncalled"]]
            
            df_old = pd.merge(df_old, df_uncalled, on="Клиент", how="left").fillna(0).drop_duplicates(keep='first')
            df_last_call = df_old.sort_values("grouped_column", ascending=False).drop_duplicates(subset=["Клиент", "Сотрудник", "Должность"], keep='first')
            
            # Потерянные* (Неперезвоненые)
            df_unsaved = df_last_call[df_last_call["Тип звонка"] == "неотвеченный"]
            #df_unsaved = df_unsaved.append(df[df["Тип звонка"] == "неуспешный исходящий"])
            df_lost_clients = df_unsaved[df_unsaved["uncalled"] == 1]
            df_lost_clients = df_lost_clients.sort_values("Сотрудник").reset_index().drop(["index"], axis=1)

            cols = [
                "Входящие", 'Неотвеченные', 'Исходящие', 'Неуспешный исходящие', 'Загрузка менеджера по звонкам', 'Уникальные'
            ]
            
            # cформируем табличку потерянных клиентов
            df_lost_clients, df_more_clients = self.set_lost_clients_table(df=df_super_old, days=days, option=params["report_mode"], df_lost_clients=df_lost_clients)
            
            # Заполняем пробелы в данных нулями и переводим столбцы в int
            df_rez = df_rez.fillna(0)
            
            if not self.table_params["filial"] in ["Оператор"]:
                #print("Операторы не выбраны")
                df_u_all = df_u_all[df_u_all["Должность"] != "НН Оператор"]
                df_u_all = df_u_all[df_u_all["Должность"] != "МСК Оператор"]
                df_rez = df_rez[df_rez["Должность"] != "НН Оператор"]
                df_rez = df_rez[df_rez["Должность"] != "МСК Оператор"]
                #print(df_rez)
            
            df_rez = df_rez.groupby(["Сотрудник", "Должность"]).sum().reset_index()
            #print(df_rez)

            if self.table_params["actors"] == "Общий":
                df_rez = df_rez
            elif self.table_params["actors"] == "Менеджеры":
                df_rez = df_rez.groupby(["Сотрудник"]).sum().reset_index()
            elif self.table_params["actors"] == "Магазины":
                df_rez = df_rez.groupby(["Должность"]).sum().reset_index()
                

            df_rez["Загрузка менеджера по звонкам"] = df_rez.sum(axis=1, numeric_only=True)
            df_rez = df_rez.merge(df_u_all[["Сотрудник", "Должность", "Уникальные"]], how='left')
            
            df_rez["Подробнее"] = "<a href=\"" + base_url.split('/')[0] + "/current_table_from_ats/manager=" + df_rez["Сотрудник"] + "shop=" + df_rez["Должность"] + "\">Подробнее..</a>"
            

            df_rez = df_rez.fillna(0)
            df_rez.loc["Итого"] = df_rez.sum(numeric_only=True)
            df_rez = df_rez.fillna("-")
            
            for col in cols:
                df_rez[col] = df_rez[col].astype('int64')
            #print(df_rez["Подробнее"])
            self.report_statistic_table = df_rez
            #print(df_rez)

            self.retir_lost_clients_table = df_lost_clients
            self.add_df_more_clients = df_more_clients

            if self.table_params["report_mode"] == "Четыре дня":
                return 2
            else:
                return 1
        return 0
    
    # Табличка потерянных клиентов
    def set_lost_clients_table(self, df, days, option, df_lost_clients):
        df = df
        df_last_call = df.drop_duplicates(subset=["Клиент"], keep='first')
        if len(df_lost_clients):
            if option in ["Статистика", "Статистика за последний день"]:
                #print(df_lost_clients)
                df_lost_clients = df_lost_clients[["Клиент", "Сотрудник", "Должность", "Через", "Дата", "Время"]]
                return df_lost_clients, None
            
            elif option == "Два дня":
                ## Первый день
                df_first = df[df["Дата"] == days[1]]
                df_first_last_call = df_first.drop_duplicates(subset=["Клиент"], keep='first')
                df_first_uncall = df_first_last_call[df_first_last_call["Тип звонка"] == "неотвеченный"]\
                    .append(df_first_last_call[df_first_last_call["Тип звонка"] == "неуспешный исходящий"])
                #df_first_uncall.loc[:, "Пропущеный вчера"] = "Да"
                if len(df_first_uncall):
                    df_first_uncall.loc[:, "Пропущеный вчера"] = "Да"
                else:
                    df_first_uncall = df_first_uncall.join(pd.DataFrame(columns=['Пропущеный вчера']))

                df_first_uncall = df_first_uncall[["Клиент", "Пропущеный вчера"]]

                ## Второй день
                df_second = df[df["Дата"] == days[0]]
                
                # Перезвон по первому дню
                df_second_called = df_second[df_second["Тип звонка"] == "входящий"]
                df_second_called = df_second_called.append(df_second[df_second["Тип звонка"] == "исходящий"])
                df_second_called = df_second_called.sort_values("grouped_column", ascending=False)\
                    .drop_duplicates(subset=["Клиент"], keep='first').sort_values("grouped_column", ascending=False)
                #df_second_called.loc[:, "Менеджер перезвонил"] = "Да"
                if len(df_second_called):
                    df_second_called.loc[:, "Менеджер перезвонил"] = "Да"
                else:
                    df_second_called = df_second_called.join(pd.DataFrame(columns=['Менеджер перезвонил']))

                df_second_called = df_second_called[["Клиент", "Менеджер перезвонил"]]
                df_first_to_second_called = df_first_uncall.merge(df_second_called, how="left")

                
                df_second_last_call = df_second.drop_duplicates(subset=["Клиент"], keep='first')
                df_second_uncall = df_second_last_call[df_second_last_call["Тип звонка"] == "неотвеченный"]
                #df_second_uncall.loc[:, "Пропущеный сегодня"] = "Да"
                if len(df_second_uncall):
                    df_second_uncall.loc[:, "Пропущеный сегодня"] = "Да"
                else:
                    df_second_uncall = df_second_uncall.join(pd.DataFrame(columns=["Пропущеный сегодня"]))
                df_second_uncall = df_second_uncall[["Клиент", "Пропущеный сегодня"]]

                # Обьединение данных за дни
                df_full = df_first_to_second_called.merge(df_second_uncall, how='outer').fillna("-")
                df_full_rez = df_full.merge(df_last_call[["Клиент", "Сотрудник", "Должность"]], how="left")

                return df_full_rez[["Клиент", "Сотрудник", "Должность", "Пропущеный вчера", "Менеджер перезвонил", "Пропущеный сегодня"]], None
            
            elif option == "Четыре дня":
                ## Первый день
                #print(days)
                df_first = df[df["Дата"] == days[3]].sort_values("grouped_column", ascending=False)
                df_first_last_call = df_first.drop_duplicates(subset=["Клиент"], keep='first')
                df_first_uncall = df_first_last_call[df_first_last_call["Тип звонка"] == "неотвеченный"]\
                    .append(df_first_last_call[df_first_last_call["Тип звонка"] == "неуспешный исходящий"])

                #df_first_uncall.loc[:, "Пропущеный 1"] = "Да"
                if len(df_first_uncall):
                    df_first_uncall.loc[:, "Пропущеный 1"] = "Да"
                else:
                    df_first_uncall = df_first_uncall.join(pd.DataFrame(columns=["Пропущеный 1"]))
                    
                df_first_uncall = df_first_uncall[["Клиент", "Пропущеный 1"]]

                ## Второй день
                df_second = df[df["Дата"] == days[2]].sort_values("grouped_column", ascending=False)
                
                # Перезвон по первому дню
                df_second_called = df_second[df_second["Тип звонка"] == "входящий"]
                df_second_called = df_second_called.append(df_second[df_second["Тип звонка"] == "исходящий"])
                df_second_called = df_second_called.sort_values("grouped_column", ascending=False)\
                    .drop_duplicates(subset=["Клиент"], keep='first').sort_values("grouped_column", ascending=False)
                if len(df_second_called):
                    df_second_called.loc[:, "Менеджер перезвонил 2"] = "Да"
                else:
                    df_second_called = df_second_called.join(pd.DataFrame(columns=['Менеджер перезвонил 2']))

                df_second_called = df_second_called[["Клиент", "Менеджер перезвонил 2"]]
                df_first_to_second_called = df_first_uncall.merge(df_second_called, how="left")
                
                df_second_last_call = df_second.drop_duplicates(subset=["Клиент"], keep='first')
                df_second_uncall = df_second_last_call[df_second_last_call["Тип звонка"] == "неотвеченный"]
                #df_second_uncall.loc[:, "Пропущеный 2"] = "Да"
                if len(df_second_uncall):
                    df_second_uncall.loc[:, "Пропущеный 2"] = "Да"
                else:
                    df_second_uncall = df_second_uncall.join(pd.DataFrame(columns=["Пропущеный 2"]))

                df_second_uncall = df_second_uncall[["Клиент", "Пропущеный 2"]]

                # Обьединение данных за дни
                df_full_0 = df_first_to_second_called.merge(df_second_uncall, how='outer').fillna("-")

                ## Третий день
                df_third = df[df["Дата"] == days[1]].sort_values("grouped_column", ascending=False)
                
                # Перезвон по третьему дню
                df_third_called = df_third[df_third["Тип звонка"] == "входящий"]
                df_third_called = df_third_called.append(df_third[df_third["Тип звонка"] == "исходящий"])
                df_third_called = df_third_called.sort_values("grouped_column", ascending=False)\
                    .drop_duplicates(subset=["Клиент"], keep='first').sort_values("grouped_column", ascending=False)
                #print(df_third_called)
                if len(df_third_called):
                    df_third_called.loc[:, "Менеджер перезвонил 3"] = "Да"
                else:
                    df_third_called = df_third_called.join(pd.DataFrame(columns=['Менеджер перезвонил 3']))

                df_third_called = df_third_called[["Клиент", "Менеджер перезвонил 3"]]
                ##  ОБЬЕДИНИТЬ ДАННЫЕ ЗА 3 ДЕНЬ С ПЕРВЫМИ ДВУМЯ
                df_full_1 = df_full_0.merge(df_third_called, how="left")
                #df_second_to_third_called = df_second_uncall.merge(df_third_called, how="left")

                df_third_last_call = df_third.drop_duplicates(subset=["Клиент"], keep='first')
                df_third_uncall = df_third_last_call[df_third_last_call["Тип звонка"] == "неотвеченный"]
                if len(df_third_uncall):
                    df_third_uncall.loc[:, "Пропущеный 3"] = "Да"
                else:
                    df_third_uncall = df_third_uncall.join(pd.DataFrame(columns=["Пропущеный 3"]))

                df_third_uncall = df_third_uncall[["Клиент", "Пропущеный 3"]]

                # Обьединение данных за дни
                df_full_2 = df_full_1.merge(df_third_uncall, how='outer').fillna("-")

                # Четвёртый день
                df_fourth = df[df["Дата"] == days[0]].sort_values("grouped_column", ascending=False)
                
                # Перезвон по четвёртому дню
                df_fourth_called = df_fourth[df_fourth["Тип звонка"] == "входящий"]
                df_fourth_called = df_fourth_called.append(df_fourth[df_fourth["Тип звонка"] == "исходящий"])
                df_fourth_called = df_fourth_called.sort_values("grouped_column", ascending=False)\
                    .drop_duplicates(subset=["Клиент"], keep='first').sort_values("grouped_column", ascending=False)
                #df_fourth_called.loc[:, "Менеджер перезвонил 4"] = "Да"
                if len(df_fourth_called):
                    df_fourth_called.loc[:, "Менеджер перезвонил 4"] = "Да"
                else:
                    df_fourth_called = df_fourth_called.join(pd.DataFrame(columns=["Менеджер перезвонил 4"]))

                df_fourth_called = df_fourth_called[["Клиент", "Менеджер перезвонил 4"]]
                ##  ОБЬЕДИНИТЬ ДАННЫЕ ЗА 3 ДЕНЬ С ПЕРВЫМИ ДВУМЯ
                df_full_3 = df_full_2.merge(df_fourth_called, how="left")
                #df_second_to_third_called = df_second_uncall.merge(df_third_called, how="left")

                df_fourth_last_call = df_fourth.drop_duplicates(subset=["Клиент"], keep='first')
                df_fourth_uncall = df_fourth_last_call[df_fourth_last_call["Тип звонка"] == "неотвеченный"]
                #df_fourth_uncall.loc[:, "Пропущеный 4"] = "Да"
                if len(df_fourth_uncall):
                    df_fourth_uncall.loc[:, "Пропущеный 4"] = "Да"
                else:
                    df_fourth_uncall = df_fourth_uncall.join(pd.DataFrame(columns=["Пропущеный 4"]))

                df_fourth_uncall = df_fourth_uncall[["Клиент", "Пропущеный 4"]]

                # Обьединение данных за дни
                df_full = df_full_3.merge(df_fourth_uncall, how='outer').fillna("-")
                df_full["Статус"] = "Пропущенный Понедельник"

                df_retir = df_full[df_full["Пропущеный 4"] == "Да"]
                df_retir["Статус"] = "Пропущенный Понедельник"
                df_full = df_full.loc[df_full["Пропущеный 4"] != "Да"]

                df_maybe = df_full[df_full["Менеджер перезвонил 4"] == "Да"]
                df_maybe["Статус"] = "Перезвонили Понедельник"
                df_full = df_full.loc[df_full["Менеджер перезвонил 4"] != "Да"]

                df_retir_slise = df_full[df_full["Пропущеный 3"] == "Да"]
                df_retir_slise["Статус"] = "Пропущенный Воскресенье"
                df_retir = df_retir.append(df_retir_slise)
                df_full = df_full.loc[df_full["Пропущеный 3"] != "Да"]
                
                df_maybe_slise = df_full[df_full["Менеджер перезвонил 3"] == "Да"]
                df_maybe_slise["Статус"] = "Перезвонили Воскресенье"
                df_maybe = df_maybe.append(df_maybe_slise)
                df_full = df_full.loc[df_full["Менеджер перезвонил 3"] != "Да"]

                df_retir_slise = df_full[df_full["Пропущеный 2"] == "Да"]
                df_retir_slise["Статус"] = "Пропущенный Суббота"
                df_retir = df_retir.append(df_retir_slise)
                df_full = df_full.loc[df_full["Пропущеный 2"] != "Да"]
                
                df_maybe_slise = df_full[df_full["Менеджер перезвонил 2"] == "Да"]
                df_maybe_slise["Статус"] = "Перезвонили Суббота"
                df_maybe = df_maybe.append(df_maybe_slise)
                df_full = df_full.loc[df_full["Менеджер перезвонил 2"] != "Да"]

                df_retir_slise = df_full[df_full["Пропущеный 1"] == "Да"]
                df_retir_slise["Статус"] = "Пропущенный Пятница"
                df_retir = df_retir.append(df_retir_slise)
                df_full = df_full.loc[df_full["Пропущеный 1"] != "Да"]


                # Добавляем магазин, с которым был последний звонок
                df_retir_rez = df_retir[["Клиент", "Статус"]].reset_index().drop(["index"], axis=1).merge(df_last_call[["Клиент", "Сотрудник", "Должность"]], how="left")
                df_maybe_rez = df_maybe[["Клиент", "Статус"]].reset_index().drop(["index"], axis=1).merge(df_last_call[["Клиент", "Сотрудник", "Должность"]], how="left")

                # Меняем столбцы местами
                df_retir_rez = df_retir_rez.reindex(columns=['Клиент', 'Сотрудник', 'Должность', 'Статус'])
                df_maybe_rez = df_maybe_rez.reindex(columns=['Клиент', 'Сотрудник', 'Должность', 'Статус'])

                return df_retir_rez, df_maybe_rez           
        else:
            return pd.DataFrame()

    # Получение уникальных клиентов для конкретного manager и shop
    def get_unique_clients_file(self, manager, shop):
        output = io.BytesIO()
        writers = pd.ExcelWriter(output, engine='xlsxwriter')

        """
        pd.io.formats.excel.header_style = {
            "font": {"bold": True},
            "borders": {"top": "thin", "right": "thin", "bottom": "thin", "left": "thin"},
            "pattern": {"pattern": "solid", "fore_colour": 26},
            "alignment": {"horizontal": "center", "vertical": "top"}
        }
        """

        uniques_0 = self.report_df_in_time[self.report_df_in_time["Сотрудник"] == manager]
        uniques_1 = uniques_0[uniques_0["Должность"] == shop]
        #uniques = pd.pivot_table(uniques_1, values="Клиент", index=['Сотрудник', 'Должность'],
        #    columns=['Тип звонка'], aggfunc='count').reset_index()
        uniques = uniques_1.groupby('Клиент').count().reset_index()
        uniques.rename(columns={"Дата":"Количество звонков"}, inplace=True)
        uniques = uniques[["Клиент", "Количество звонков"]]
        uniques.sort_values("Количество звонков")
        uniques.to_excel(writers, sheet_name='Sheet1')

        writers.save()

        return output, "uniques" + "_" + manager + "_" + shop + ".xlsx"


    # Формирование .xlsx файла статистики
    def get_statistic_file(self):
        output = io.BytesIO()
        writer = pd.ExcelWriter(output, engine='xlsxwriter')

        pd.io.formats.excel.header_style = {
            "font": {"bold": True},
            "borders": {"top": "thin", "right": "thin", "bottom": "thin", "left": "thin"},
            "pattern": {"pattern": "solid", "fore_colour": 26},
            "alignment": {"horizontal": "center", "vertical": "top"}
        }

        statistic_table_nn = self.report_statistic_table[self.report_statistic_table["Должность"].str.startswith("НН", na= False)].reset_index(drop=True)
        statistic_table_kr = self.report_statistic_table[self.report_statistic_table["Должность"].str.startswith("КР", na= False)].reset_index(drop=True)
        statistic_table_msk = self.report_statistic_table[self.report_statistic_table["Должность"].str.startswith("МСК", na= False)].reset_index(drop=True)
        
        statistic_table_nn.index += 1
        statistic_table_kr.index += 1
        statistic_table_msk.index += 1

        statistic_table_nn.to_excel(writer, sheet_name='Sheet1',startrow=0)
        statistic_table_kr.to_excel(writer, sheet_name='Sheet1',startrow=statistic_table_nn.shape[0] + 2, header=False)
        statistic_table_msk.to_excel(writer, sheet_name='Sheet1',startrow=statistic_table_nn.shape[0] + statistic_table_kr.shape[0] + 3, header=False)
        
        worksheet = writer.sheets['Sheet1']
        worksheet.set_column(1, 2, 45)
        worksheet.set_column(3, 8, 20)

        writer.save()
        

        return output, "stats.xlsx"
    

    # Формирование .xlsx файла уникальных
    def get_unique_file(self):
        output = io.BytesIO()
        writer = pd.ExcelWriter(output, engine='xlsxwriter')
        
        pd.io.formats.excel.header_style = {
            "font": {"bold": True},
            "borders": {"top": "thin", "right": "thin", "bottom": "thin", "left": "thin"},
            "pattern": {"pattern": "solid", "fore_colour": 26},
            "alignment": {"horizontal": "center", "vertical": "top"}
        }
        self.unique_clients.to_excel(writer, sheet_name='Sheet1')
        
        worksheet = writer.sheets['Sheet1']
        worksheet.set_column(1, 2, 30)
        
        writer.save()
        
        [mindate, maxdate] = self.get_file_date()

        return output, "unique_clients_" + mindate + "_" + maxdate + ".xlsx"

    # Формирование .xlsx файла потерянных
    def get_retir_file(self):
        output = io.BytesIO()
        writer = pd.ExcelWriter(output, engine='xlsxwriter')
        
        pd.io.formats.excel.header_style = {
            "font": {"bold": True},
            "borders": {"top": "thin", "right": "thin", "bottom": "thin", "left": "thin"},
            "pattern": {"pattern": "solid", "fore_colour": 26},
            "alignment": {"horizontal": "center", "vertical": "top"}
        }
        self.retir_lost_clients_table.to_excel(writer, sheet_name='Sheet1')
        
        worksheet = writer.sheets['Sheet1']
        worksheet.set_column(1, 7, 30)
        
        writer.save()
        

        return output, "retir.xlsx"
    
    # Формирование .xlsx файла доп таблицы
    def get_add_file(self):
        output = io.BytesIO()
        writer = pd.ExcelWriter(output, engine='xlsxwriter')
        
        pd.io.formats.excel.header_style = {
            "font": {"bold": True},
            "borders": {"top": "thin", "right": "thin", "bottom": "thin", "left": "thin"},
            "pattern": {"pattern": "solid", "fore_colour": 26},
            "alignment": {"horizontal": "center", "vertical": "top"}
        }

        self.add_df_more_clients.to_excel(writer, sheet_name='Sheet1')

        worksheet = writer.sheets['Sheet1']
        worksheet.set_column(1, 7, 30)
        
        writer.save()
        

        return output, "retir_maybe.xlsx"
