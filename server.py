import streamlit as st
from xls_preprocessor import XlsProcessor
import pandas as pd

st.set_page_config(layout="wide")
st.write("""
# Приложение для выгрузки статистики из xls файла
### Загрузите .xls файл и получите обработанный результат
""")

# Получение дневного файла, 
# создание репорта по менеджерам, статистики на каждого из них

# Подгрузка файла
uploaded_report_file = st.file_uploader("Выберите файл")
if uploaded_report_file is not None:
    processor = XlsProcessor()
    # Предварительная обработка файла .xlsx
    processor.set_report_file(uploaded_report_file)
    # создаём колонки чтобы задать параметры формируемого файла
    file_date = processor.get_file_date()
    st.write("C " + file_date[0] + " по " + file_date[1])
    col1, col2, col3 = st.columns(3)
    with col1:
        option1 = st.selectbox(
            'Формат отчёта:',
            ('Общий', 'Магазины', 'Менеджеры'))
        #st.write('You selected:', option1)

    with col2:
        option2 = st.selectbox(
            'Филиалы:',
            ('Все', 'НН', 'КР', 'МСК'))
        #st.write('You selected:', option2)

    with col3:
        option3 = st.selectbox(
            'Временной промежуток:',
            ('Последний День', 'Весь Период'))
        #st.write('You selected:', option3)
    
    col4, col5, col6, col7, col8, col9  = st.columns(6)
    #with col4:
    #    option4 = st.checkbox("Уникальные клиенты")

    #with col5:
        #option2 = st.selectbox(
        #    'Филиалы:',
        #    ('Все', 'НН', 'КР', 'МСК'))
    #    st.write('You selected:', option2)

    #with col6:
        #option3 = st.selectbox(
        #    'Временной промежуток:',
        #    ('Весь Период', 'Последний День'))
    #    st.write('You selected:', option3)

    options = [option1, option2, option3] #, option4]

    # Формируем таблицу статистики за нужный временной период
    statistic_table, lost_clients_table = processor.set_statistic_day_table(options)
    #st.write(x)
    st.header("Менеджеры / Магазины")
    st.table(statistic_table)
    
    # Таблица -> .xlsx file c возможностью скачать
    xlsx_static_file, sfilename = processor.get_statistic_file()


    st.download_button(
            label="Скачать файл статистики",
            data=xlsx_static_file.getvalue(),
            file_name=sfilename,
            mime='text/xls',
    )
    
    st.header("Потерянные клиенты")
    st.table(lost_clients_table)

    # Таблица -> .xlsx file c возможностью скачать
    xlsx_retir_file, rfilename = processor.get_retir_file()

    st.download_button(
            label="Скачать файл потеренных клиентов",
            data=xlsx_retir_file.getvalue(),
            file_name=rfilename,
            mime='text/xls',
    )
    
