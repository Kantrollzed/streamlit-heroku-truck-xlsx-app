from flask import Flask, render_template, request, redirect, url_for, send_file
from xls_preprocessor import XlsProcessor
import os
from datetime import timedelta, datetime

processor = XlsProcessor()

app = Flask(__name__, static_folder="static")

# Шапка
@app.route('/')
def index():
    #return render_template('main.html')
    return redirect(url_for('set_table_from_ats'))


# Подгрузка данных из файла
@app.route('/', methods=['POST'])
@app.route('/current_table', methods=["POST"])
def upload_file():
    uploaded_file = request.files['file']
    if uploaded_file.filename != '':
        print(uploaded_file.filename)
        processor.filename = uploaded_file.filename
        processor.set_report_file(True, uploaded_file.read())
    return redirect(url_for('current_table'))

# Подгрузка данных с АТС
@app.route('/current_table_from_ats', methods=["POST", "GET"])
def set_table_from_ats():
    processor.set_report_file(False, None)
    return current_table()

# Подгрузка данных с АТС
@app.route('/current_table_from_ats/manager=<manager>shop=<shop>', methods=["POST", "GET"])
def set_table_from_ats_uniques(manager, shop):
    xlsx_file, fil = processor.get_unique_clients_file(manager, shop)
    xlsx_file.seek(0)
    return send_file(
        xlsx_file,
        as_attachment=True,
        download_name=fil,
        mimetype='text/xls',
    )

# Таблицы за выбранный временной период
@app.route('/current_table', methods=["GET"])
def current_table(actors = "Общий", filial = "Все", report_mode = "Статистика за последний день", is_unique_users = 0, date_limits = [0, 0]):
    # временной промежуток
    file_date = processor.get_file_date()
    if date_limits == [0, 0]:
        date_limits = file_date
    
    params = {
        "actors": actors,
        "filial": filial,
        "report_mode": report_mode,
        "is_unique_users": is_unique_users,
        "date_limits": date_limits,
    }

    table_formats, table_filials, table_times = processor.get_table_filters(params)
    table_filters = {
        "table_formats": table_formats,
        "table_filials": table_filials,
        "table_times": table_times
    }
    is_valid = processor.set_statistic_day_table(params, base_url = request.base_url)

    # Формируем таблицу статистики за нужный временной период
    statistic_table = processor.get_statistic_table()
    lost_clients_table = processor.get_retir_table()
    add_table = processor.get_add_table()
    
    print(params)

    return render_template(
        'tables.html',
        file_or_api = processor.file_or_api,
        is_valid = is_valid,
        params = params,
        table_filters = table_filters,
        filename = processor.filename,
        file_date = file_date,
        statistic_table = statistic_table,
        lost_clients_table = lost_clients_table,
        add_table = add_table
    )


# Скачивание файла статистики
@app.route('/download_st', methods=["POST"])
def download_sfile():
    xlsx_static_file, sfilename = processor.get_statistic_file()
    if request.method == "POST":
        xlsx_static_file.seek(0)
        return send_file(
            xlsx_static_file,
            as_attachment=True,
            download_name=sfilename,
            mimetype='text/xls',
        )
    return redirect(url_for('current_table'))

# Скачивание файла потерянных
@app.route('/download_rt', methods=["POST"])
def download_rfile():
    xlsx_retir_file, rfilename = processor.get_retir_file()
    if request.method == "POST":
        xlsx_retir_file.seek(0)
        return send_file(
            xlsx_retir_file,
            as_attachment=True,
            download_name=rfilename,
            mimetype='text/xls'
        )
    return redirect(url_for('current_table'))

# download_unique_clients
@app.route('/download_unique_clients', methods=["POST"])
def download_ufile():
    unique_clients, rfilename = processor.get_unique_file()
    if request.method == "POST":
        unique_clients.seek(0)
        return send_file(
            unique_clients,
            as_attachment=True,
            download_name=rfilename,
            mimetype='text/xls'
        )
    return redirect(url_for('current_table'))

# Скачивание файла доп
@app.route('/download_at', methods=["POST"])
def download_afile():
    xlsx_retir_file, rfilename = processor.get_add_file()
    if request.method == "POST":
        xlsx_retir_file.seek(0)
        return send_file(
            xlsx_retir_file,
            as_attachment=True,
            download_name=rfilename,
            mimetype='text/xls'
        )
    return redirect(url_for('current_table'))


@app.route('/reset_table', methods=["POST"])
def set_table():
    actors =  request.form.get('actors')
    filial =  request.form.get('filial')
    report_mode =  request.form.get('report_mode')
    is_unique_users = request.form.get('is_unique_users')
    date_limits = [request.form.get('DateIn'), request.form.get('DateOut')]
    
    return current_table(actors, filial, report_mode, is_unique_users, date_limits)


if __name__ == "__main__":
    #app.run(debug=True)
    port = int(os.environ.get('PORT', 5000))
    #app.run(host='0.0.0.0', port=port)
    from waitress import serve
    serve(app, host="0.0.0.0", port=port)

