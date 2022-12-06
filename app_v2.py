from datetime import timedelta, datetime
from email.policy import default
from flask import Flask, redirect, session, url_for, render_template, request, session, flash, send_file
from flask_sqlalchemy import SQLAlchemy
import pandas as pd
import io
import os
import pytz

app = Flask(__name__, static_folder="static")
app.secret_key = "secret key"
app.config['SQLALCHEMY_DATABASE_URI'] = 'postgresql+psycopg2://dzyzeaowvvroes:f7f9bb8bb589cccd1c5a653488735700845e16e402a87c7a56f34c003cbf94c7@ec2-44-193-178-122.compute-1.amazonaws.com:5432/d1gbgp4lcedffq'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
app.permanent_session_lifetime = timedelta(days=10)
db = SQLAlchemy(app)

class download_stuff_class():
    df = pd.DataFrame()
    def __init__(self, df):
        self.df = df
    
    def change_df(self, df):
        self.df = df

download_stuff_df = download_stuff_class(pd.DataFrame())

# Таблица пользователей
class users(db.Model):
    _id = db.Column("id", db.Integer, primary_key=True)
    name = db.Column(db.String(100))
    email = db.Column(db.String(100))
    password = db.Column(db.String(100))

    def __init__(self, name, email, password):
        self.name = name
        self.email = email
        self.password = password



@app.route("/")
def home():
    if "user" in session:
        return redirect(url_for("user"))
    else:
        return redirect(url_for("login"))
    

@app.route("/login", methods=["POST", "GET"])
def login():
    if request.method == "POST":
        session.permanent = True
        email = request.form["email"]
        #print(users.query)
        found_user = users.query.filter_by(email=email).first()
        if found_user and request.form["pass"] == found_user.password:
            session["email"] = email
            session["user"] = found_user.name
            return redirect(url_for("user", usr=found_user.name))
        else:
            flash("Пользователь не найден")
            return render_template("login.html")
    
    else:
        if "user" in session:
            flash("Пользователь уже авторизирован")
            return redirect(url_for("user"))
        
        return render_template("login.html")


@app.route("/reg", methods=["POST", "GET"])
def reg():
    if request.method == "POST":
        session.permanent = True
        
        found_user = users.query.filter_by(name=request.form["name"]).first()
        if not found_user:
            session["email"] = request.form["email"]
            session["user"] = request.form["name"]
            session["password"] = request.form["pass"]
            usr = users(request.form["name"], request.form["email"], request.form["pass"])
            db.session.add(usr)
            db.session.commit()

            return redirect(url_for("user", usr=request.form["name"]))
            
        else:
            flash("Пользователь уже добавлен")
            return redirect(url_for("reg"))
    
    else:
        #if "user" in session:
        #    flash("Пользователь уже добавлен")
        #s    return redirect(url_for("user"))
        
        return render_template("reg.html")


@app.route("/user/<phone_number>/set")
@app.route("/user", defaults={"phone_number": "1"})
def user(phone_number):
    if "user" in session:
        # Подгружаем данные полностью
        staff = staffs.query.all()
        
        if len(staff):
            staff_dicts = []
            for row in staff:
                staff_dicts.append(row.get_dict())
            staff_dicts = pd.DataFrame(staff_dicts)
            #print(staff_dicts)

            download_stuff_df.change_df(staff_dicts)
            #print(download_stuff_df)

            staff_dicts["work_post_url"] = "<a href=\"" + request.base_url.split('/user')[0] + "/user/" + staff_dicts["phone_number"] +  "/set#change_modal\"" + ">Подробнее..</a>"
            df_staffs = staff_dicts[["date_add", "name", "work_post", "phone_number", "salary", "address", "date_interview", "work_post_url"]]\
                    .rename(columns={
                        "date_add": "Дата анкетирования",
                        "work_post": "Должность",
                        "address": "Адрес", 
                        "name": "ФИО", 
                        "phone_number": "Номер телефона",
                        "salary": "Зарплатные ожидания",
                        "date_interview": "Дата интервью",
                        "work_post_url": "Подробнее",
                        }
                    )
            #print(staff_dicts["work_post_url"])

            #print(phone_number)
            # Пробуем подгрузить заданный номер
            found_stuff = staffs.query.filter_by(phone_number=phone_number).first()
            if found_stuff:
                return render_template(
                "user.html",
                found_stuff = found_stuff.get_dict(),
                df_staffs = df_staffs
                )

            # Если не вышло - Подгружаем без конкретного клиента
            else:
                return render_template(
                "user.html",
                found_stuff = {},
                df_staffs = df_staffs
                )

        elif len(staff) == 0:
            #print(pd.DataFrame())
            return render_template(
                "user.html",
                df_staffs = pd.DataFrame()
            )
    
    else:
        flash("Вы не вошли в систему")
        return redirect(url_for("login"))


@app.route("/user/<phone_number>/set",  methods=["POST", "GET"])
def current_user(phone_number):
    if "user" in session:
        found_stuff = staffs.query.filter_by(phone_number=phone_number).first()
        if found_stuff:
            return render_template(
                "user.html",
                df_staffs = pd.DataFrame()
            )
    else:
        flash("Вы не вошли в систему")
        return redirect(url_for("login"))


@app.route("/user/<phone_number>/delete",  methods=["POST", "GET"])
def delete_current_user(phone_number):
    if "user" in session:
        found_stuff = staffs.query.filter_by(phone_number=phone_number).first()
        if found_stuff:
            db.session.delete(found_stuff)
            db.session.commit()

            flash("Кандидат " + phone_number + " удалён")
            return redirect(url_for("user"))
    else:
        flash("Вы не вошли в систему")
        return redirect(url_for("login"))


@app.route("/add_employee",  methods=["POST"])
def add_employee():
    if request.method == "POST":
        new_person_dict = {
            "name": request.form["name"], 
            "age": request.form["age"], 
            "femaly_status": request.form["femaly_status"],
            "address": request.form["address"],
            "is_driver": request.form["is_driver"],
            "bad_habits": request.form["bad_habits"],
            "fav_activity": request.form["fav_activity"],
            "work_experience": request.form["work_experience"],
            "education": request.form["education"],
            "work_post": request.form["work_post"],
            "why_this_work": request.form["why_this_work"],
            "qualities": request.form["qualities"],
            "salary": request.form["salary"],
            "sale_experience": request.form["sale_experience"],
            "social_network": request.form["social_network"],
            "phone_number": request.form["phone_number"],
            "date_interview": request.form["date_interview"],
            "date_add": datetime.now(pytz.timezone('Europe/Moscow')).strftime("%d-%m-%Y %H:%M")
        }

        found_stuff = staffs.query.filter_by(phone_number=new_person_dict["phone_number"]).first()
        if not found_stuff:
            new_person = staffs(new_person_dict)
            db.session.add(new_person)
            db.session.commit()
        else:
            flash("Кандидат с таким же номером телефона уже добавлен")
    
    return redirect(url_for("user"))

@app.route("/refactor_employee",  methods=["POST"])
def refactor_employee():
    if request.method == "POST":
        #tz = pytz.timezone("Europe/Moscow", is_dst=None)
        now = datetime.now(pytz.timezone('Europe/Moscow')) # + timedelta(hours=3) #tz.localize(datetime.now())
        print(now)
        new_person_dict = {
            "name": request.form["name"], 
            "age": request.form["age"], 
            "femaly_status": request.form["femaly_status"],
            "address": request.form["address"],
            "is_driver": request.form["is_driver"],
            "bad_habits": request.form["bad_habits"],
            "fav_activity": request.form["fav_activity"],
            "work_experience": request.form["work_experience"],
            "education": request.form["education"],
            "work_post": request.form["work_post"],
            "why_this_work": request.form["why_this_work"],
            "qualities": request.form["qualities"],
            "salary": request.form["salary"],
            "sale_experience": request.form["sale_experience"],
            "social_network": request.form["social_network"],
            "phone_number": request.form["phone_number"],
            "date_interview": request.form["date_interview"],
            "date_add": now.strftime("%d-%m-%Y %H:%M")
        }

        found_stuff = staffs.query.filter_by(phone_number=new_person_dict["phone_number"]).first()
        if found_stuff:
            found_stuff.set_employee(new_person_dict)
            db.session.commit()
            #new_person = staffs(new_person_dict)
            #db.session.add(new_person)
            #print(found_stuff.name)
            
            #for key in new_person_dict:
            #    if found_stuff.key != new_person_dict[key]:
            #        setattr(found_stuff, key, new_person_dict[key])
            #        db.session.commit()
        else:
            flash("Почему-то такого пользователя в системе нет")
    
    return redirect(url_for("user"))

@app.route("/download_stuff",  methods=["POST"])
def download_stuff():
    output = io.BytesIO()
    print(download_stuff_df.df)
    if not download_stuff_df.df.empty:
        writer = pd.ExcelWriter(output, engine='xlsxwriter')
        download_stuff_df.df.to_excel(writer, sheet_name='Sheet1')
        writer.save()
    
    flash("Скачен файл клиентов", "info")
    unique_clients = output
    rfilename = "stuff_file.xlsx"

    if request.method == "POST":
        unique_clients.seek(0)
        return send_file(
            unique_clients,
            as_attachment=True,
            download_name=rfilename,
            mimetype='text/xls'
        )
    return redirect(url_for("user"))



@app.route("/logout")
def logout():
    flash("Вы вышли из аккаунта", "info")
    session.pop("user", None)
    session.pop("email", None)
    return redirect(url_for("login"))




if __name__ == "__main__":
    with app.app_context():
        db.create_all()
    #app.run(debug=True)
    port = int(os.environ.get('PORT', 5000))
    #app.run(host='0.0.0.0', port=port)
    from waitress import serve
    serve(app, host="0.0.0.0", port=port)