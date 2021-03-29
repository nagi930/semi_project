from flask import Flask, render_template
import pandas as pd

app = Flask(__name__, template_folder='templates')

@app.route('/')
def index():
    # return 'Hello Flask'
    price = 20000

    li = [1000, 2000, 3000, 4000, 5000, 6000, 7000, 8000, 9000, 10000, 11000, 12000, 13000]
    a = [df_list, li]
    return render_template('index.html', price_= a)



@app.route('/info')
def info():
    return 'Info'

if __name__ == '__main__':
    app.run(host='localhost', port=8078, debug=True)