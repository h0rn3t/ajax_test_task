from flask import Flask, request, jsonify, make_response
import csv
import sqlite3


app = Flask(__name__)


class DBCrud:

    def __init__(self):
        self.conn = sqlite3.connect('test_results.sqlite3')
        self.conn.row_factory = self._dict_factory
        self.cursor = self.conn.cursor()

    def _dict_factory(self, cursor, row):
        d = {}
        for idx, col in enumerate(cursor.description):
            d[col[0]] = row[idx]
        return d

    def create_table(self):
        query = """
            CREATE TABLE IF NOT EXISTS test_results(
                `id` INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL ,
                device_type TEXT, 
                operator TEXT,
                time DATETIME,
                success INTEGER 
            )   
        """
        self.cursor.execute(query)
        self.conn.commit()

    def add_result(self, device_type, operator, time, success):
        query = """
            INSERT INTO test_results(  device_type, operator, time, success)
                        VALUES ( ?,?,?,?)
        """
        self.cursor.execute(query, (device_type, operator, time, success))
        self.conn.commit()

    def get_results(self):
        query = "SELECT * FROM test_results"
        self.cursor.execute(query)
        all_rows = self.cursor.fetchall()
        self.conn.commit()
        return all_rows

    def get_results_by_operator(self, operator: str) -> list:
        query = f"SELECT * FROM test_results WHERE operator='{operator}'"
        print(query)
        self.cursor.execute(query)
        all_rows = self.cursor.fetchall()
        self.conn.commit()
        return all_rows

    def delete_result(self, res_id: int):
        query = "DELETE FROM test_results WHERE id = {}".format(res_id)
        self.cursor.execute(query)
        self.conn.commit()

    def __del__(self):
        self.conn.close()


@app.before_request
def before_request_func():
    db = DBCrud()
    db.create_table()
    with open('test_results.csv') as csvfile:
        reader = csv.DictReader(csvfile, delimiter=',')
        reader.fieldnames = [name.lower().replace(' ', '_') for name in reader.fieldnames]
        for row in reader:
            db.add_result(
                row['device_type'],
                row['operator'],
                row['time'],
                row['success'])


@app.route('/api_v1/stat', methods=["GET"])
def get_stat():
    operator = request.args.get('operator')
    db = DBCrud()
    stat = dict()
    if operator:
        results = db.get_results_by_operator(operator)
    else:
        results = db.get_results()
    for res in results:
        if res['device_type'] not in stat:
            stat[res['device_type']] = {'success': 0, 'fail': 0, 'total': 0}
        if res['success']:
            stat[res['device_type']]['success'] += 1
        else:
            stat[res['device_type']]['fail'] += 1
        stat[res['device_type']]['total'] += 1
    return make_response(jsonify(stat), 200)


@app.route('/api_v1/test_result', methods=["POST"])
def add_result():
    db = DBCrud()
    db.add_result(
        request.form['device_type'],
        request.form['operator'],
        request.form['time'],
        request.form['success'])
    return make_response(jsonify({}), 201)


@app.route('/api_v1/test_result/<int:record_id>', methods=["DELETE"])
def del_result(record_id):
    db = DBCrud()
    db.delete_result(record_id)
    return make_response(jsonify({}), 204)


if __name__ == '__main__':
    app.run()
