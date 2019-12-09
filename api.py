from flask import Flask, g
from flask_restful import Resource, Api, reqparse
from main import (get_by_gender, get_birth_date_ascending,
    get_last_name_descending, set_db_path, parse_data_line,
    insert_into_sqlite_table, create_sqlite_table, connect_sqlite_db,
    get_db_path)

app = Flask(__name__)
api = Api(app)

def get_conn():
    db = getattr(g, '_database', None)
    if db is None:
        db = g._database = connect_sqlite_db(get_db_path())
    return db

@app.teardown_appcontext
def teardown_db(exception):
    db = getattr(g, '_database', None)
    if db is not None:
        db.close()

def rows_to_dictionary(rows, keys):
    return [{key : val for key, val in zip(keys, row)}
        for row in rows]

class Records(Resource):
    record_getters = {
        'gender' : get_by_gender ,
        'birthdate' : get_birth_date_ascending ,
        'name' : get_last_name_descending ,
    }
    def get(self, get_by):
        db = get_conn()
        if get_by not in Records.record_getters:
            return {'message' : 'Resource not found', 'data' : []} , 404
        function = Records.record_getters[get_by]

        keys = ('last_name', 'first_name', 'gender', 'color', 'dateofbirth')
        rows = function(db)

        dictionary_list = rows_to_dictionary(rows, keys)
        return dictionary_list , 200


class AddRecord(Resource):
    def post(self):
        parser = reqparse.RequestParser()
        db = get_conn()

        parser.add_argument('data', required=True)
        args = parser.parse_args()
        data = args['data']
        parsed_line = parse_data_line(data)

        insert_into_sqlite_table(db, parsed_line)
        db.commit()
        return {'message' : 'data successfully parsed and added'} , 201

api.add_resource(Records, '/records/<string:get_by>')

api.add_resource(AddRecord, '/records')

if __name__ == "__main__":
    set_db_path("./people.db")
    # conn = get_db()
    # create_sqlite_table(conn)
    app.run(debug=True)