__author__ = 'Pramod.Kumar'
from flask import Flask,jsonify,abort,request
import json
app = Flask(__name__)

tasks = [
    {
        'id': 1,
        'title': u'Buy groceries',
        'description': u'Milk, Cheese, Pizza, Fruit, Tylenol',
        'done': False
    },
    {
        'id': 2,
        'title': u'Learn Python',
        'description': u'Need to find a good Python tutorial on the web',
        'done': False
    }
]

@app.route('/')
def hello_world():
    return 'Hello World !'
def jsonpify(obj):
    """
    Like jsonify but wraps result in a JSONP callback if a 'callback'
    query param is supplied.
    """
    try:
        callback = request.args['callback']
        response = app.make_response("%s(%s)" % (callback, json.dumps(obj)))
        response.mimetype = "text/javascript"
        return response
    except KeyError:
        return jsonify(obj)
@app.route('/tasks',  methods=['POST', 'GET'])
def get_tasks():
    query = request.form.get('query')
    return jsonpify({'name':'Percentage Allocation Reconsiliation service','percentageAlloction': query})
    # return jsonify({'name':'Percentage Allocation Reconsiliation service','percentageAlloction': query})

@app.route('/tasks/<pa>', methods=['GET'])
def get_task(pa):
    task =  str(pa).strip('%')
    if task == None:
        abort(404)
    return jsonify({'name':'Percentage Allocation Reconsiliation service','percentageAlloction': task})

if __name__ == '__main__':
    app.run(debug=True)