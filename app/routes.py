from app import app
from flask import render_template, Flask, g
from redisearch import Client, Query

@app.before_request
def before_request():
    g.rsbeer = Client(
        'beerIdx',
        host=app.config['REDIS_HOST'],
        port=app.config['REDIS_PORT']
    )
    # g.rs.brewery = Client('breweryIdx')

@app.route('/')
@app.route('/index')
def index():
    query = '\"lager\" +@abv:[2 7]'
    q = Query(query)
    result = g.rsbeer.search(q)
    
    return render_template(
        'index.html',
        title='Home',
        rsindex=g.rsbeer.info()['index_name'],
        rsquery=q.query_string(),
        result=result
    )

            

