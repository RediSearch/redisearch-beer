from app import app
from flask import render_template, Flask, g
import redis
from redis.commands.search.query import Query

def docs_to_dict(docs):
    reslist = []
    for doc in docs:
        ddict = {}
        for field in dir(doc):
            if field.startswith('__'):
                continue
            ddict.update({ field : getattr(doc, field) })
        reslist.append(ddict)
    return reslist

@app.before_request
def before_request():
    g.redis = redis.StrictRedis(
        host=app.config['REDIS_HOST'],
        port=app.config['REDIS_PORT'],
    )
    g.rsbeer = g.redis.ft("beerIdx")
    g.rsbrewery = g.redis.ft("breweryIdx")

@app.route('/')
@app.route('/index')
def index():
    query = '+@abv:[2 7] +@ibu:[1 +inf]'
    q = Query(query)
    result = g.rsbeer.search(q)
    res = docs_to_dict(result.docs)

    return render_template(
        'index.html',
        title='Home',
        count=result.total,
        duration=result.duration,
        rsindex=g.rsbeer.info()['index_name'],
        rsquery=q.query_string(),
        result=res
    )
