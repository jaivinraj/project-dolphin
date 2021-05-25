import sqlalchemy as sa
from jinja2 import Template


def get_engine(user, password, host, database="dolphin_database"):
    return sa.create_engine(f"postgresql://{user}:{password}@{host}:5432/{database}")


def get_table_creation_query(tablename, coldict, schema, index_cols=[], unique_cols=[]):
    q_params = {
        "schema": schema,
        "tablename": tablename,
        "cols": coldict,
        "index_cols": index_cols,
        "unique_str": {i: ("UNIQUE" if i in unique_cols else "") for i in coldict}
        #         "tempstr": "TEMP TABLE " if temp else "TABLE IF NOT EXISTS",
    }
    qj_tmplt = Template(
        """
        CREATE TABLE IF NOT EXISTS {{schema}}.{{tablename}}
            
            (
            id SERIAL PRIMARY KEY,
            {% for col in cols %}
            {{col}} {{cols[col]}} {{unique_str[col]}}
            {% if not loop.last %}
                ,
            {% endif %}
            {% endfor %}
            );
            {% for indcol in index_cols %}
            CREATE INDEX IF NOT EXISTS {{indcol}}_{{tablename}}_idx ON {{schema}}.{{tablename}} ({{indcol}});
            {% endfor %}
        
        """
    )
    return qj_tmplt.render(q_params)