import html
import json
import os
import bleach
import plotly
import tornado.ioloop
import plotly.graph_objects as go
import tornado.web
from pyflink.table.window import Tumble

from plotly.utils import PlotlyJSONEncoder
import plotly.io as pio
from pyflink.table import functions as expr
from pyflink.common import Row
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table.expressions import lit, col
from pyflink.table import TableEnvironment, EnvironmentSettings, DataTypes, CsvTableSource, TableDescriptor, Schema, \
    FormatDescriptor, Table, TableSchema, Expression
import jinja2
import pandas as pd

from pyflink.table.udf import udtf
import markupsafe
class Jinja2Loader(jinja2.BaseLoader):
    def __init__(self):
        super(Jinja2Loader, self).__init__()

    def get_source(self, environment, template):
        return template, None, lambda: True


class MainHandler(tornado.web.RequestHandler):
    def initialize(self, template_env):
        self.template_env = template_env

    def get(self):
      try:

        #output_path ='output1.csv'
        output_path = ''
        current_dir = os.path.dirname(os.path.abspath(__file__))

        # Specify the relative path to the Excel file
        excel_file_path = os.path.join(current_dir, 'Repair_date.csv')
        # Set up the execution environment
        #env = StreamExecutionEnvironment.get_execution_environment()
        env_settings = EnvironmentSettings.new_instance().in_batch_mode().build()
        tbl_env = TableEnvironment.create(env_settings)
        t_env = TableEnvironment.create(EnvironmentSettings.in_streaming_mode())
        # write all the data to one file
        t_env.get_config().set("parallelism.default", "1")

        #column_names = ['trx_id', 'trx_date', 'src_curr', 'amnt_src_curr',
        #                'amnt_gbp', 'user_id', 'user_type', 'user_country']

        #column_types = [DataTypes.STRING(), DataTypes.DATE(), DataTypes.STRING(), DataTypes.DOUBLE(),
        #                DataTypes.DOUBLE(), DataTypes.INT(), DataTypes.STRING(), DataTypes.STRING()]

        column_names = ['created_date', 'repair_date', 'isdelivery', 'price', 'tech_id', 'client_id', 'brand',
                            'problem', 'component', 'feedback_rate','feedback_text','repair_id']
        column_types = [DataTypes.STRING(), DataTypes.STRING(), DataTypes.BOOLEAN(), DataTypes.DOUBLE(),
                            DataTypes.STRING(), DataTypes.STRING(), DataTypes.STRING(), DataTypes.STRING(),
                            DataTypes.STRING(), DataTypes.DOUBLE(),DataTypes.STRING(),DataTypes.STRING()]

        source = CsvTableSource(
            'fin_trxs.csv',
            column_names,
            column_types,
            ignore_first_line=False
        )
        schema = Schema.new_builder()
        for name, data_type in zip(column_names, column_types):
            schema.column(name, data_type)
        schema = schema.build()
        tble_desc = TableDescriptor.for_connector('filesystem')\
            .schema(schema).format('csv').option('path',excel_file_path).build()

        catalog_name = "my_catalog"
        table_name = "financial_trxs"


        tbl_env.create_temporary_table('financial_trxs', tble_desc)
        tbl = tbl_env.from_path('financial_trxs')

        if output_path is not None:
            # Get the schema of the query result
            result_schema = tbl.get_schema()

            # Create the sink table with the same schema as the query result
            tbl_env.create_temporary_table('sink',
                                           TableDescriptor.for_connector('print')
                                           .schema(schema)
                                           .build())
        else:
            print("Printing result to stdout. Use --output to specify output path.")
            # Get the schema of the query result
            result_schema = tbl.get_schema()

            # Create the sink table with the same schema as the query result
            tbl_env.create_temporary_table('sink',
                                           TableDescriptor.for_connector('print')
                                           .schema(result_schema)
                                           .build())
        tbl.execute_insert('sink').wait()

        # Convert the table to a pandas DataFrame
        df = tbl.to_pandas()
        # Render the DataFrame in an HTML table
        table_html = df.to_html()
        template_dir = os.path.join(os.path.dirname(__file__), 'template')
        template = self.template_env.get_template(os.path.join(template_dir, 'index.html'))
        template = self.template_env.get_template('\\Template\\index.html')
        #rendered_table = template.render(table=table_html)
        safe_table_html = bleach.clean(table_html)

        # Execute the job
        #id = t_env.execute("MyJob")
        #print("job id" + str(id))
        print(df)
        table = df.to_dict('records')
        self.render("Template\\index.html", table=table)
      except Exception as e:
          print(e)
          exit(1)


class DynamicHandler(tornado.web.RequestHandler):
    def get(self):
        t_env = TableEnvironment.create(EnvironmentSettings.in_streaming_mode())
        # write all the data to one file
        t_env.get_config().set("parallelism.default", "1")
        column_names = ['created_date', 'repair_date', 'isdelivery', 'price', 'tech_id', 'client_id', 'brand',
                            'problem', 'component', 'feedback_rate','feedback_text','repair_id']
        column_types = [DataTypes.STRING(), DataTypes.STRING(), DataTypes.BOOLEAN(), DataTypes.DOUBLE(),
                            DataTypes.STRING(), DataTypes.STRING(), DataTypes.STRING(), DataTypes.STRING(),
                            DataTypes.STRING(), DataTypes.DOUBLE(),DataTypes.STRING(),DataTypes.STRING()]
        source = CsvTableSource(
            'fin_trxs.csv',
            column_names,
            column_types,
            ignore_first_line=False
        )
        schema = Schema.new_builder()
        for name, data_type in zip(column_names, column_types):
            schema.column(name, data_type)
        schema = schema.build()
        tble_desc = TableDescriptor.for_connector('filesystem')\
            .schema(schema).format('csv').option('path','fin_trxs.csv').build()

        catalog_name = "my_catalog"
        table_name = "financial_trxs"


        t_env.create_temporary_table('financial_trxs', tble_desc)

        tbl = t_env.from_path('financial_trxs')

        # Convert the table to a pandas DataFrame
        df = tbl.to_pandas()
        # Retrieve the dynamic content using PyFlink
        # Example: Query a table and fetch the result
        query = "SELECT * FROM financial_trxs"
        result = t_env.execute_sql(query).collect()

        @udtf(result_types=[DataTypes.STRING()])
        def split(line: Row):
            for s in str(line[0]).split():
                yield Row(s)

        # compute word count
        count = tbl.flat_map(split).alias('repair_id') \
            .group_by(col('repair_id')) \
            .select(col('repair_id'), lit(1).count).execute_insert('sink').wait()


        resultSchema = count.to_pandas()
        print(resultSchema)
        # Pass the dynamic content to the template
        self.render("Template\\count.html",  count=resultSchema)


class DashboardHandlerByBrand(tornado.web.RequestHandler):
    def get(self):
     try:
        output_path = ''
        env_settings = EnvironmentSettings.new_instance() \
            .in_batch_mode() \
            .build()
        tbl_env = TableEnvironment.create(env_settings)

        column_names = ['created_date', 'repair_date', 'isdelivery', 'price', 'tech_id', 'client_id', 'brand',
                        'problem', 'component', 'feedback_rate', 'feedback_text', 'repair_id']
        column_types = [DataTypes.STRING(), DataTypes.STRING(), DataTypes.BOOLEAN(), DataTypes.DOUBLE(),
                        DataTypes.STRING(), DataTypes.STRING(), DataTypes.STRING(), DataTypes.STRING(),
                        DataTypes.STRING(), DataTypes.DOUBLE(), DataTypes.STRING(), DataTypes.STRING()]
        column_exprs = [f"col('{column}')" for column in column_names]
        column_exprs_str = ', '.join(column_exprs)
        schema = Schema.new_builder()
        for name, data_type in zip(column_names, column_types):
            schema.column(name, data_type)
        schema = schema.build()

        @udtf(result_types=[DataTypes.STRING(), DataTypes.DOUBLE()])
        def split(line: Row):
            for s, p in zip(str(line[6]).split(), str(line[3]).split()):
                yield Row(s, float(p))

        # tble_desc = TableDescriptor.for_connector('filesystem') \
        #    .schema(schema).format('csv').option('path', 'Repair_date.csv').build()
        source = CsvTableSource(
            'Repair_date.csv',
            column_names,
            column_types,
            ignore_first_line=False
        )

        tbl_env.register_table_source('financial_trxs', source)

        tbl = tbl_env.from_path('financial_trxs')

        result_table = tbl.flat_map(split).alias('brand','price')\
            .group_by(col("brand"))\
            .select(col('brand'), (col('price')).sum.alias('total'))


        # Convert the result table to Pandas DataFrame
        # Convert the result table to Pandas DataFrame
        result_df = result_table.to_pandas()
        feedback_rate_data = result_df.set_index('brand')['total']
        total_count = feedback_rate_data.sum()

        feedback_rate_percentages = {
            rate: count
            for rate, count in feedback_rate_data.items()
        }

        # Create Plotly figure
        x_values = list(feedback_rate_percentages.keys())
        y_values = list(feedback_rate_percentages.values())
        fig = go.Figure(data=[go.Bar(x=x_values, y=y_values)])
        fig.update_layout(title='Revenue by Brand Dashboard', xaxis_title='Brand',
                          yaxis_title='Revenue')

        # Convert the figure to JSON
        fig_json = fig.to_json()
        print(fig_json)
        # Render the HTML template and pass the JSON data
        fig_json_escaped = html.escape(fig_json)
        fig.show()
        #self.render("template\\dashboard.html", fig_json=fig_json)
     except Exception as e:
        print(e)
        exit(1)

class DashboardHandlerByRate(tornado.web.RequestHandler):
            def get(self):
                try:
                    env_settings = EnvironmentSettings.new_instance() \
                        .in_batch_mode() \
                        .build()
                    tbl_env = TableEnvironment.create(env_settings)

                    column_names = ['created_date', 'repair_date', 'isdelivery', 'price', 'tech_id', 'client_id',
                                    'brand',
                                    'problem', 'component', 'feedback_rate', 'feedback_text', 'repair_id']
                    column_types = [DataTypes.STRING(), DataTypes.STRING(), DataTypes.BOOLEAN(), DataTypes.DOUBLE(),
                                    DataTypes.STRING(), DataTypes.STRING(), DataTypes.STRING(), DataTypes.STRING(),
                                    DataTypes.STRING(), DataTypes.DOUBLE(), DataTypes.STRING(), DataTypes.STRING()]
                    column_exprs = [f"col('{column}')" for column in column_names]
                    column_exprs_str = ', '.join(column_exprs)
                    schema = Schema.new_builder()
                    for name, data_type in zip(column_names, column_types):
                        schema.column(name, data_type)
                    schema = schema.build()

                    @udtf(result_types=[DataTypes.STRING()])
                    def split(line: Row):
                        for s in str(line[9]).split():
                            print(s)
                            print(Row(s))

                            yield Row(s)

                    @udtf(result_types=[DataTypes.STRING()])
                    def splitprice(line: Row):
                        for s in str(line[3]).split():
                            print(s)
                            print(Row(s))

                            yield Row(s)
                    # tble_desc = TableDescriptor.for_connector('filesystem') \
                    #    .schema(schema).format('csv').option('path', 'Repair_date.csv').build()
                    source = CsvTableSource(
                        'Repair_date.csv',
                        column_names,
                        column_types,
                        ignore_first_line=False
                    )

                    tbl_env.register_table_source('financial_trxs', source)

                    tbl = tbl_env.from_path('financial_trxs')

                    result_table = tbl.flat_map(split).alias('feedback_rate') \
                        .group_by(col("feedback_rate")) \
                        .select(col('feedback_rate'), (col('feedback_rate')).count.alias('count')) \
 \
                        # Convert the result table to Pandas DataFrame
                    # Convert the result table to Pandas DataFrame
                    result_df = result_table.to_pandas()
                    feedback_rate_data = result_df.set_index('feedback_rate')['count']
                    total_count = feedback_rate_data.sum()

                    feedback_rate_percentages = {
                        rate: (count / total_count) * 100
                        for rate, count in feedback_rate_data.items()
                    }

                    # Create Plotly figure
                    x_values = list(feedback_rate_percentages.keys())
                    y_values = list(feedback_rate_percentages.values())
                    fig = go.Figure(data=[go.Bar(x=x_values, y=y_values)])

                    fig.update_layout(title='Feedback Rate Dashboard', xaxis_title='Feedback Rate',
                                      yaxis_title='Percentage')
                    # Convert the figure to JSON
                    fig_json = fig.to_json()
                    print(fig_json)
                    # Render the HTML template and pass the JSON data
                    fig_json_escaped = html.escape(fig_json)
                    fig.show()
                    self.render("template\\dashboard.html", fig_json=fig_json)
                except Exception as e:
                    print(e)
                    exit(1)


class ByUser(tornado.web.RequestHandler):
    def initialize(self):



        ''' self.table_env.execute_sql(
            f"CREATE TEMPORARY TABLE repair ("
            f"created_date STRING, repair_date STRING, isdelivery BOOLEAN, "
            f"price DOUBLE, tech_id STRING, client_id STRING, brand STRING, "
            f"problem STRING, component STRING, feedback_rate DOUBLE, "
            f"feedback_text STRING, repair_id STRING"
            f") WITH ( 'format' = 'csv', 'path' = 'fin_trxs.csv', "
            f"'schema' = '{schema}')")

        t_env.create_temporary_table('repair', tble_desc)

        tbl = t_env.from_path('repair')'''

    def post(self):
        try:

            tech_email = self.get_argument('tech_email')
            #request.form['filterValue']
            print(tech_email)
            #tech_email = tech_email
            env_settings = EnvironmentSettings.new_instance() \
                .in_batch_mode() \
                .build()
            tbl_env = TableEnvironment.create(env_settings)

            column_names = ['created_date', 'repair_date', 'isdelivery', 'price', 'tech_id', 'client_id', 'brand',
                            'problem', 'component', 'feedback_rate', 'feedback_text', 'repair_id']
            column_types = [DataTypes.STRING(), DataTypes.STRING(), DataTypes.BOOLEAN(), DataTypes.DOUBLE(),
                            DataTypes.STRING(), DataTypes.STRING(), DataTypes.STRING(), DataTypes.STRING(),
                            DataTypes.STRING(), DataTypes.DOUBLE(), DataTypes.STRING(), DataTypes.STRING()]
            column_exprs = [f"col('{column}')" for column in column_names]
            column_exprs_str = ', '.join(column_exprs)
            schema = Schema.new_builder()
            for name, data_type in zip(column_names, column_types):
                schema.column(name, data_type)
            schema = schema.build()

            #tble_desc = TableDescriptor.for_connector('filesystem') \
            #    .schema(schema).format('csv').option('path', 'Repair_date.csv').build()
            source = CsvTableSource(
                'Repair_date.csv',
                column_names,
                column_types,
                ignore_first_line=False
            )

            tbl_env.register_table_source('financial_trxs', source)

            tbl = tbl_env.from_path('financial_trxs')

            @udtf(result_types=[DataTypes.STRING()])
            def split(line: Row):
                for s in str(line[0]).split():
                    yield Row(s)

            #result = tbl.flat_map(split).alias('repair_id') \
            #    .select('*') \
            #    .filter(col('tech_id') == lit(tech_email))
            print(tech_email)
            result = tbl.select(col('created_date'), col('repair_date'), col('isdelivery'), col('price'), col('tech_id'), col('client_id'), col('brand'), col('problem'), col('component'), col('feedback_rate'), col('feedback_text'), col('repair_id'))\
                .filter(col('tech_id').like(lit(tech_email)))

            print("1")
            result_schema = result.to_pandas()
            print(result_schema)

            if not result_schema.empty:
                #self.render("Template\\index.html", table=result_schema)
                filtered_list = result_schema.to_dict('records')
                self.set_header('Content-Type', 'application/json')
                self.write({'data': filtered_list})
                #result_json = result_schema.to_json(orient='records')
                #result_json = json.dumps(result_schema)
                print(result_schema)
                #print(result_json)
                #return  result_json
                print("Repair filtered successfully!\n" + str(result_schema))
            else:
                self.write("No results found.")
        except Exception as e:
                print(e)
                exit(1)


def make_app():
    t_env = TableEnvironment.create(EnvironmentSettings.in_streaming_mode())
    # write all the data to one file
    t_env.get_config().set("parallelism.default", "1")
    template_loader = Jinja2Loader()
    template_env = jinja2.Environment(loader=template_loader)
    return tornado.web.Application([
        (r"/", MainHandler, dict(template_env=template_env)),
        (r"/count", DynamicHandler),
        (r"/byuser", ByUser),
        (r"/GoToDashboardRate", DashboardHandlerByRate),
        (r"/GoToDashboardBrand", DashboardHandlerByBrand),

    ])

if __name__ == "__main__":
    app = make_app()
    app.listen(8888)
    tornado.ioloop.IOLoop.current().start()