import argparse
import json
import logging
from datetime import datetime
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
import apache_beam.transforms.window as window
from apache_beam.transforms.trigger import AfterWatermark, AccumulationMode, AfterCount

class AssignEventWatermark(beam.DoFn):
    def process(self,element):
        try:
            data = json.loads(element.decode('utf-8'))
            dt_str = data.get("meta").get("dt")
            if dt_str:
                dt_obj = datetime.strptime(dt_str,"%Y-%m-%dT%H:%M:%S.%fZ")
                dt_unix = dt_obj.timestamp()

                yield beam.window.TimestampedValue(data, dt_unix)
        except Exception as e:
            logging.error(f"Error parsing timestamp: {e}")
    
def bq_formatter_domain(element):
    server_name, count = element
    return {
        'server_name': server_name,
        'edit_count': count,
        'processing_time': datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    }

def bq_formatter_article(element):
    title_url, count = element
    return {
        'title_url': title_url,
        'edit_count': count,
        'processing_time': datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    }

def run():
    parser = argparse.ArgumentParser()
    parser.add_argument('--project',required=True,help='GCP Project Id')
    parser.add_argument('--subscription',required=True,help='subscription_id')
    parser.add_argument('--bigq_dataset',required=True,help='big query dataset')

    known_args,pipeline_args = parser.parse_known_args()

    pipeline_options = PipelineOptions(pipeline_args,
        project=known_args.project)
    pipeline_options.view_as(StandardOptions).streaming = True

    with beam.Pipeline(options=pipeline_options) as p:
        parsed_stream=(
            p
            | "Read from Pub/Sub" >> beam.io.ReadFromPubSub(subscription=known_args.subscription)
            | "Assign Event Time Watermark" >> beam.ParDo(AssignEventWatermark())
        )

        # branch 1 - Trending Domains (1-Minute Fixed Window, grace period - 10 sec)
        trending_domains = (
            parsed_stream
            | "Branch1 : Extract Server Domain" >> beam.Map(lambda x: (x.get("server_name","unknown"),1))
            | "Branch1 : 1-Min Fixed Window" >> beam.WindowInto(
                window.FixedWindows(60),
                trigger=AfterWatermark(late=AfterCount(1)),
                allowed_lateness=10,
                accumulation_mode=AccumulationMode.ACCUMULATING)
            | "Branch1 : Count Per Domain" >> beam.CombinePerKey(sum)
            | "Branch1 : Format for Bq" >> beam.Map(bq_formatter_domain)
            | "Barch1 : Write to BigQuery" >> beam.io.WriteToBigQuery(
                table=f"{known_args.project}:{known_args.bigq_dataset}.trending_domains",
                schema='server_name:STRING, edit_count:INTEGER, processing_time:TIMESTAMP',
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
            )
        )

        #  branch 2 - viral articles ( 5 mins sliding windw, updating every 1 min)
        viral_articles = (
            parsed_stream
            | "Branch2 : Filter edits" >> beam.Filter(lambda x: x.get("type")=="edit")
            | "Branch2 : Extract title url" >> beam.Map(lambda x : (x.get("title_url","unknown"),1))
            | "Branch2 : 5-min sliding window" >> beam.WindowInto(window.SlidingWindows(size=300,period=60))
            | "Branch2 : Count per article" >> beam.CombinePerKey(sum)
            | "Branch2 : Format for BQ" >> beam.Map(bq_formatter_article)
            | "Branch2 : Write to bigquery" >> beam.io.WriteToBigQuery(
                table=f"{known_args.project}:{known_args.bigq_dataset}.viral_articles",
                schema='server_name:STRING, edit_count:INTEGER, processing_time:TIMESTAMP',
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
            )
        )

if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()