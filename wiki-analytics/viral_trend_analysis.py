import argparse
import json
import logging
from datetime import datetime
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
import apache_beam.transforms.window as window

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
    
    def format_article_for_bq(element):
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
    parser.add_argument('--tableName',required=True,help="BigQuery Table")

    known_args,pipeline_args = parser.parse_known_args

    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(StandardOptions).streaming = True

    with beam.Pipeline(pipeline_options) as p:
        parsed_stream=(
            p
            | "Read from Pub/Sub" >> beam.io.ReadFromPubSub(subscription=known_args.subscription)
            | "Assign Event Time Watermark" >> beam.Pardo(AssignEventWatermark())
        )

        # branch 1 - Trending Domains (1-Minute Fixed Window)
        trending_domains = (
            parsed_stream
            | "Branch1 : Extract Server Domain" >> beam.Map(lambda x: (x.get("server_name","unknown"),1))
            | "Branch1 : 1-Min Fixed Window" >> beam.WindowInto(window.FixedWindows(60))
            | "Branch1 : Count Per Domain" >> beam.CombinePerkey(sum)
            | "Branch1 : Format for Bq" >> beam.Map(format_article_for_bq)
            | "Barch1 : Write to BigQuery" >> beam.io.WriteToBigQuery(
                table=f"{known_args.project}:{known_args.bigq_dataset}.{known_args.tableName}",
                schema='server_name:STRING, edit_count:INTEGER, processing_time:TIMESTAMP',
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
            )
        )

if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()