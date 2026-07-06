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

def run():
    parser = argparse.ArgumentParser()
    parser.add_argument('--project',required=True,help='GCP Project Id')
    parser.add_argument('--subscription',required=True,help='subscription_id')
    parser.add_argument('--bigq_dataset',required=True,help='big query dataset')

    known_args,pipeline_args = parser.parse_known_args

    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(StandardOptions).streaming = True

    with beam.Pipeline(pipeline_options) as p:
        parsed_stream=(
            p
            | "Read from Pub/Sub" >> beam.io.ReadFromPubSub(subscription=known_args.subscription)
            | "Assign Event Time Watermark" >> beam.Pardo(AssignEventWatermark())
        )

if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()