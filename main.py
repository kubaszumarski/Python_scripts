import pandas as pd

output_bucket='kubasz_out'
# [START message_validatation_helper]
def validate_message(message, param):
    var = message.get(param)
    if not var:
        raise ValueError(
            "{} is not provided. Make sure you have \
                          property {} in the request".format(
                param, param
            )
        )
    return var
# [END message_validatation_helper]

def transform_file(bucket,filename):
    
    df=pd.read_csv(f"gs://{bucket}/{filename}", sep=',')
    filename_prefix=filename.split('.')[0]
    df.to_excel(f"gs://{output_bucket}/{filename_prefix}.xlsx", index=False)


def hello_gcs(event, context):
    """Triggered by a change to a Cloud Storage bucket.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """
    bucket = validate_message(event, 'bucket')
    name = validate_message(event, 'name')

    transform_file(bucket, name)
