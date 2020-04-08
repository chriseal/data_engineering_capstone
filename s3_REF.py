import numpy as np
import boto3
import re
import os


running_locally = False
scramble = False
s3_access = {
    'bucket-dev': {}, 
    'storage-dev': {}, 
    'bucket-staging': {}, 
    'bucket-prod': {}
}
# migrated collections
colls = {
    'collection': None,
    'collection2': None
}
scrambled_buckets = ['storage-dev', 'bucket-dev']
raw_buckets = ['bucket-staging'] #, 'bucket-prod']
DB_FPATH = './data'
prefix_for_initial_files = "initial/"
prefix_for_uploaded_files = "uploads/"

leading_slash_p = re.compile(r'^\/')


def _get_access_info():
    """ set envirnoment variables using create_credentialed_env_vars.py or manually """

    def get_val_from_key_str(l):
        return get_val_from_key_str(l)

    if running_locally:
        # bucket-dev
        with open('../../keys_bucket-dev.txt', 'r+') as f:
            access_info = f.readlines()
        s3_access['bucket-dev']['AWS_ACCESS_ID'] = [get_val_from_key_str(l) for l in access_info if l.lower().startswith('access key id')][0]
        s3_access['bucket-dev']['AWS_ACCESS_KEY'] = [get_val_from_key_str(l) for l in access_info if l.lower().startswith('secret access key')][0]

        # storage-dev
        with open('../../keys_storage-dev.txt', 'r+') as f:
            access_info = f.readlines()
        s3_access['storage-dev']['AWS_ACCESS_ID'] = [get_val_from_key_str(l) for l in access_info if l.lower().startswith('access key id')][0]
        s3_access['storage-dev']['AWS_ACCESS_KEY'] = [get_val_from_key_str(l) for l in access_info if l.lower().startswith('secret access key')][0]

        # 'bucket-staging
        with open('../../keys_bucket-staging.txt', 'r+') as f:
            access_info = f.readlines()
        s3_access['bucket-staging']['AWS_ACCESS_ID'] = [get_val_from_key_str(l) for l in access_info if l.lower().startswith('access key id')][0]
        s3_access['bucket-staging']['AWS_ACCESS_KEY'] = [get_val_from_key_str(l) for l in access_info if l.lower().startswith('secret access key')][0]

        # 'bucket-prod
        with open('../../keys_bucket-prod.txt', 'r+') as f:
            access_info = f.readlines()
        s3_access['bucket-prod']['AWS_ACCESS_ID'] = [get_val_from_key_str(l) for l in access_info if l.lower().startswith('access key id')][0]
        s3_access['bucket-prod']['AWS_ACCESS_KEY'] = [get_val_from_key_str(l) for l in access_info if l.lower().startswith('secret access key')][0]

    else:
        try:
            os.environ['BUCKET']
            os.environ['AWS_ACCESS_ID']
            os.environ['AWS_ACCESS_KEY']
        except KeyError:
            print("Looks like you need to add environment variables manually to ~/.bash_profile")
            print("""Append all variables manually to ~/.bash_profile in the form of:
export BUCKET=...
export AWS_ACCESS_ID=...
export ACCESS_KEY=...
and then restart terminal.""")

        s3_access = {
            os.environ['BUCKET']: {
                'AWS_ACCESS_ID': os.environ['AWS_ACCESS_ID'],
                'AWS_ACCESS_KEY': os.environ['AWS_ACCESS_KEY'],
            }
        }

    for bucket, credentials in s3_access.items():
        assert credentials['AWS_ACCESS_ID']
        assert credentials['AWS_ACCESS_KEY']
        s3_access[bucket]['client'] = boto3.client('s3', 
            aws_access_key_id=credentials['AWS_ACCESS_ID'], 
            aws_secret_access_key=credentials['AWS_ACCESS_KEY']
        )
        s3_access[bucket]['resource'] = boto3.resource('s3', 
            aws_access_key_id=credentials['AWS_ACCESS_ID'], 
            aws_secret_access_key=credentials['AWS_ACCESS_KEY']
        )

def upload_collections_to_s3():
    """
    # https://stackoverflow.com/questions/49886530/mongo-cursor-s3-putobject
    """

    if running_locally:
        if scramble:
            buckets = scrambled_buckets
        else:
            buckets = raw_buckets
    else:
        buckets = list(s3_access.keys())


    # description and titles replace null with -
    print("\n\nupload_collections_to_s3")
    sizes = {}
    for bucket in buckets:
        client = s3_access[bucket]['client']
        resource = s3_access[bucket]['resource']
        for coll in colls.keys():
            cursor = colls[coll].find() # https://stackoverflow.com/questions/19674311/json-serializing-mongodb
            collection_list = list(cursor)

            # put resource in s3
            res = resource.Bucket(bucket
                ).put_object(
                    Key=os.path.join(prefix_for_initial_files, coll+'.json'), 
                    Body=collection_list
                )
            obj = client.get_object(Bucket=res.bucket_name, Key=res.key)

# get object
client = s3_access[bucket]['client']
obj = client.get_object(Bucket=bucket, Key=key)
assert obj['ResponseMetadata']['HTTPStatusCode'] == 200

# put object
resource = s3_access[bucket]['resource']
res = resource.Bucket(bucket
    ).put_object(
        Key=os.path.join(prefix_for_initial_files, coll+'.json'), 
        Body=collection_list
    )


def delete_s3_folders():
    """
    delete folder across all s3 buckets
    https://stackoverflow.com/questions/11426560/amazon-s3-boto-how-to-delete-folder
    """

    folders_to_delete = ['<folder1>','<folder2>']
    for folder in folders_to_delete:
        for bucket, credentials in s3_access.items():
            s3 = boto3.resource('s3')
            objects_to_delete = credentials['client'].list_objects(Bucket=bucket, Prefix=folder+"/")

            delete_keys = {'Objects' : []}
            delete_keys['Objects'] = []
            keys = [obj['Key'] for obj in objects_to_delete.get('Contents', [])]
            for k in keys:
                delete_keys.append({'Key' : k})

            s3.meta.client.delete_objects(Bucket=bucket, Delete=delete_keys)


def upload_file_to_s3(bucket, s3_fpath, local_fpath, credentials):
    """ """

    credentials['resource'].Bucket(bucket).upload_file(local_fpath, s3_fpath)
    if np.random.choice([0,1], p=[0.98,0.02]):
        assert_object_exists(credentials['client'], credentials['resource'], 
            bucket, s3_fpath, download=False)


def assert_object_exists(client, resource, bucket_name, s3_fpath, download=False):
    """ """

    if download:
        try:
            resource.Bucket(bucket_name).download_file(s3_fpath, s3_fpath.split('/')[-1])
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == "404":
                print("The object does not exist.")
            else:
                raise
    else:
        obj = client.get_object(Bucket=bucket_name, Key=s3_fpath)
        assert obj['ResponseMetadata']['HTTPStatusCode'] == 200

        return obj


def upload_csvs_to_s3(, prefix):
    """ """

    csv_fnames = [f for f in os.listdir(DB_FPATH) if f.endswith('.csv')]
    buckets = scrambled_buckets if scramble else raw_buckets
    for csv_file_idx, csv_file in enumerate(csv_fnames):
        local_fpath = os.path.join(DB_FPATH, csv_file)
        s3_fpath = os.path.join(prefix, csv_file)
        df = pd.read_csv(local_fpath)
        if scramble:
            pass
        for bucket in buckets:
            upload_file_to_s3(bucket, s3_fpath, local_fpath, s3_access[bucket])
        print("{}/{}: Uploaded {} to {}".format(csv_file_idx+1, len(csv_fnames), csv_file, s3_fpath))


def see_all_available_buckets(resource):
    print("All available buckets:")
    for bucket in resource.buckets.all():
        print('\t', bucket.name)


def upload_documents_to_s3(min_doc_idx=0, num_jobs=1, modulo_remainder=0, scrambled_buckets=None, raw_buckets=None
    ):
    """
    min_doc_idx=0
        - if doc_idx is below this value, skip it
        - used in cases where the script was interrupted midway through the migration
    num_jobs=1, modulo_remainder=0
        - if there is 1 job, only one terminal window is needed and modulo_remainder should == 0
        - if there are multiple jobs, there should be num_jobs terminal windows open and modulo_remainder should be range(0,num_jobs) incrementing up by one in each individual terminal
    """

    assert running_locally

    if scrambled_buckets == None:
        scrambled_buckets = scrambled_buckets
    if raw_buckets == None:
        raw_buckets = raw_buckets

    filetypes = { # sampling of relevant filetypes
        'multipage_imgs': ['pdf','ppt','pptx'],
        'images': ['jpeg','jpg','png','png_400','tif','gif'],
        'text': ['txt','doc','docx'],
        'sheets': ['xls','xlsx'],
        'videos': ['wmv','mp4'],
        'other': ['psd','svg','webp','ai','bmp','eps','htm','ico','jfif'],
    }
    fileextensions = set([item for sublist in filetypes.values() for item in sublist])

    ignores = ['.DS_Store', '<folder_to_ignore>']
    test_files = [f for f in os.listdir('../../<database>') if f not in ignores]
    unavailable_files = []

    migrated_file_raw_cnt, migrated_file_scrambled_cnt = 0, 0
    for doc_idx, doc in enumerate(colls['collection'].find()):
        if doc_idx < min_doc_idx:
            continue
        if doc_idx % num_jobs != modulo_remainder:
            continue

        relative_fpath = re.sub(leading_slash_p, '', doc['path'])
        s3_fpath = os.path.join(prefix_for_uploaded_files, relative_fpath)

        # upload to raw buckets
        if raw_buckets:
            local_fpath = os.path.join(uploads_fpath, relative_fpath)
            if os.path.exists(local_fpath):
                for bucket in raw_buckets:
                    upload_file_to_s3(bucket, s3_fpath, local_fpath, s3_access[bucket])
                migrated_file_raw_cnt += 1
            else:
                print("\t{} not available".format(local_fpath))
                unavailable_files.append(local_fpath)
            if np.random.choice([0,1], p=[0.99,0.01]):
                print("\traw: {} --> {}".format(local_fpath, s3_fpath))


def list_bucket_files():
    """
    https://stackoverflow.com/questions/30249069/listing-contents-of-a-bucket-with-boto3
    """

    for bucket in s3_access.keys():
        resource = s3_access[bucket]['resource']
        res = resource.Bucket(bucket)
        for obj in res.objects.filter(Prefix=prefix_for_initial_files):
            print(obj.key)
        uploads = []
        for obj in res.objects.filter(Prefix=prefix_for_uploaded_files):
            uploads.append(obj.key)
        with open('{}_uploads.txt'.format(bucket), 'w') as f:
            for key in uploads:
                f.write("{}\n".format(key))


KEY = config.get('AWS','ACCESS_ID')
SECRET = config.get('AWS','ACCESS_KEY')

# Create clients
ec2 = boto3.resource('ec2',
    region_name='us-west-2',
    aws_access_key_id=KEY,
    aws_secret_access_key=SECRET
)

s3 = boto3.resource('s3',
    region_name='us-west-2',
    aws_access_key_id=KEY,
    aws_secret_access_key=SECRET
)

iam = boto3.client('iam', 
    region_name='us-west-2',
    aws_access_key_id=KEY,
    aws_secret_access_key=SECRET
)

redshift = boto3.client('redshift',
    region_name='us-west-2',
    aws_access_key_id=KEY,
    aws_secret_access_key=SECRET
)

# count number of objects in bucket
sampleDbBucket = s3.Bucket("<bucket>")
bucket_cnt = 0
for obj in sampleDbBucket.objects.filter(Prefix='<prefix>'):
    bucket_cnt += 1
print("{} objects in <bucket>/<prefix>".format(bucket_cnt))

# download this log path file locally for manual inspection
s3.Bucket('<bucket>').download_file('<s3_path.json>', '<local_path.json>')